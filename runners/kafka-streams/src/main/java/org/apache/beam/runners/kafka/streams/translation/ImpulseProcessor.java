/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.kafka.streams.translation;

import java.time.Duration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.kafka.streams.processor.Cancellable;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams {@link Processor} implementing Beam's {@code Impulse} transform.
 *
 * <p>For each task instance, emits exactly two {@link KStreamsPayload}s downstream:
 *
 * <ol>
 *   <li>A {@link KStreamsPayload#data data} payload wrapping a {@link WindowedValue} of an empty
 *       {@code byte[]} in the {@link org.apache.beam.sdk.transforms.windowing.GlobalWindow}, with
 *       event-time {@link BoundedWindow#TIMESTAMP_MIN_VALUE}.
 *   <li>A {@link KStreamsPayload#watermark watermark} payload at {@link
 *       BoundedWindow#TIMESTAMP_MAX_VALUE} that tells downstream transforms the source is done.
 * </ol>
 *
 * <p>A persistent state store records whether the data element has already been emitted so that
 * task restarts do not duplicate the data. The terminal watermark, on the other hand, is re-emitted
 * on every restart so downstream watermark holds release correctly after recovery (per Jan's review
 * on PR #38689).
 *
 * <p>The trigger comes from a wall-clock punctuator scheduled on {@link #init} — this lets the
 * processor fire even when the dedicated bootstrap source topic is empty, which is the expected
 * production state.
 *
 * <p>Kafka Streams disallows negative record timestamps, so the forwarded {@link Record} carries
 * the Unix epoch ({@code 0L}). The Beam event-time lives inside the {@link KStreamsPayload}
 * variant: inside the {@link WindowedValue} for data, or as the explicit watermark millis.
 */
class ImpulseProcessor implements Processor<byte[], byte[], byte[], KStreamsPayload<byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(ImpulseProcessor.class);

  /** Sole entry in the state store; the value tracks whether this processor has already emitted. */
  static final String FIRED_KEY = "fired";

  /** How soon after {@link #init} the punctuator first fires. */
  private static final Duration PUNCTUATION_DELAY = Duration.ofMillis(50);

  private final String stateStoreName;
  private final String transformId;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<byte[]>> context;
  private @Nullable KeyValueStore<String, Boolean> firedStore;
  private @Nullable Cancellable scheduledPunctuator;

  ImpulseProcessor(String stateStoreName, String transformId) {
    this.stateStoreName = stateStoreName;
    this.transformId = transformId;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<byte[]>> context) {
    this.context = context;
    this.firedStore = context.getStateStore(stateStoreName);
    this.scheduledPunctuator =
        context.schedule(PUNCTUATION_DELAY, PunctuationType.WALL_CLOCK_TIME, ts -> maybeFire());
  }

  @Override
  public void process(Record<byte[], byte[]> record) {
    // Records that happen to land on the bootstrap topic are not actual data; they just provide an
    // extra opportunity to fire the impulse on restart. The state store still gates the emit.
    maybeFire();
  }

  private void maybeFire() {
    ProcessorContext<byte[], KStreamsPayload<byte[]>> ctx = context;
    KeyValueStore<String, Boolean> store = firedStore;
    if (ctx == null || store == null) {
      return;
    }
    if (Boolean.TRUE.equals(store.get(FIRED_KEY))) {
      // Data was already emitted in a previous task lifetime, but downstream watermark holds may
      // still need to be released after the restart — re-emit the terminal watermark and stop the
      // punctuator.
      forwardWatermarkMax(ctx);
      cancelPunctuator();
      return;
    }
    WindowedValue<byte[]> impulse = WindowedValues.valueInGlobalWindow(new byte[0]);
    // The output PCollection is not keyed (PCollection<byte[]>); use an empty byte[] as a
    // placeholder key so downstream processors that adopt the byte[]-key convention see a
    // consistent shape.
    ctx.forward(
        new Record<byte[], KStreamsPayload<byte[]>>(
            new byte[0], KStreamsPayload.data(impulse), 0L));
    forwardWatermarkMax(ctx);
    store.put(FIRED_KEY, Boolean.TRUE);
    cancelPunctuator();
    LOG.debug("Impulse {} emitted single element and terminal watermark", transformId);
  }

  /**
   * Forwards a terminal {@code TIMESTAMP_MAX_VALUE} watermark payload to downstream processors,
   * stamped with this transform's id. Impulse is a single-instance source, so the report is for its
   * only partition: {@code sourcePartition=0} of {@code totalSourcePartitions=1}. Real
   * per-partition identities arrive once the topology gains topic-based shuffle.
   */
  private void forwardWatermarkMax(ProcessorContext<byte[], KStreamsPayload<byte[]>> ctx) {
    long maxMillis = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    ctx.forward(
        new Record<byte[], KStreamsPayload<byte[]>>(
            new byte[0], KStreamsPayload.watermark(maxMillis, transformId, 0, 1), 0L));
  }

  /** Cancels the wall-clock punctuator after the impulse has fired to stop periodic wakeups. */
  private void cancelPunctuator() {
    Cancellable handle = scheduledPunctuator;
    if (handle != null) {
      handle.cancel();
      scheduledPunctuator = null;
    }
  }
}

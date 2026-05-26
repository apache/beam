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
 * <p>Emits exactly one {@link WindowedValue} carrying an empty {@code byte[]} payload in the {@link
 * org.apache.beam.sdk.transforms.windowing.GlobalWindow}, with timestamp {@link
 * BoundedWindow#TIMESTAMP_MIN_VALUE}. The emission happens once per task and is persisted in a
 * state store keyed by the transform id so that task restarts do not re-emit.
 *
 * <p>The trigger comes from a wall-clock punctuator scheduled on {@link #init} — this lets the
 * processor fire even when the dedicated bootstrap source topic is empty, which is the expected
 * production state.
 *
 * <p><b>Watermark advancement to {@code TIMESTAMP_MAX_VALUE}</b> (design doc §4.1) is intentionally
 * <em>not</em> performed here. Kafka Streams has no native Beam watermark; the output PCollection's
 * watermark moves through the (future) runner-side watermark manager rather than through the {@link
 * Record} timestamp. The forwarded Kafka Streams record carries a non-negative record timestamp
 * ({@code 0L}) because KS rejects negative record timestamps; the Beam event-time lives inside the
 * {@link WindowedValue}.
 */
class ImpulseProcessor implements Processor<byte[], byte[], byte[], WindowedValue<byte[]>> {

  private static final Logger LOG = LoggerFactory.getLogger(ImpulseProcessor.class);

  /** Sole entry in the state store; the value tracks whether this processor has already emitted. */
  static final String FIRED_KEY = "fired";

  /** How soon after {@link #init} the punctuator first fires. */
  private static final Duration PUNCTUATION_DELAY = Duration.ofMillis(50);

  private final String stateStoreName;
  private final String transformId;

  private @Nullable ProcessorContext<byte[], WindowedValue<byte[]>> context;
  private @Nullable KeyValueStore<String, Boolean> firedStore;
  private @Nullable Cancellable scheduledPunctuator;

  ImpulseProcessor(String stateStoreName, String transformId) {
    this.stateStoreName = stateStoreName;
    this.transformId = transformId;
  }

  @Override
  public void init(ProcessorContext<byte[], WindowedValue<byte[]>> context) {
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
    ProcessorContext<byte[], WindowedValue<byte[]>> ctx = context;
    KeyValueStore<String, Boolean> store = firedStore;
    if (ctx == null || store == null) {
      return;
    }
    if (Boolean.TRUE.equals(store.get(FIRED_KEY))) {
      cancelPunctuator();
      return;
    }
    WindowedValue<byte[]> impulse = WindowedValues.valueInGlobalWindow(new byte[0]);
    // The output PCollection is not keyed (PCollection<byte[]>); use an empty byte[] as a
    // placeholder key so downstream processors that adopt the byte[]-key convention see a
    // consistent shape.
    //
    // Kafka Streams disallows negative record timestamps, so the Record carries the Unix epoch
    // (0L). The Beam event-time, BoundedWindow.TIMESTAMP_MIN_VALUE, lives inside the forwarded
    // WindowedValue and is what downstream Beam logic must consult.
    ctx.forward(new Record<byte[], WindowedValue<byte[]>>(new byte[0], impulse, 0L));
    store.put(FIRED_KEY, Boolean.TRUE);
    cancelPunctuator();
    LOG.debug("Impulse {} emitted single element", transformId);
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

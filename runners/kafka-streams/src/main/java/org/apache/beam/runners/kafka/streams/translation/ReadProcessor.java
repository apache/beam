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

import java.io.IOException;
import java.time.Duration;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.CoderUtils;
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
 * Kafka Streams {@link Processor} implementing Beam's deprecated primitive {@code Read}
 * (beam:transform:read:v1) over a {@link BoundedSource}.
 *
 * <p>For each task instance, reads the whole {@link BoundedSource} once and emits, in order:
 *
 * <ol>
 *   <li>One {@link KStreamsPayload#data data} payload per source element, each wrapping a {@link
 *       WindowedValue} in the {@link org.apache.beam.sdk.transforms.windowing.GlobalWindow} at the
 *       element's own event time (from {@link BoundedReader#getCurrentTimestamp()}).
 *   <li>A {@link KStreamsPayload#watermark watermark} payload at {@link
 *       BoundedWindow#TIMESTAMP_MAX_VALUE} telling downstream transforms the source is done.
 * </ol>
 *
 * <p><b>Wire form.</b> Unlike Impulse (whose element is already an opaque {@code byte[]}), a Read
 * produces <em>decoded</em> Java objects. Downstream {@link ExecutableStageProcessor} feeds
 * whatever it receives straight into the SDK harness, whose main-input receiver expects each
 * element in the runner-side wire form — a raw object for a model coder, but a length-prefixed
 * {@code byte[]} for a coder the runner does not know (e.g. {@code VarIntCoder}). Stage-to-stage
 * edges already carry that wire form because harness outputs are decoded with the runner-side wire
 * coder; this processor reproduces it for the source edge by transcoding each element through the
 * SDK-side wire coder (encode) and back through the runner-side wire coder (decode). The two are
 * byte-compatible by construction, so the transcode yields exactly the object the receiver expects,
 * nesting and all.
 *
 * <p>This mirrors {@link ImpulseProcessor}: a persistent state store records whether the elements
 * have already been emitted so task restarts do not duplicate them, while the terminal watermark is
 * re-emitted on every restart so downstream watermark holds still release after recovery. The
 * trigger is a wall-clock punctuator scheduled on {@link #init} so the processor fires even though
 * its bootstrap source topic is empty.
 *
 * <p>The source is read in a single instance with no splitting — parallelism across the source's
 * splits arrives with the topic-based shuffle work (#18479). Kafka Streams disallows negative
 * record timestamps, so each forwarded {@link Record} carries the Unix epoch ({@code 0L}); the Beam
 * event time lives inside the {@link WindowedValue}.
 */
class ReadProcessor<T> implements Processor<byte[], byte[], byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(ReadProcessor.class);

  /** Sole entry in the state store; the value tracks whether this processor has already emitted. */
  static final String FIRED_KEY = "fired";

  /** How soon after {@link #init} the punctuator first fires. */
  private static final Duration PUNCTUATION_DELAY = Duration.ofMillis(50);

  private final BoundedSource<T> source;
  private final PipelineOptions options;
  // Encodes a raw WindowedValue<T> as the SDK harness would on the wire (length-prefixing the
  // element coder if the runner does not know it); the runner-side coder then decodes it into the
  // wire form the downstream stage's input receiver expects. See the class javadoc.
  private final Coder<WindowedValue<T>> sdkWireCoder;
  private final Coder<WindowedValue<?>> runnerWireCoder;
  private final String stateStoreName;
  private final String transformId;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  private @Nullable KeyValueStore<String, Boolean> firedStore;
  private @Nullable Cancellable scheduledPunctuator;

  ReadProcessor(
      BoundedSource<T> source,
      PipelineOptions options,
      Coder<WindowedValue<T>> sdkWireCoder,
      Coder<WindowedValue<?>> runnerWireCoder,
      String stateStoreName,
      String transformId) {
    this.source = source;
    this.options = options;
    this.sdkWireCoder = sdkWireCoder;
    this.runnerWireCoder = runnerWireCoder;
    this.stateStoreName = stateStoreName;
    this.transformId = transformId;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    this.firedStore = context.getStateStore(stateStoreName);
    this.scheduledPunctuator =
        context.schedule(PUNCTUATION_DELAY, PunctuationType.WALL_CLOCK_TIME, ts -> maybeFire());
  }

  @Override
  public void process(Record<byte[], byte[]> record) {
    // Records that happen to land on the bootstrap topic are not actual data; they just provide an
    // extra opportunity to fire the read on restart. The state store still gates the emit.
    maybeFire();
  }

  private void maybeFire() {
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = context;
    KeyValueStore<String, Boolean> store = firedStore;
    if (ctx == null || store == null) {
      return;
    }
    if (Boolean.TRUE.equals(store.get(FIRED_KEY))) {
      // Elements were already emitted in a previous task lifetime, but downstream watermark holds
      // may still need to release after the restart — re-emit the terminal watermark and stop.
      forwardWatermarkMax(ctx);
      cancelPunctuator();
      return;
    }
    int count = readAndForward(ctx);
    forwardWatermarkMax(ctx);
    store.put(FIRED_KEY, Boolean.TRUE);
    cancelPunctuator();
    LOG.debug("Read {} emitted {} elements and terminal watermark", transformId, count);
  }

  /** Reads the whole bounded source and forwards each element, in wire form, as a data payload. */
  private int readAndForward(ProcessorContext<byte[], KStreamsPayload<?>> ctx) {
    int count = 0;
    try (BoundedReader<T> reader = source.createReader(options)) {
      for (boolean hasElement = reader.start(); hasElement; hasElement = reader.advance()) {
        WindowedValue<T> element =
            WindowedValues.timestampedValueInGlobalWindow(
                reader.getCurrent(), reader.getCurrentTimestamp());
        // The Read output PCollection is not keyed; use an empty byte[] as a placeholder key so
        // downstream processors that adopt the byte[]-key convention see a consistent shape.
        ctx.forward(
            new Record<byte[], KStreamsPayload<?>>(
                new byte[0], KStreamsPayload.data(toRunnerWire(element)), 0L));
        count++;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read bounded source for transform " + transformId, e);
    }
    return count;
  }

  /** Transcodes a raw element into the runner-side wire form the SDK harness input expects. */
  private WindowedValue<?> toRunnerWire(WindowedValue<T> element) {
    try {
      byte[] wireBytes = CoderUtils.encodeToByteArray(sdkWireCoder, element);
      return CoderUtils.decodeFromByteArray(runnerWireCoder, wireBytes);
    } catch (CoderException e) {
      throw new RuntimeException(
          "Failed to transcode a read element to wire form for transform " + transformId, e);
    }
  }

  /**
   * Forwards a terminal {@code TIMESTAMP_MAX_VALUE} watermark payload to downstream processors.
   *
   * <p>Read is a single-instance source, so the report is stamped as the only source partition:
   * {@code sourcePartition=0} of {@code totalSourcePartitions=1}. Real per-partition identities
   * arrive once the topology gains topic-based shuffle.
   */
  private static void forwardWatermarkMax(ProcessorContext<byte[], KStreamsPayload<?>> ctx) {
    long maxMillis = BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis();
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            new byte[0], KStreamsPayload.<Object>watermark(maxMillis, 0, 1), 0L));
  }

  /** Cancels the wall-clock punctuator after the read has fired to stop periodic wakeups. */
  private void cancelPunctuator() {
    Cancellable handle = scheduledPunctuator;
    if (handle != null) {
      handle.cancel();
      scheduledPunctuator = null;
    }
  }
}

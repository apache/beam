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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
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
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads an {@link UnboundedSource} and forwards its elements and watermark downstream.
 *
 * <p>Where the bounded {@link ReadProcessor} drains its source once and jumps the watermark to the
 * end of time, an unbounded source never finishes: it is polled repeatedly, and its watermark
 * advances gradually as the reader reports progress. That difference is what makes this a streaming
 * runner rather than a batch one — downstream windows close because the source says time has moved
 * on, not because the input ran out.
 *
 * <p>Polling happens on a wall-clock punctuator rather than in {@code process}, because the
 * processor's bootstrap topic is empty and nothing else would drive it. Each turn reads at most
 * {@link #maxElementsPerPoll} elements so a busy source cannot monopolise the Kafka Streams thread
 * and starve the rest of the topology, then forwards the reader's watermark if it advanced.
 *
 * <p><b>Restart</b> is what the checkpoint mark is for. {@link UnboundedReader#getCheckpointMark()}
 * describes the position the reader has consumed to; it is written to a persistent state store, and
 * on {@link #init} the reader is created from the stored mark rather than from scratch, so a task
 * that moves or restarts resumes where it left off instead of re-reading from the beginning. The
 * store is changelogged and, under exactly-once, its writes commit atomically with the records the
 * processor forwarded, so the mark can never be ahead of the data that was actually emitted.
 *
 * <p>The source handed to this processor has already been split by {@link ReadTranslator}, which is
 * where splitting belongs: it happens once for the pipeline rather than once per task instance, and
 * the contract does not define splitting an already-split source. Reading several splits in
 * parallel arrives with the topic-based shuffle work (#18479). As in the bounded processor, Kafka
 * Streams disallows negative record timestamps, so each forwarded {@link Record} carries the Unix
 * epoch and the Beam event time travels inside the {@link WindowedValue}.
 */
class UnboundedReadProcessor<T, CheckpointT extends CheckpointMark>
    implements Processor<byte[], byte[], byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadProcessor.class);

  /** Sole entry in the state store; the value is the encoded checkpoint mark. */
  static final String CHECKPOINT_KEY = "checkpoint";

  /** How often the source is polled. */
  private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

  private final UnboundedSource<T, CheckpointT> source;
  private final SerializablePipelineOptions options;
  // See ReadProcessor: a source produces decoded objects, but the downstream stage's harness input
  // expects the runner-side wire form, so each element is transcoded through these two coders.
  private final Coder<WindowedValue<T>> sdkWireCoder;
  private final Coder<WindowedValue<?>> runnerWireCoder;
  private final Coder<CheckpointT> checkpointCoder;
  private final String stateStoreName;
  private final String transformId;
  private final int maxElementsPerPoll;
  private final int checkpointEveryNPolls;

  private @Nullable ProcessorContext<byte[], KStreamsPayload<?>> context;
  private @Nullable KeyValueStore<String, byte[]> checkpointStore;
  private @Nullable UnboundedReader<T> reader;
  private boolean readerStarted;
  private Instant lastForwardedWatermark = BoundedWindow.TIMESTAMP_MIN_VALUE;
  /** Set once the source's watermark reaches the end of time; it will produce nothing more. */
  private boolean exhausted;

  // An unbounded source normally never reaches the terminal watermark, so this normally never
  // reports anything. It does matter for a source that is drained or is bounded in practice.
  private final TerminationReporter terminationReporter;

  private @Nullable Cancellable scheduledPunctuator;

  UnboundedReadProcessor(
      UnboundedSource<T, CheckpointT> source,
      SerializablePipelineOptions options,
      Coder<WindowedValue<T>> sdkWireCoder,
      Coder<WindowedValue<?>> runnerWireCoder,
      Coder<CheckpointT> checkpointCoder,
      String stateStoreName,
      String transformId,
      int maxElementsPerPoll,
      int checkpointEveryNPolls,
      TerminationTracker terminationTracker) {
    this.terminationReporter = new TerminationReporter(terminationTracker, transformId);
    this.source = source;
    this.options = options;
    this.sdkWireCoder = sdkWireCoder;
    this.runnerWireCoder = runnerWireCoder;
    this.checkpointCoder = checkpointCoder;
    this.stateStoreName = stateStoreName;
    this.transformId = transformId;
    this.maxElementsPerPoll = maxElementsPerPoll;
    this.checkpointEveryNPolls = checkpointEveryNPolls;
  }

  @Override
  public void init(ProcessorContext<byte[], KStreamsPayload<?>> context) {
    this.context = context;
    this.checkpointStore = context.getStateStore(stateStoreName);
    terminationReporter.init(context);
    this.scheduledPunctuator =
        context.schedule(POLL_INTERVAL, PunctuationType.WALL_CLOCK_TIME, timestamp -> poll());
  }

  @Override
  public void process(Record<byte[], byte[]> record) {
    // The bootstrap topic carries no real data; a record arriving on it is just another chance to
    // poll. The reader's own position decides what is actually emitted.
    poll();
  }

  /**
   * Drains what the source currently has, in batches, then publishes the watermark.
   *
   * <p>A batch is capped at {@link #maxElementsPerPoll} so that the checkpoint mark and the
   * watermark are updated as the reader progresses rather than only at the end. Batches run back to
   * back while the source keeps filling them, since returning after every batch would cap
   * throughput at one batch per punctuation interval.
   *
   * <p>The run is bounded all the same. A source that always has data — which is the normal case
   * for one that is keeping up — would otherwise never let this method return, and the Kafka
   * Streams thread would never get back to committing or to the rest of the topology. So at most
   * {@link #checkpointEveryNPolls} batches are taken before yielding, which is also where the
   * checkpoint mark is stored, and the next punctuation carries on from there.
   */
  private void poll() {
    if (exhausted) {
      return;
    }
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    UnboundedReader<T> currentReader = ensureReader();
    for (int batch = 0; batch < checkpointEveryNPolls; batch++) {
      int emitted = readBatch(ctx, currentReader);
      Instant watermark = currentReader.getWatermark();
      forwardWatermarkIfAdvanced(ctx, watermark);
      if (!watermark.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
        // The source has declared it will produce nothing further, so stop polling it. Store the
        // final position first, since the loop will not come back to it.
        storeCheckpoint(currentReader);
        exhausted = true;
        Cancellable punctuator = scheduledPunctuator;
        if (punctuator != null) {
          punctuator.cancel();
          scheduledPunctuator = null;
        }
        return;
      }
      if (emitted < maxElementsPerPoll) {
        // Short batch: the source has nothing more for now, so store what was read and wait for
        // the next punctuation rather than spinning on a reader that keeps returning false.
        if (emitted > 0) {
          storeCheckpoint(currentReader);
        }
        return;
      }
    }
    // Yielded on the batch bound rather than on an empty source, so record the position reached.
    storeCheckpoint(currentReader);
  }

  /** Forwards up to {@link #maxElementsPerPoll} elements, returning how many were available. */
  private int readBatch(
      ProcessorContext<byte[], KStreamsPayload<?>> ctx, UnboundedReader<T> currentReader) {
    int emitted = 0;
    try {
      while (emitted < maxElementsPerPoll) {
        // start() positions the reader on its first element; advance() moves to the next. Either
        // returning false means nothing is available right now — not that the source is finished,
        // which is the difference from a bounded read.
        boolean hasElement = readerStarted ? currentReader.advance() : currentReader.start();
        readerStarted = true;
        if (!hasElement) {
          break;
        }
        WindowedValue<T> element =
            WindowedValues.timestampedValueInGlobalWindow(
                currentReader.getCurrent(), currentReader.getCurrentTimestamp());
        ctx.forward(
            new Record<byte[], KStreamsPayload<?>>(
                new byte[0], KStreamsPayload.data(toRunnerWire(element)), 0L));
        emitted++;
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to read unbounded source for transform " + transformId, e);
    }
    return emitted;
  }

  /** Publishes the reader's watermark, which is what lets downstream windows close. */
  private void forwardWatermarkIfAdvanced(
      ProcessorContext<byte[], KStreamsPayload<?>> ctx, Instant watermark) {
    if (!watermark.isAfter(lastForwardedWatermark)) {
      return;
    }
    lastForwardedWatermark = watermark;
    ctx.forward(
        new Record<byte[], KStreamsPayload<?>>(
            new byte[0], KStreamsPayload.watermark(watermark.getMillis(), transformId, 0, 1), 0L));
    terminationReporter.watermarkEmitted(ctx, watermark.getMillis());
  }

  /** Creates the reader on first use, resuming from the stored checkpoint mark if there is one. */
  private UnboundedReader<T> ensureReader() {
    UnboundedReader<T> existing = reader;
    if (existing != null) {
      return existing;
    }
    try {
      UnboundedReader<T> created = source.createReader(options.get(), restoreCheckpoint());
      reader = created;
      return created;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create a reader for unbounded source in transform " + transformId, e);
    }
  }

  private @Nullable CheckpointT restoreCheckpoint() {
    KeyValueStore<String, byte[]> store = checkInitialized(checkpointStore);
    byte[] encoded = store.get(CHECKPOINT_KEY);
    if (encoded == null) {
      return null;
    }
    try {
      CheckpointT mark = CoderUtils.decodeFromByteArray(checkpointCoder, encoded);
      LOG.info("Unbounded read {} resuming from a stored checkpoint mark", transformId);
      return mark;
    } catch (CoderException e) {
      throw new RuntimeException(
          "Failed to decode the checkpoint mark for transform " + transformId, e);
    }
  }

  private void storeCheckpoint(UnboundedReader<T> currentReader) {
    KeyValueStore<String, byte[]> store = checkInitialized(checkpointStore);
    @SuppressWarnings("unchecked")
    CheckpointT mark = (CheckpointT) currentReader.getCheckpointMark();
    try {
      store.put(CHECKPOINT_KEY, CoderUtils.encodeToByteArray(checkpointCoder, mark));
    } catch (CoderException e) {
      throw new RuntimeException(
          "Failed to encode the checkpoint mark for transform " + transformId, e);
    }
  }

  /** Transcodes a raw element into the runner-side wire form the SDK harness input expects. */
  private WindowedValue<?> toRunnerWire(WindowedValue<T> element) {
    try {
      byte[] wireBytes = CoderUtils.encodeToByteArray(sdkWireCoder, element);
      return CoderUtils.decodeFromByteArray(runnerWireCoder, wireBytes);
    } catch (CoderException e) {
      throw new RuntimeException(
          "Failed to transcode an unbounded-read element to wire form for transform " + transformId,
          e);
    }
  }

  @Override
  public void close() {
    UnboundedReader<T> currentReader = reader;
    if (currentReader != null) {
      try {
        currentReader.close();
      } catch (IOException e) {
        LOG.warn("Error closing the reader for unbounded source {}", transformId, e);
      }
      reader = null;
    }
    // Last, so the pipeline is not declared finished while this source is still closing down.
    terminationReporter.close();
  }

  private static <V> V checkInitialized(@Nullable V value) {
    if (value == null) {
      throw new IllegalStateException("UnboundedReadProcessor used before init()");
    }
    return value;
  }
}

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
 * end of time, an unbounded source never finishes: it is polled repeatedly and its watermark
 * advances as the reader reports progress. That is what makes downstream windows close because time
 * moved on rather than because the input ran out.
 *
 * <p>Polling runs on a wall-clock punctuator, since the bootstrap topic is empty and nothing else
 * would drive it. A turn is bounded by {@link #maxElementsPerPoll} and by {@link #maxPollTimeMs} so
 * a busy source cannot hold the Kafka Streams thread and starve the rest of the topology.
 *
 * <p>The checkpoint mark is what makes restart work. {@link UnboundedReader#getCheckpointMark()} is
 * written to a persistent state store and the reader is recreated from it in {@link #init}, so a
 * task that moves or restarts resumes where it left off. The store is changelogged and, under
 * exactly-once, commits atomically with the records forwarded, so the mark cannot run ahead of the
 * data actually emitted.
 *
 * <p>The source is split once by {@link ReadTranslator} rather than per task; reading several
 * splits in parallel arrives with #18479. Kafka Streams rejects negative record timestamps, so each
 * {@link Record} carries the Unix epoch and the event time travels inside the {@link
 * WindowedValue}.
 */
class UnboundedReadProcessor<T, CheckpointT extends CheckpointMark>
    implements Processor<byte[], byte[], byte[], KStreamsPayload<?>> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedReadProcessor.class);

  /** Sole entry in the state store; the value is the encoded checkpoint mark. */
  static final String CHECKPOINT_KEY = "checkpoint";

  /** How often the source is polled. */
  private static final Duration POLL_INTERVAL = Duration.ofMillis(50);

  /** Elements read between two checks of whether the turn is out of time. */
  private static final int ELEMENTS_BETWEEN_DEADLINE_CHECKS = 64;

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
  private final int maxPollTimeMs;

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
      int maxPollTimeMs,
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
    this.maxPollTimeMs = maxPollTimeMs;
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
   * <p>A batch is capped at {@link #maxElementsPerPoll} so the checkpoint mark and the watermark
   * move as the reader progresses, and batches run back to back while the source keeps filling
   * them, since returning after each would cap throughput at one batch per punctuation.
   *
   * <p>Both bounds exist because this runs on the thread that also serves the rest of the topology.
   * At most {@link #checkpointEveryNPolls} batches are taken before yielding, and {@link
   * #maxPollTimeMs} bounds the turn in time — a count cannot, since how long an element takes is
   * decided by the pipeline below the source. A turn that overruns {@link #POLL_INTERVAL} is due
   * again the moment it returns and runs instead of the tasks beneath it, which shows up as a
   * pipeline that reads steadily and emits nothing.
   */
  private void poll() {
    if (exhausted) {
      return;
    }
    ProcessorContext<byte[], KStreamsPayload<?>> ctx = checkInitialized(context);
    UnboundedReader<T> currentReader = ensureReader();
    long deadline = System.currentTimeMillis() + maxPollTimeMs;
    for (int batch = 0; batch < checkpointEveryNPolls; batch++) {
      int emitted = readBatch(ctx, currentReader, deadline);
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
        // Short batch: either the source has nothing more for now, or the turn ran out of time.
        // Either way, store what was read and wait for the next punctuation rather than spinning
        // on a reader that keeps returning false.
        if (emitted > 0) {
          storeCheckpoint(currentReader);
        }
        return;
      }
      if (System.currentTimeMillis() >= deadline) {
        // A full batch and the turn is out of time: yield with the position recorded, so the next
        // punctuation carries on rather than this one running the thread out from under the rest
        // of the topology.
        storeCheckpoint(currentReader);
        return;
      }
    }
    // Yielded on the batch bound rather than on an empty source, so record the position reached.
    storeCheckpoint(currentReader);
  }

  /**
   * Forwards up to {@link #maxElementsPerPoll} elements, returning how many were available.
   *
   * <p>Stops early if the turn's deadline passes, since one batch can be long enough on its own to
   * overrun it. The clock is read every {@link #ELEMENTS_BETWEEN_DEADLINE_CHECKS} elements rather
   * than every element, which bounds the overshoot to that many elements without putting a clock
   * read in front of each one.
   */
  private int readBatch(
      ProcessorContext<byte[], KStreamsPayload<?>> ctx,
      UnboundedReader<T> currentReader,
      long deadline) {
    int emitted = 0;
    try {
      while (emitted < maxElementsPerPoll) {
        if (emitted % ELEMENTS_BETWEEN_DEADLINE_CHECKS == 0
            && emitted > 0
            && System.currentTimeMillis() >= deadline) {
          break;
        }
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

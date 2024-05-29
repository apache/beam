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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceReaderBase;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link org.apache.flink.api.connector.source.SourceReader SourceReader} implementation
 * that reads from the assigned {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit
 * FlinkSourceSplits} by using Beam {@link org.apache.beam.sdk.io.UnboundedSource.UnboundedReader
 * UnboundedReaders}.
 *
 * <p>This reader consumes all the assigned source splits concurrently.
 *
 * @param <T> the output element type of the encapsulated Beam {@link
 *     org.apache.beam.sdk.io.UnboundedSource.UnboundedReader UnboundedReader}.
 */
public class FlinkUnboundedSourceReader<T>
    extends FlinkSourceReaderBase<T, WindowedValue<ValueWithRecordId<T>>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkUnboundedSourceReader.class);
  // This name is defined in FLIP-33.
  @VisibleForTesting protected static final String PENDING_BYTES_METRIC_NAME = "pendingBytes";
  private static final long SLEEP_ON_IDLE_MS = 50L;
  private static final long MIN_WATERMARK_EMIT_INTERVAL_MS = 10L;
  private final AtomicReference<@Nullable CompletableFuture<Void>> dataAvailableFutureRef =
      new AtomicReference<>();
  private final List<ReaderAndOutput> readers = new ArrayList<>();
  private int currentReaderIndex = 0;
  private volatile boolean shouldEmitWatermark;
  private final LinkedHashMap<Long, List<FlinkSourceSplit<T>>> pendingCheckpoints =
      new LinkedHashMap<>();

  public FlinkUnboundedSourceReader(
      String stepName,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      @Nullable Function<WindowedValue<ValueWithRecordId<T>>, Long> timestampExtractor) {
    super(stepName, context, pipelineOptions, timestampExtractor);
  }

  @VisibleForTesting
  protected FlinkUnboundedSourceReader(
      String stepName,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      ScheduledExecutorService executor,
      @Nullable Function<WindowedValue<ValueWithRecordId<T>>, Long> timestampExtractor) {
    super(stepName, executor, context, pipelineOptions, timestampExtractor);
  }

  @Override
  protected void addSplitsToUnfinishedForCheckpoint(
      long checkpointId, List<FlinkSourceSplit<T>> flinkSourceSplits) {

    pendingCheckpoints.put(checkpointId, flinkSourceSplits);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    super.notifyCheckpointComplete(checkpointId);
    List<Long> finalized = new ArrayList<>();
    for (Map.Entry<Long, List<FlinkSourceSplit<T>>> e : pendingCheckpoints.entrySet()) {
      if (e.getKey() <= checkpointId) {
        for (FlinkSourceSplit<T> s : e.getValue()) {
          finalizeSourceSplit(s.getCheckpointMark());
        }
        finalized.add(e.getKey());
      }
    }
    finalized.forEach(pendingCheckpoints::remove);
  }

  @Override
  public void start() {
    createPendingBytesGauge(context);
    Long watermarkInterval =
        pipelineOptions.as(FlinkPipelineOptions.class).getAutoWatermarkInterval();
    if (watermarkInterval == null) {
      watermarkInterval =
          (pipelineOptions.as(FlinkPipelineOptions.class).getMaxBundleTimeMills()) / 5L;
      watermarkInterval =
          (watermarkInterval > MIN_WATERMARK_EMIT_INTERVAL_MS)
              ? watermarkInterval
              : MIN_WATERMARK_EMIT_INTERVAL_MS;
      LOG.warn(
          "AutoWatermarkInterval is not set, watermarks will be emitted at a default interval of {} ms",
          watermarkInterval);
    }
    scheduleTaskAtFixedRate(
        () -> {
          // Set the watermark emission flag first.
          shouldEmitWatermark = true;
          // Wake up the main thread if necessary.
          CompletableFuture<Void> f = dataAvailableFutureRef.get();
          if (f != null) {
            f.complete(null);
          }
        },
        watermarkInterval,
        watermarkInterval);
  }

  @Override
  public InputStatus pollNext(ReaderOutput<WindowedValue<ValueWithRecordId<T>>> output)
      throws Exception {
    checkExceptionAndMaybeThrow();
    maybeEmitWatermark();
    maybeCreateReaderForNewSplits();

    ReaderAndOutput reader = nextReaderWithData(output);
    if (reader != null) {
      emitRecord(reader, output);
      return InputStatus.MORE_AVAILABLE;
    } else if (noMoreSplits() && isEndOfAllReaders()) {
      LOG.info("No more splits and no reader available. Terminating consumption.");
      return InputStatus.END_OF_INPUT;
    } else {
      LOG.trace("No data available for now.");
      return InputStatus.NOTHING_AVAILABLE;
    }
  }

  private boolean isEndOfAllReaders() {
    return allReaders().values().stream()
        .allMatch(
            r ->
                asUnbounded(r.reader).getWatermark().getMillis()
                    >= BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
  }

  /**
   * Check whether there are data available from alive readers. If not, set a future and wait for
   * the periodically running wake-up task to complete that future when the check interval passes.
   * This method is only called by the main thread, which is the only thread writing to the future
   * ref. Note that for UnboundedSource, because the splits never finishes, there are always alive
   * readers after the first split assigment. Hence, the return value of {@link
   * FlinkSourceReaderBase#isAvailable()} will effectively be determined by this method after the
   * first split assignment.
   */
  @Override
  protected CompletableFuture<Void> isAvailableForAliveReaders() {
    CompletableFuture<Void> future = dataAvailableFutureRef.get();
    if (future == null) {
      CompletableFuture<Void> newFuture = new CompletableFuture<>();
      // Need to set the future first to avoid the race condition of missing the watermark emission
      // notification.
      dataAvailableFutureRef.set(newFuture);
      if (shouldEmitWatermark || hasException()) {
        // There are exception after we set the new future,
        // immediately complete the future and return.
        dataAvailableFutureRef.set(null);
        newFuture.complete(null);
      } else {
        LOG.debug("There is no data available, scheduling the idle reader checker.");
        scheduleTask(
            () -> {
              CompletableFuture<Void> f = dataAvailableFutureRef.get();
              if (f != null) {
                f.complete(null);
              }
            },
            SLEEP_ON_IDLE_MS);
      }
      return newFuture;
    } else if (future.isDone()) {
      // The previous future is completed, just use it and reset the future ref.
      dataAvailableFutureRef.compareAndSet(future, null);
      return future;
    } else {
      // The previous future has not been completed, just use it.
      return future;
    }
  }

  @Override
  protected FlinkSourceSplit<T> getReaderCheckpoint(int splitId, ReaderAndOutput readerAndOutput) {
    // The checkpoint for unbounded sources is fine granular.
    UnboundedSource.UnboundedReader<T> reader =
        (UnboundedSource.UnboundedReader<T>) readerAndOutput.reader;
    UnboundedSource.CheckpointMark checkpointMark = reader.getCheckpointMark();
    @SuppressWarnings("unchecked")
    Coder<UnboundedSource.CheckpointMark> coder =
        (Coder<UnboundedSource.CheckpointMark>) reader.getCurrentSource().getCheckpointMarkCoder();
    byte[] checkpointState = encodeCheckpointMark(coder, checkpointMark);

    return new FlinkSourceSplit<>(
        splitId, readerAndOutput.reader.getCurrentSource(), checkpointState, checkpointMark);
  }

  @Override
  protected Source.Reader<T> createReader(@Nonnull FlinkSourceSplit<T> sourceSplit)
      throws IOException {
    Source<T> beamSource = sourceSplit.getBeamSplitSource();
    return createUnboundedSourceReader(beamSource, sourceSplit.getSplitState());
  }

  // -------------- private helper methods ----------------

  private void emitRecord(
      ReaderAndOutput readerAndOutput, ReaderOutput<WindowedValue<ValueWithRecordId<T>>> output) {
    UnboundedSource.UnboundedReader<T> reader = asUnbounded(readerAndOutput.reader);
    T item = reader.getCurrent();
    byte[] recordId = reader.getCurrentRecordId();
    Instant timestamp = reader.getCurrentTimestamp();

    WindowedValue<ValueWithRecordId<T>> windowedValue =
        WindowedValue.of(
            new ValueWithRecordId<>(item, recordId),
            timestamp,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING);
    LOG.trace("Emitting record: {}", windowedValue);
    if (timestampExtractor == null) {
      readerAndOutput.getAndMaybeCreateSplitOutput(output).collect(windowedValue);
    } else {
      readerAndOutput
          .getAndMaybeCreateSplitOutput(output)
          .collect(windowedValue, timestampExtractor.apply(windowedValue));
    }
    numRecordsInCounter.inc();
  }

  private void maybeEmitWatermark() {
    // Here we rely on the Flink source watermark multiplexer to combine the per-split watermark.
    // The runnable may emit more than one watermark when it runs.
    if (shouldEmitWatermark) {
      allReaders()
          .values()
          .forEach(
              readerAndOutput -> {
                SourceOutput<?> sourceOutput = readerAndOutput.sourceOutput();
                if (sourceOutput != null) {
                  long watermark = asUnbounded(readerAndOutput.reader).getWatermark().getMillis();
                  sourceOutput.emitWatermark(new Watermark(watermark));
                }
              });
      shouldEmitWatermark = false;
    }
  }

  private void maybeCreateReaderForNewSplits() throws Exception {
    while (!sourceSplits().isEmpty()) {
      Optional<ReaderAndOutput> readerAndOutputOpt = createAndTrackNextReader();
      if (readerAndOutputOpt.isPresent()) {
        readers.add(readerAndOutputOpt.get());
      } else {
        // Null splitId is only possible when exception occurs, just check exception to throw it.
        checkExceptionAndMaybeThrow();
      }
    }
  }

  private @Nullable ReaderAndOutput nextReaderWithData(
      ReaderOutput<WindowedValue<ValueWithRecordId<T>>> output) throws IOException {

    int numReaders = readers.size();
    for (int i = 0; i < numReaders; i++) {
      ReaderAndOutput readerAndOutput = readers.get(currentReaderIndex);
      currentReaderIndex = (currentReaderIndex + 1) % numReaders;
      if (readerAndOutput.startOrAdvance(output)) {
        return readerAndOutput;
      }
    }
    return null;
  }

  private static <T> UnboundedSource.UnboundedReader<T> asUnbounded(Source.Reader<T> reader) {
    return (UnboundedSource.UnboundedReader<T>) reader;
  }

  private void createPendingBytesGauge(SourceReaderContext context) {
    // TODO: Replace with SourceReaderContest.metricGroup().setPendingBytesGauge() after Flink 1.14
    // and above.
    context
        .metricGroup()
        .gauge(
            PENDING_BYTES_METRIC_NAME,
            () -> {
              long pendingBytes = -1L;
              for (FlinkSourceReaderBase<?, ?>.ReaderAndOutput readerAndOutput :
                  allReaders().values()) {
                long pendingBytesForReader =
                    asUnbounded(readerAndOutput.reader).getSplitBacklogBytes();
                if (pendingBytesForReader != -1L) {
                  pendingBytes =
                      pendingBytes == -1L
                          ? pendingBytesForReader
                          : pendingBytes + pendingBytesForReader;
                }
              }
              return pendingBytes;
            });
  }

  private <CheckpointMarkT extends UnboundedSource.CheckpointMark> byte[] encodeCheckpointMark(
      Coder<CheckpointMarkT> coder, CheckpointMarkT checkpointMark) {

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      coder.encode(checkpointMark, baos);
      return baos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to encode checkpoint mark.", ioe);
    }
  }

  private <CheckpointMarkT extends UnboundedSource.CheckpointMark>
      Source.Reader<T> createUnboundedSourceReader(
          Source<T> beamSource, byte @Nullable [] splitState) throws IOException {
    UnboundedSource<T, CheckpointMarkT> unboundedSource =
        (UnboundedSource<T, CheckpointMarkT>) beamSource;
    Coder<CheckpointMarkT> coder = unboundedSource.getCheckpointMarkCoder();
    if (splitState == null) {
      return unboundedSource.createReader(pipelineOptions, null);
    } else {
      try (ByteArrayInputStream bais = new ByteArrayInputStream(splitState)) {
        return unboundedSource.createReader(pipelineOptions, coder.decode(bais));
      }
    }
  }

  private void finalizeSourceSplit(UnboundedSource.@Nullable CheckpointMark mark)
      throws IOException {
    if (mark != null) {
      mark.finalizeCheckpoint();
    }
  }
}

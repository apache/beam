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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import javax.annotation.Nonnull;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainerWithoutAccumulator;
import org.apache.beam.runners.flink.metrics.ReaderInvocationUtil;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.compat.FlinkSourceCompat;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.Counter;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract implementation of {@link SourceReader} which encapsulates {@link Source Beam Sources}
 * for data reading.
 *
 * <ol>
 *   <li>Idle timeout support.
 *   <li>Splits addition handling.
 *   <li>Split reader creation and management.
 *   <li>checkpoint management
 * </ol>
 *
 * <p>This implementation provides unified logic for both {@link BoundedSource} and {@link
 * UnboundedSource}. The subclasses are expected to only implement the {@link
 * #pollNext(ReaderOutput)} method.
 *
 * @param <OutputT> the output element type from the encapsulated {@link Source Beam sources.}
 */
public abstract class FlinkSourceReaderBase<T, OutputT>
    implements SourceReader<OutputT, FlinkSourceSplit<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceReaderBase.class);
  protected static final CompletableFuture<Void> AVAILABLE_NOW =
      CompletableFuture.completedFuture(null);

  protected final PipelineOptions pipelineOptions;
  protected final @Nullable Function<OutputT, Long> timestampExtractor;
  private final Queue<FlinkSourceSplit<T>> sourceSplits = new ArrayDeque<>();
  // This needs to be a ConcurrentHashMap because the metric retrieving thread may access it.
  private final ConcurrentMap<Integer, ReaderAndOutput> beamSourceReaders;
  protected final SourceReaderContext context;
  private final ScheduledExecutorService executor;

  protected final ReaderInvocationUtil<T, Source.Reader<T>> invocationUtil;
  protected final Counter numRecordsInCounter;
  protected final long idleTimeoutMs;
  private final CompletableFuture<Void> idleTimeoutFuture;
  private final AtomicReference<@Nullable Throwable> exception;
  private boolean idleTimeoutCountingDown;
  private final AtomicReference<CompletableFuture<Void>> waitingForSplitChangeFuture =
      new AtomicReference<>(new CompletableFuture<>());
  private boolean noMoreSplits;

  protected FlinkSourceReaderBase(
      String stepName,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      @Nullable Function<OutputT, Long> timestampExtractor) {
    this(
        stepName,
        Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "FlinkSource-Executor-Thread-" + context.getIndexOfSubtask())),
        context,
        pipelineOptions,
        timestampExtractor);
  }

  protected FlinkSourceReaderBase(
      String stepName,
      ScheduledExecutorService executor,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      @Nullable Function<OutputT, Long> timestampExtractor) {
    this.context = context;
    this.pipelineOptions = pipelineOptions;
    this.timestampExtractor = timestampExtractor;
    this.beamSourceReaders = new ConcurrentHashMap<>();
    this.exception = new AtomicReference<>();
    this.executor = executor;
    this.idleTimeoutMs =
        pipelineOptions.as(FlinkPipelineOptions.class).getShutdownSourcesAfterIdleMs();
    this.idleTimeoutFuture = new CompletableFuture<>();
    this.idleTimeoutCountingDown = false;
    this.numRecordsInCounter = FlinkSourceCompat.getNumRecordsInCounter(context);
    FlinkMetricContainerWithoutAccumulator metricsContainer =
        new FlinkMetricContainerWithoutAccumulator(context.metricGroup());
    this.invocationUtil = new ReaderInvocationUtil<>(stepName, pipelineOptions, metricsContainer);
  }

  @Override
  public void start() {}

  @Override
  public List<FlinkSourceSplit<T>> snapshotState(long checkpointId) {
    checkExceptionAndMaybeThrow();
    // Add all the source splits whose readers haven't been created.
    List<FlinkSourceSplit<T>> splitsState = new ArrayList<>(sourceSplits);

    // Add all the source splits being actively read.
    beamSourceReaders.forEach(
        (splitId, readerAndOutput) -> {
          try {
            FlinkSourceSplit<T> checkpoint = getReaderCheckpoint(splitId, readerAndOutput);
            splitsState.add(checkpoint);
          } catch (IOException e) {
            throw new IllegalStateException(
                String.format("Failed to get checkpoint for split %d", splitId), e);
          }
        });
    addSplitsToUnfinishedForCheckpoint(checkpointId, splitsState);
    return splitsState;
  }

  @Override
  public CompletableFuture<Void> isAvailable() {
    checkExceptionAndMaybeThrow();
    if (!sourceSplits.isEmpty() || !beamSourceReaders.isEmpty()) {
      // There are still live readers.
      CompletableFuture<Void> aliveReaderAvailableFuture = isAvailableForAliveReaders();
      // Regardless of whether there is data available from the alive readers, the
      // main thread needs to be woken up if there is a split change. Hence, we
      // need to combine the data available future with the split change future.
      if (waitingForSplitChangeFuture.get().isDone()) {
        waitingForSplitChangeFuture.set(new CompletableFuture<>());
      }
      return CompletableFuture.anyOf(aliveReaderAvailableFuture, waitingForSplitChangeFuture.get())
          .thenAccept(ignored -> {});
    } else if (noMoreSplits) {
      // All the splits have been read, wait for idle timeout.
      LOG.info("All splits have been read, waiting for shutdown timeout {}", idleTimeoutMs);
      checkIdleTimeoutAndMaybeStartCountdown();
      return idleTimeoutFuture;
    } else {
      // There are no live readers, waiting for new split assignments or no more splits
      // notification.
      if (waitingForSplitChangeFuture.get().isDone()) {
        waitingForSplitChangeFuture.set(new CompletableFuture<>());
      }
      return waitingForSplitChangeFuture.get();
    }
  }

  @Override
  public void notifyNoMoreSplits() {
    checkExceptionAndMaybeThrow();
    LOG.info("Received NoMoreSplits signal from enumerator.");
    noMoreSplits = true;
    waitingForSplitChangeFuture.get().complete(null);
  }

  @Override
  public void addSplits(List<FlinkSourceSplit<T>> splits) {
    checkExceptionAndMaybeThrow();
    LOG.info("Adding splits {}", splits);
    sourceSplits.addAll(splits);
    waitingForSplitChangeFuture.get().complete(null);
  }

  @Override
  public void close() throws Exception {
    for (ReaderAndOutput readerAndOutput : beamSourceReaders.values()) {
      readerAndOutput.reader.close();
    }
    executor.shutdown();
  }

  // ----------------- protected abstract methods ----------------------

  /**
   * This method needs to be overridden by subclasses to determine if data is available when there
   * are alive readers. For example, an unbounded source may not have any source split ready for
   * data emission even if all the sources are still alive. Whereas for the bounded source, data is
   * always available as long as there are alive readers.
   */
  protected abstract CompletableFuture<Void> isAvailableForAliveReaders();

  /** Create {@link FlinkSourceSplit} for given {@code splitId}. */
  protected abstract FlinkSourceSplit<T> getReaderCheckpoint(
      int splitId, ReaderAndOutput readerAndOutput) throws IOException;

  /** Create {@link Source.Reader} for given {@link FlinkSourceSplit}. */
  protected abstract Source.Reader<T> createReader(@Nonnull FlinkSourceSplit<T> sourceSplit)
      throws IOException;

  /**
   * To be overridden in unbounded reader. Notify the reader of created splits that will be part of
   * checkpoint. Will be processed during notifyCheckpointComplete to finalize the associated
   * CheckpointMarks.
   */
  protected void addSplitsToUnfinishedForCheckpoint(
      long checkpointId, List<FlinkSourceSplit<T>> splits) {
    // nop
  }

  // ----------------- protected helper methods for subclasses --------------------

  protected final Optional<ReaderAndOutput> createAndTrackNextReader() throws IOException {
    FlinkSourceSplit<T> sourceSplit = sourceSplits.poll();
    if (sourceSplit != null) {
      Source.Reader<T> reader = createReader(sourceSplit);
      ReaderAndOutput readerAndOutput = new ReaderAndOutput(sourceSplit.splitId(), reader, false);
      beamSourceReaders.put(sourceSplit.splitIndex(), readerAndOutput);
      return Optional.of(readerAndOutput);
    }
    return Optional.empty();
  }

  protected final void finishSplit(int splitIndex) throws IOException {
    ReaderAndOutput readerAndOutput = beamSourceReaders.remove(splitIndex);
    if (readerAndOutput != null) {
      LOG.info("Finished reading from split {}", readerAndOutput.splitId);
      readerAndOutput.reader.close();
    } else {
      throw new IllegalStateException(
          "SourceReader for split " + splitIndex + " should never be null!");
    }
  }

  protected final boolean checkIdleTimeoutAndMaybeStartCountdown() {
    if (idleTimeoutMs <= 0) {
      idleTimeoutFuture.complete(null);
    } else if (!idleTimeoutCountingDown) {
      scheduleTask(() -> idleTimeoutFuture.complete(null), idleTimeoutMs);
      idleTimeoutCountingDown = true;
    }
    return idleTimeoutFuture.isDone();
  }

  protected final boolean noMoreSplits() {
    return noMoreSplits;
  }

  protected void scheduleTask(Runnable runnable, long delayMs) {
    ignoreReturnValue(
        executor.schedule(new ErrorRecordingRunnable(runnable), delayMs, TimeUnit.MILLISECONDS));
  }

  protected void scheduleTaskAtFixedRate(Runnable runnable, long delayMs, long periodMs) {
    ignoreReturnValue(
        executor.scheduleAtFixedRate(
            new ErrorRecordingRunnable(runnable), delayMs, periodMs, TimeUnit.MILLISECONDS));
  }

  protected void execute(Runnable runnable) {
    executor.execute(new ErrorRecordingRunnable(runnable));
  }

  protected void recordException(Throwable e) {
    if (!exception.compareAndSet(null, e)) {
      Optional.ofNullable(exception.get()).ifPresent(exc -> exc.addSuppressed(e));
    }
  }

  protected void checkExceptionAndMaybeThrow() {
    if (exception.get() != null) {
      throw new RuntimeException("The source reader received exception.", exception.get());
    }
  }

  protected boolean hasException() {
    return exception.get() != null;
  }

  protected Collection<FlinkSourceSplit<T>> sourceSplits() {
    return Collections.unmodifiableCollection(sourceSplits);
  }

  protected Map<Integer, ReaderAndOutput> allReaders() {
    return Collections.unmodifiableMap(beamSourceReaders);
  }

  protected static void ignoreReturnValue(Object o) {
    // do nothing.
  }

  // -------------------- protected helper class ---------------------

  /** A wrapper for the reader and its associated information. */
  protected final class ReaderAndOutput {
    public final String splitId;
    public final Source.Reader<T> reader;
    private boolean started;
    private @Nullable SourceOutput<OutputT> outputForSplit;

    public ReaderAndOutput(String splitId, Source.Reader<T> reader, boolean started) {
      this.splitId = splitId;
      this.reader = reader;
      this.started = started;
      this.outputForSplit = null;
    }

    public SourceOutput<OutputT> getAndMaybeCreateSplitOutput(ReaderOutput<OutputT> output) {
      if (outputForSplit == null) {
        outputForSplit = output.createOutputForSplit(splitId);
      }
      return outputForSplit;
    }

    public boolean startOrAdvance() throws IOException {
      if (started) {
        return invocationUtil.invokeAdvance(reader);
      } else {
        started = true;
        return invocationUtil.invokeStart(reader);
      }
    }

    public @Nullable SourceOutput<OutputT> sourceOutput() {
      return outputForSplit;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("splitId", splitId)
          .add("reader", reader)
          .add("started", started)
          .toString();
    }
  }

  private final class ErrorRecordingRunnable implements Runnable {
    private final Runnable runnable;

    ErrorRecordingRunnable(Runnable r) {
      this.runnable = r;
    }

    @Override
    public void run() {
      try {
        runnable.run();
      } catch (Throwable t) {
        recordException(t);
      }
    }
  }
}

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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.construction.UnboundedReadFromBoundedSource;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.metrics.FlinkMetricContainer;
import org.apache.beam.runners.flink.metrics.ReaderInvocationUtil;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.utils.Workarounds;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.DefaultOperatorStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper for executing {@link UnboundedSource UnboundedSources} as a Flink Source. */
public class UnboundedSourceWrapper<OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends RichParallelSourceFunction<WindowedValue<ValueWithRecordId<OutputT>>>
    implements ProcessingTimeCallback,
        BeamStoppableFunction,
        CheckpointListener,
        CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceWrapper.class);

  private final String stepName;
  /** Keep the options so that we can initialize the localReaders. */
  private final SerializablePipelineOptions serializedOptions;

  /**
   * We are processing bounded data and should read from the sources sequentially instead of reading
   * round-robin from all the sources. In case of file sources this avoids having too many open
   * files/connections at once.
   */
  private final boolean isConvertedBoundedSource;

  /** For snapshot and restore. */
  private final KvCoder<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>
      checkpointCoder;

  /**
   * The split sources. We split them in the constructor to ensure that all parallel sources are
   * consistent about the split sources.
   */
  private final List<? extends UnboundedSource<OutputT, CheckpointMarkT>> splitSources;

  /** The idle time before we the source shuts down. */
  private final long idleTimeoutMs;

  /** The local split sources. Assigned at runtime when the wrapper is executed in parallel. */
  private transient List<UnboundedSource<OutputT, CheckpointMarkT>> localSplitSources;

  /**
   * The local split readers. Assigned at runtime when the wrapper is executed in parallel. Make it
   * a field so that we can access it in {@link #onProcessingTime(long)} for emitting watermarks.
   */
  private transient List<UnboundedSource.UnboundedReader<OutputT>> localReaders;

  /**
   * Flag to indicate whether the source is running. Initialize here and not in run() to prevent
   * races where we cancel a job before run() is ever called or run() is called after cancel().
   */
  private volatile boolean isRunning = true;

  /**
   * Make it a field so that we can access it in {@link #onProcessingTime(long)} for registering new
   * triggers.
   */
  private transient StreamingRuntimeContext runtimeContext;

  /**
   * Make it a field so that we can access it in {@link #onProcessingTime(long)} for emitting
   * watermarks.
   */
  private transient SourceContext<WindowedValue<ValueWithRecordId<OutputT>>> context;

  /** Pending checkpoints which have not been acknowledged yet. */
  private transient LinkedHashMap<Long, List<CheckpointMarkT>> pendingCheckpoints;
  /** Keep a maximum of 32 checkpoints for {@code CheckpointMark.finalizeCheckpoint()}. */
  private static final int MAX_NUMBER_PENDING_CHECKPOINTS = 32;

  private transient ListState<
          KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
      stateForCheckpoint;

  /** false if checkpointCoder is null or no restore state by starting first. */
  private transient boolean isRestored = false;

  /** Flag to indicate whether all readers have reached the maximum watermark. */
  private transient boolean maxWatermarkReached;

  /** Metrics container which will be reported as Flink accumulators at the end of the job. */
  private transient FlinkMetricContainer metricContainer;

  @SuppressWarnings("unchecked")
  public UnboundedSourceWrapper(
      String stepName,
      PipelineOptions pipelineOptions,
      UnboundedSource<OutputT, CheckpointMarkT> source,
      int parallelism)
      throws Exception {
    this.stepName = stepName;
    this.serializedOptions = new SerializablePipelineOptions(pipelineOptions);
    this.isConvertedBoundedSource =
        source instanceof UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;

    if (source.requiresDeduping()) {
      LOG.warn("Source {} requires deduping but Flink runner doesn't support this yet.", source);
    }

    Coder<CheckpointMarkT> checkpointMarkCoder = source.getCheckpointMarkCoder();
    if (checkpointMarkCoder == null) {
      LOG.info("No CheckpointMarkCoder specified for this source. Won't create snapshots.");
      checkpointCoder = null;
    } else {

      Coder<? extends UnboundedSource<OutputT, CheckpointMarkT>> sourceCoder =
          (Coder) SerializableCoder.of(new TypeDescriptor<UnboundedSource>() {});

      checkpointCoder = KvCoder.of(sourceCoder, checkpointMarkCoder);
    }

    // get the splits early. we assume that the generated splits are stable,
    // this is necessary so that the mapping of state to source is correct
    // when restoring
    splitSources = source.split(parallelism, pipelineOptions);

    FlinkPipelineOptions options = pipelineOptions.as(FlinkPipelineOptions.class);
    idleTimeoutMs = options.getShutdownSourcesAfterIdleMs();
  }

  /** Initialize and restore state before starting execution of the source. */
  @Override
  public void open(Configuration parameters) throws Exception {
    FileSystems.setDefaultPipelineOptions(serializedOptions.get());
    runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
    metricContainer = new FlinkMetricContainer(runtimeContext);

    // figure out which split sources we're responsible for
    int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
    int numSubtasks = runtimeContext.getNumberOfParallelSubtasks();

    localSplitSources = new ArrayList<>();
    localReaders = new ArrayList<>();

    pendingCheckpoints = new LinkedHashMap<>();

    if (isRestored) {
      // restore the splitSources from the checkpoint to ensure consistent ordering
      for (KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> restored :
          stateForCheckpoint.get()) {
        localSplitSources.add(restored.getKey());
        localReaders.add(
            restored.getKey().createReader(serializedOptions.get(), restored.getValue()));
      }
    } else {
      // initialize localReaders and localSources from scratch
      for (int i = 0; i < splitSources.size(); i++) {
        if (i % numSubtasks == subtaskIndex) {
          UnboundedSource<OutputT, CheckpointMarkT> source = splitSources.get(i);
          UnboundedSource.UnboundedReader<OutputT> reader =
              source.createReader(serializedOptions.get(), null);
          localSplitSources.add(source);
          localReaders.add(reader);
        }
      }
    }

    LOG.info(
        "Unbounded Flink Source {}/{} is reading from sources: {}",
        subtaskIndex + 1,
        numSubtasks,
        localSplitSources);
  }

  @Override
  public void run(SourceContext<WindowedValue<ValueWithRecordId<OutputT>>> ctx) throws Exception {

    context = ctx;

    ReaderInvocationUtil<OutputT, UnboundedSource.UnboundedReader<OutputT>> readerInvoker =
        new ReaderInvocationUtil<>(stepName, serializedOptions.get(), metricContainer);

    setNextWatermarkTimer(this.runtimeContext);

    if (localReaders.isEmpty()) {
      // It can happen when value of parallelism is greater than number of IO readers (for example,
      // parallelism is 2 and number of Kafka topic partitions is 1). In this case, we just fall
      // through to idle this executor.
      LOG.info("Number of readers is 0 for this task executor, idle");
      // Do nothing here but still execute the rest of the source logic
    } else if (isConvertedBoundedSource) {

      // We read sequentially from all bounded sources
      for (int i = 0; i < localReaders.size() && isRunning; i++) {
        UnboundedSource.UnboundedReader<OutputT> reader = localReaders.get(i);

        synchronized (ctx.getCheckpointLock()) {
          boolean dataAvailable = readerInvoker.invokeStart(reader);
          if (dataAvailable) {
            emitElement(ctx, reader);
          }
        }

        boolean dataAvailable;
        do {
          synchronized (ctx.getCheckpointLock()) {
            dataAvailable = readerInvoker.invokeAdvance(reader);

            if (dataAvailable) {
              emitElement(ctx, reader);
            }
          }
        } while (dataAvailable && isRunning);
      }
    } else {
      // Read from multiple unbounded sources,
      // loop through them and sleep if none of them had any data

      int numReaders = localReaders.size();
      int currentReader = 0;

      // start each reader and emit data if immediately available
      for (UnboundedSource.UnboundedReader<OutputT> reader : localReaders) {
        synchronized (ctx.getCheckpointLock()) {
          boolean dataAvailable = readerInvoker.invokeStart(reader);
          if (dataAvailable) {
            emitElement(ctx, reader);
          }
        }
      }

      // a flag telling us whether any of the localReaders had data
      // if no reader had data, sleep for bit
      boolean hadData = false;
      while (isRunning && !maxWatermarkReached) {
        UnboundedSource.UnboundedReader<OutputT> reader = localReaders.get(currentReader);

        synchronized (ctx.getCheckpointLock()) {
          if (readerInvoker.invokeAdvance(reader)) {
            emitElement(ctx, reader);
            hadData = true;
          }
        }

        currentReader = (currentReader + 1) % numReaders;
        if (currentReader == 0 && !hadData) {
          // We have visited all the readers and none had data
          // Wait for a bit and check if more data is available
          Thread.sleep(50);
        } else if (currentReader == 0) {
          // Reset the flag for another round across the readers
          hadData = false;
        }
      }
    }

    ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
    finalizeSource();
  }

  private void finalizeSource() {
    // do nothing, but still look busy ...
    // we can't return here since Flink requires that all operators stay up,
    // otherwise checkpointing would not work correctly anymore
    //
    // See https://issues.apache.org/jira/browse/FLINK-2491 for progress on this issue
    long idleStart = System.currentTimeMillis();
    while (isRunning && System.currentTimeMillis() - idleStart < idleTimeoutMs) {
      try {
        // Flink will interrupt us at some point
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        if (!isRunning) {
          // restore the interrupted state, and fall through the loop
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Emit the current element from the given Reader. The reader is guaranteed to have data. */
  private void emitElement(
      SourceContext<WindowedValue<ValueWithRecordId<OutputT>>> ctx,
      UnboundedSource.UnboundedReader<OutputT> reader) {
    // make sure that reader state update and element emission are atomic
    // with respect to snapshots
    OutputT item = reader.getCurrent();
    byte[] recordId = reader.getCurrentRecordId();
    Instant timestamp = reader.getCurrentTimestamp();

    WindowedValue<ValueWithRecordId<OutputT>> windowedValue =
        WindowedValue.of(
            new ValueWithRecordId<>(item, recordId),
            timestamp,
            GlobalWindow.INSTANCE,
            PaneInfo.NO_FIRING);
    ctx.collect(windowedValue);
  }

  @Override
  public void close() throws Exception {
    try {
      if (metricContainer != null) {
        metricContainer.registerMetricsForPipelineResult();
      }
      super.close();
      if (localReaders != null) {
        for (UnboundedSource.UnboundedReader<OutputT> reader : localReaders) {
          reader.close();
        }
      }
    } finally {
      Workarounds.deleteStaticCaches();
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void stop() {
    isRunning = false;
  }

  // ------------------------------------------------------------------------
  //  Checkpoint and restore
  // ------------------------------------------------------------------------

  @Override
  public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
    if (!isRunning) {
      LOG.debug("snapshotState() called on closed source");
    } else {

      if (checkpointCoder == null) {
        // no checkpoint coder available in this source
        return;
      }

      stateForCheckpoint.clear();

      long checkpointId = functionSnapshotContext.getCheckpointId();

      // we checkpoint the sources along with the CheckpointMarkT to ensure
      // than we have a correct mapping of checkpoints to sources when
      // restoring
      List<CheckpointMarkT> checkpointMarks = new ArrayList<>(localSplitSources.size());

      for (int i = 0; i < localSplitSources.size(); i++) {
        UnboundedSource<OutputT, CheckpointMarkT> source = localSplitSources.get(i);
        UnboundedSource.UnboundedReader<OutputT> reader = localReaders.get(i);

        @SuppressWarnings("unchecked")
        CheckpointMarkT mark = (CheckpointMarkT) reader.getCheckpointMark();
        checkpointMarks.add(mark);
        KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> kv = KV.of(source, mark);
        stateForCheckpoint.add(kv);
      }

      // cleanup old pending checkpoints and add new checkpoint
      int diff = pendingCheckpoints.size() - MAX_NUMBER_PENDING_CHECKPOINTS;
      if (diff >= 0) {
        for (Iterator<Long> iterator = pendingCheckpoints.keySet().iterator(); diff >= 0; diff--) {
          iterator.next();
          iterator.remove();
        }
      }
      pendingCheckpoints.put(checkpointId, checkpointMarks);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {
    if (checkpointCoder == null) {
      // no checkpoint coder available in this source
      return;
    }

    OperatorStateStore stateStore = context.getOperatorStateStore();
    @SuppressWarnings("unchecked")
    CoderTypeInformation<KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
        typeInformation = (CoderTypeInformation) new CoderTypeInformation<>(checkpointCoder);
    stateForCheckpoint =
        stateStore.getOperatorState(
            new ListStateDescriptor<>(
                DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
                typeInformation.createSerializer(new ExecutionConfig())));

    if (context.isRestored()) {
      isRestored = true;
      LOG.info("Restoring state in the UnboundedSourceWrapper.");
    } else {
      LOG.info("No restore state for UnboundedSourceWrapper.");
    }
  }

  @Override
  public void onProcessingTime(long timestamp) {
    if (this.isRunning) {
      synchronized (context.getCheckpointLock()) {
        // find minimum watermark over all localReaders
        long watermarkMillis = Long.MAX_VALUE;
        for (UnboundedSource.UnboundedReader<OutputT> reader : localReaders) {
          Instant watermark = reader.getWatermark();
          if (watermark != null) {
            watermarkMillis = Math.min(watermark.getMillis(), watermarkMillis);
          }
        }
        context.emitWatermark(new Watermark(watermarkMillis));

        if (watermarkMillis < BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis()) {
          setNextWatermarkTimer(this.runtimeContext);
        } else {
          this.maxWatermarkReached = true;
        }
      }
    }
  }

  // the callback is ourselves so there is nothing meaningful we can do with the ScheduledFuture
  @SuppressWarnings("FutureReturnValueIgnored")
  private void setNextWatermarkTimer(StreamingRuntimeContext runtime) {
    if (this.isRunning) {
      long watermarkInterval = runtime.getExecutionConfig().getAutoWatermarkInterval();
      synchronized (context.getCheckpointLock()) {
        long currentProcessingTime = runtime.getProcessingTimeService().getCurrentProcessingTime();
        if (currentProcessingTime < Long.MAX_VALUE) {
          long nextTriggerTime = currentProcessingTime + watermarkInterval;
          if (nextTriggerTime < currentProcessingTime) {
            // overflow, just trigger once for the max timestamp
            nextTriggerTime = Long.MAX_VALUE;
          }
          runtime.getProcessingTimeService().registerTimer(nextTriggerTime, this);
        }
      }
    }
  }

  /** Visible so that we can check this in tests. Must not be used for anything else. */
  @VisibleForTesting
  public List<? extends UnboundedSource<OutputT, CheckpointMarkT>> getSplitSources() {
    return splitSources;
  }

  /** Visible so that we can check this in tests. Must not be used for anything else. */
  @VisibleForTesting
  List<? extends UnboundedSource<OutputT, CheckpointMarkT>> getLocalSplitSources() {
    return localSplitSources;
  }

  /** Visible so that we can check this in tests. Must not be used for anything else. */
  @VisibleForTesting
  List<UnboundedSource.UnboundedReader<OutputT>> getLocalReaders() {
    return localReaders;
  }

  /** Visible so that we can check this in tests. Must not be used for anything else. */
  @VisibleForTesting
  boolean isRunning() {
    return isRunning;
  }

  /**
   * Visible so that we can set this in tests. This is only set in the run method which is
   * inconvenient for the tests where the context is assumed to be set when run is called. Must not
   * be used for anything else.
   */
  @VisibleForTesting
  public void setSourceContext(SourceContext<WindowedValue<ValueWithRecordId<OutputT>>> ctx) {
    context = ctx;
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {

    List<CheckpointMarkT> checkpointMarks = pendingCheckpoints.get(checkpointId);

    if (checkpointMarks != null) {

      // remove old checkpoints including the current one
      Iterator<Long> iterator = pendingCheckpoints.keySet().iterator();
      long currentId;
      do {
        currentId = iterator.next();
        iterator.remove();
      } while (currentId != checkpointId);

      // confirm all marks
      for (CheckpointMarkT mark : checkpointMarks) {
        mark.finalizeCheckpoint();
      }
    }
  }
}

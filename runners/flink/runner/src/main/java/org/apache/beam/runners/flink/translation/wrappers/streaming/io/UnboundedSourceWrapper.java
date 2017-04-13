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

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.StoppableFunction;
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

/**
 * Wrapper for executing {@link UnboundedSource UnboundedSources} as a Flink Source.
 */
public class UnboundedSourceWrapper<
    OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends RichParallelSourceFunction<WindowedValue<OutputT>>
    implements ProcessingTimeCallback, StoppableFunction,
    CheckpointListener, CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceWrapper.class);

  /**
   * Keep the options so that we can initialize the localReaders.
   */
  private final SerializedPipelineOptions serializedOptions;

  /**
   * For snapshot and restore.
   */
  private final KvCoder<
      ? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> checkpointCoder;

  /**
   * The split sources. We split them in the constructor to ensure that all parallel
   * sources are consistent about the split sources.
   */
  private final List<? extends UnboundedSource<OutputT, CheckpointMarkT>> splitSources;

  /**
   * The local split sources. Assigned at runtime when the wrapper is executed in parallel.
   */
  private transient List<UnboundedSource<OutputT, CheckpointMarkT>> localSplitSources;

  /**
   * The local split readers. Assigned at runtime when the wrapper is executed in parallel.
   * Make it a field so that we can access it in {@link #onProcessingTime(long)} for
   * emitting watermarks.
   */
  private transient List<UnboundedSource.UnboundedReader<OutputT>> localReaders;

  /**
   * Flag to indicate whether the source is running.
   * Initialize here and not in run() to prevent races where we cancel a job before run() is
   * ever called or run() is called after cancel().
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
  private transient SourceContext<WindowedValue<OutputT>> context;

  /**
   * Pending checkpoints which have not been acknowledged yet.
   */
  private transient LinkedHashMap<Long, List<CheckpointMarkT>> pendingCheckpoints;
  /**
   * Keep a maximum of 32 checkpoints for {@code CheckpointMark.finalizeCheckpoint()}.
   */
  private static final int MAX_NUMBER_PENDING_CHECKPOINTS = 32;

  private transient ListState<KV<? extends
      UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> stateForCheckpoint;

  /**
   * false if checkpointCoder is null or no restore state by starting first.
   */
  private transient boolean isRestored = false;

  @SuppressWarnings("unchecked")
  public UnboundedSourceWrapper(
      PipelineOptions pipelineOptions,
      UnboundedSource<OutputT, CheckpointMarkT> source,
      int parallelism) throws Exception {
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);

    if (source.requiresDeduping()) {
      LOG.warn("Source {} requires deduping but Flink runner doesn't support this yet.", source);
    }

    Coder<CheckpointMarkT> checkpointMarkCoder = source.getCheckpointMarkCoder();
    if (checkpointMarkCoder == null) {
      LOG.info("No CheckpointMarkCoder specified for this source. Won't create snapshots.");
      checkpointCoder = null;
    } else {

      Coder<? extends UnboundedSource<OutputT, CheckpointMarkT>> sourceCoder =
          (Coder) SerializableCoder.of(new TypeDescriptor<UnboundedSource>() {
          });

      checkpointCoder = KvCoder.of(sourceCoder, checkpointMarkCoder);
    }

    // get the splits early. we assume that the generated splits are stable,
    // this is necessary so that the mapping of state to source is correct
    // when restoring
    splitSources = source.split(parallelism, pipelineOptions);
  }


  /**
   * Initialize and restore state before starting execution of the source.
   */
  @Override
  public void open(Configuration parameters) throws Exception {
    runtimeContext = (StreamingRuntimeContext) getRuntimeContext();

    // figure out which split sources we're responsible for
    int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
    int numSubtasks = runtimeContext.getNumberOfParallelSubtasks();

    localSplitSources = new ArrayList<>();
    localReaders = new ArrayList<>();

    pendingCheckpoints = new LinkedHashMap<>();

    if (isRestored) {
      // restore the splitSources from the checkpoint to ensure consistent ordering
      for (KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> restored:
          stateForCheckpoint.get()) {
        localSplitSources.add(restored.getKey());
        localReaders.add(restored.getKey().createReader(
            serializedOptions.getPipelineOptions(), restored.getValue()));
      }
    } else {
      // initialize localReaders and localSources from scratch
      for (int i = 0; i < splitSources.size(); i++) {
        if (i % numSubtasks == subtaskIndex) {
          UnboundedSource<OutputT, CheckpointMarkT> source =
              splitSources.get(i);
          UnboundedSource.UnboundedReader<OutputT> reader =
              source.createReader(serializedOptions.getPipelineOptions(), null);
          localSplitSources.add(source);
          localReaders.add(reader);
        }
      }
    }

    LOG.info("Unbounded Flink Source {}/{} is reading from sources: {}",
        subtaskIndex,
        numSubtasks,
        localSplitSources);
  }

  @Override
  public void run(SourceContext<WindowedValue<OutputT>> ctx) throws Exception {

    context = ctx;

    if (localReaders.size() == 0) {
      // do nothing, but still look busy ...
      // also, output a Long.MAX_VALUE watermark since we know that we're not
      // going to emit anything
      // we can't return here since Flink requires that all operators stay up,
      // otherwise checkpointing would not work correctly anymore
      ctx.emitWatermark(new Watermark(Long.MAX_VALUE));

      // wait until this is canceled
      final Object waitLock = new Object();
      while (isRunning) {
        try {
          // Flink will interrupt us at some point
          //noinspection SynchronizationOnLocalVariableOrMethodParameter
          synchronized (waitLock) {
            // don't wait indefinitely, in case something goes horribly wrong
            waitLock.wait(1000);
          }
        } catch (InterruptedException e) {
          if (!isRunning) {
            // restore the interrupted state, and fall through the loop
            Thread.currentThread().interrupt();
          }
        }
      }
    } else if (localReaders.size() == 1) {
      // the easy case, we just read from one reader
      UnboundedSource.UnboundedReader<OutputT> reader = localReaders.get(0);

      boolean dataAvailable = reader.start();
      if (dataAvailable) {
        emitElement(ctx, reader);
      }

      setNextWatermarkTimer(this.runtimeContext);

      while (isRunning) {
        dataAvailable = reader.advance();

        if (dataAvailable)  {
          emitElement(ctx, reader);
        } else {
          Thread.sleep(50);
        }
      }
    } else {
      // a bit more complicated, we are responsible for several localReaders
      // loop through them and sleep if none of them had any data

      int numReaders = localReaders.size();
      int currentReader = 0;

      // start each reader and emit data if immediately available
      for (UnboundedSource.UnboundedReader<OutputT> reader : localReaders) {
        boolean dataAvailable = reader.start();
        if (dataAvailable) {
          emitElement(ctx, reader);
        }
      }

      // a flag telling us whether any of the localReaders had data
      // if no reader had data, sleep for bit
      boolean hadData = false;
      while (isRunning) {
        UnboundedSource.UnboundedReader<OutputT> reader = localReaders.get(currentReader);
        boolean dataAvailable = reader.advance();

        if (dataAvailable) {
          emitElement(ctx, reader);
          hadData = true;
        }

        currentReader = (currentReader + 1) % numReaders;
        if (currentReader == 0 && !hadData) {
          Thread.sleep(50);
        } else if (currentReader == 0) {
          hadData = false;
        }
      }

    }
  }

  /**
   * Emit the current element from the given Reader. The reader is guaranteed to have data.
   */
  private void emitElement(
      SourceContext<WindowedValue<OutputT>> ctx,
      UnboundedSource.UnboundedReader<OutputT> reader) {
    // make sure that reader state update and element emission are atomic
    // with respect to snapshots
    synchronized (ctx.getCheckpointLock()) {

      OutputT item = reader.getCurrent();
      Instant timestamp = reader.getCurrentTimestamp();

      WindowedValue<OutputT> windowedValue =
          WindowedValue.of(item, timestamp, GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
      ctx.collectWithTimestamp(windowedValue, timestamp.getMillis());
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
    if (localReaders != null) {
      for (UnboundedSource.UnboundedReader<OutputT> reader: localReaders) {
        reader.close();
      }
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
        KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> kv =
            KV.of(source, mark);
        stateForCheckpoint.add(kv);
      }

      // cleanup old pending checkpoints and add new checkpoint
      int diff = pendingCheckpoints.size() - MAX_NUMBER_PENDING_CHECKPOINTS;
      if (diff >= 0) {
        for (Iterator<Long> iterator = pendingCheckpoints.keySet().iterator();
             diff >= 0;
             diff--) {
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
    CoderTypeInformation<
        KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>>
        typeInformation = (CoderTypeInformation) new CoderTypeInformation<>(checkpointCoder);
    stateForCheckpoint = stateStore.getOperatorState(
        new ListStateDescriptor<>(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME,
            typeInformation.createSerializer(new ExecutionConfig())));

    if (context.isRestored()) {
      isRestored = true;
      LOG.info("Having restore state in the UnbounedSourceWrapper.");
    } else {
      LOG.info("No restore state for UnbounedSourceWrapper.");
    }
  }

  @Override
  public void onProcessingTime(long timestamp) throws Exception {
    if (this.isRunning) {
      synchronized (context.getCheckpointLock()) {
        // find minimum watermark over all localReaders
        long watermarkMillis = Long.MAX_VALUE;
        for (UnboundedSource.UnboundedReader<OutputT> reader: localReaders) {
          Instant watermark = reader.getWatermark();
          if (watermark != null) {
            watermarkMillis = Math.min(watermark.getMillis(), watermarkMillis);
          }
        }
        context.emitWatermark(new Watermark(watermarkMillis));
      }
      setNextWatermarkTimer(this.runtimeContext);
    }
  }

  private void setNextWatermarkTimer(StreamingRuntimeContext runtime) {
    if (this.isRunning) {
      long watermarkInterval =  runtime.getExecutionConfig().getAutoWatermarkInterval();
      long timeToNextWatermark = getTimeToNextWatermark(watermarkInterval);
      runtime.getProcessingTimeService().registerTimer(timeToNextWatermark, this);
    }
  }

  private long getTimeToNextWatermark(long watermarkInterval) {
    return System.currentTimeMillis() + watermarkInterval;
  }

  /**
   * Visible so that we can check this in tests. Must not be used for anything else.
   */
  @VisibleForTesting
  public List<? extends UnboundedSource<OutputT, CheckpointMarkT>> getSplitSources() {
    return splitSources;
  }

  /**
   * Visible so that we can check this in tests. Must not be used for anything else.
   */
  @VisibleForTesting
  public List<? extends UnboundedSource<OutputT, CheckpointMarkT>> getLocalSplitSources() {
    return localSplitSources;
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

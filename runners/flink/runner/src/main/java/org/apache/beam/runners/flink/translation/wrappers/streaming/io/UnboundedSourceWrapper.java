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

import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper for executing {@link UnboundedSource UnboundedSources} as a Flink Source.
 */
public class UnboundedSourceWrapper<
    OutputT, CheckpointMarkT extends UnboundedSource.CheckpointMark>
    extends RichParallelSourceFunction<WindowedValue<OutputT>>
    implements Triggerable, Checkpointed<byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceWrapper.class);

  /**
   * Keep the options so that we can initialize the readers.
   */
  private final SerializedPipelineOptions serializedOptions;

  /**
   * For snapshot and restore.
   */
  private final ListCoder<
      KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> checkpointCoder;

  /**
   * The split sources. We split them in the constructor to ensure that all parallel
   * sources are consistent about the split sources.
   */
  private List<? extends UnboundedSource<OutputT, CheckpointMarkT>> splitSources;

  /**
   * Make it a field so that we can access it in {@link #trigger(long)} for
   * emitting watermarks.
   */
  private transient List<UnboundedSource.UnboundedReader<OutputT>> readers;

  /**
   * Initialize here and not in run() to prevent races where we cancel a job before run() is
   * ever called or run() is called after cancel().
   */
  private volatile boolean isRunning = true;

  /**
   * Make it a field so that we can access it in {@link #trigger(long)} for registering new
   * triggers.
   */
  private transient StreamingRuntimeContext runtimeContext;

  /**
   * Make it a field so that we can access it in {@link #trigger(long)} for emitting
   * watermarks.
   */
  private transient StreamSource.ManualWatermarkContext<WindowedValue<OutputT>> context;

  /**
   * When restoring from a snapshot we put the restored sources/checkpoint marks here
   * and open in {@link #open(Configuration)}.
   */
  private transient List<
      KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> restoredState;

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
    Coder<? extends UnboundedSource<OutputT, CheckpointMarkT>> sourceCoder =
        SerializableCoder.of(new TypeDescriptor<UnboundedSource<OutputT, CheckpointMarkT>>() {});

    checkpointCoder = (ListCoder) ListCoder.of(KvCoder.of(sourceCoder, checkpointMarkCoder));

    // get the splits early. we assume that the generated splits are stable,
    // this is necessary so that the mapping of state to source is correct
    // when restoring
    splitSources = source.generateInitialSplits(parallelism, pipelineOptions);
  }

  @Override
  public void run(SourceContext<WindowedValue<OutputT>> ctx) throws Exception {
    if (!(ctx instanceof StreamSource.ManualWatermarkContext)) {
      throw new RuntimeException(
          "Cannot emit watermarks, this hints at a misconfiguration/bug.");
    }

    context = (StreamSource.ManualWatermarkContext<WindowedValue<OutputT>>) ctx;
    runtimeContext = (StreamingRuntimeContext) getRuntimeContext();

    // figure out which split sources we're responsible for
    int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

    List<UnboundedSource<OutputT, CheckpointMarkT>> localSources = new ArrayList<>();

    for (int i = 0; i < splitSources.size(); i++) {
      if (i % numSubtasks == subtaskIndex) {
        localSources.add(splitSources.get(i));
      }
    }

    LOG.info("Unbounded Flink Source {}/{} is reading from sources: {}",
        subtaskIndex,
        numSubtasks,
        localSources);

    readers = new ArrayList<>();
    if (restoredState != null) {

      // restore the splitSources from the checkpoint to ensure consistent ordering
      // do it using a transform because otherwise we would have to do
      // unchecked casts
      splitSources = Lists.transform(
          restoredState,
          new Function<
              KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>,
              UnboundedSource<OutputT, CheckpointMarkT>>() {
        @Override
        public UnboundedSource<OutputT, CheckpointMarkT> apply(
            KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> input) {
          return input.getKey();
        }
      });

      for (KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> restored:
          restoredState) {
        readers.add(
            restored.getKey().createReader(
                serializedOptions.getPipelineOptions(), restored.getValue()));
      }
      restoredState = null;
    } else {
      // initialize readers from scratch
      for (UnboundedSource<OutputT, CheckpointMarkT> source : localSources) {
        readers.add(source.createReader(serializedOptions.getPipelineOptions(), null));
      }
    }

    if (readers.size() == 0) {
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
            waitLock.wait();
          }
        } catch (InterruptedException e) {
          if (!isRunning) {
            // restore the interrupted state, and fall through the loop
            Thread.currentThread().interrupt();
          }
        }
      }
    } else if (readers.size() == 1) {
      // the easy case, we just read from one reader
      UnboundedSource.UnboundedReader<OutputT> reader = readers.get(0);

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
      // a bit more complicated, we are responsible for several readers
      // loop through them and sleep if none of them had any data

      int numReaders = readers.size();
      int currentReader = 0;

      // start each reader and emit data if immediately available
      for (UnboundedSource.UnboundedReader<OutputT> reader : readers) {
        boolean dataAvailable = reader.start();
        if (dataAvailable) {
          emitElement(ctx, reader);
        }
      }

      // a flag telling us whether any of the readers had data
      // if no reader had data, sleep for bit
      boolean hadData = false;
      while (isRunning) {
        UnboundedSource.UnboundedReader<OutputT> reader = readers.get(currentReader);
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
    if (readers != null) {
      for (UnboundedSource.UnboundedReader<OutputT> reader: readers) {
        reader.close();
      }
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public byte[] snapshotState(long l, long l1) throws Exception {
    // we checkpoint the sources along with the CheckpointMarkT to ensure
    // than we have a correct mapping of checkpoints to sources when
    // restoring
    List<KV<? extends UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT>> checkpoints =
        new ArrayList<>();

    for (int i = 0; i < splitSources.size(); i++) {
      UnboundedSource<OutputT, CheckpointMarkT> source = splitSources.get(i);
      UnboundedSource.UnboundedReader<OutputT> reader = readers.get(i);

      @SuppressWarnings("unchecked")
      CheckpointMarkT mark = (CheckpointMarkT) reader.getCheckpointMark();
      KV<UnboundedSource<OutputT, CheckpointMarkT>, CheckpointMarkT> kv =
          KV.of(source, mark);
      checkpoints.add(kv);
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      checkpointCoder.encode(checkpoints, baos, Coder.Context.OUTER);
      return baos.toByteArray();
    }
  }

  @Override
  public void restoreState(byte[] bytes) throws Exception {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes)) {
      restoredState = checkpointCoder.decode(bais, Coder.Context.OUTER);
    }
  }

  @Override
  public void trigger(long timestamp) throws Exception {
    if (this.isRunning) {
      synchronized (context.getCheckpointLock()) {
        // find minimum watermark over all readers
        long watermarkMillis = Long.MAX_VALUE;
        for (UnboundedSource.UnboundedReader<OutputT> reader: readers) {
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
      runtime.registerTimer(timeToNextWatermark, this);
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
}

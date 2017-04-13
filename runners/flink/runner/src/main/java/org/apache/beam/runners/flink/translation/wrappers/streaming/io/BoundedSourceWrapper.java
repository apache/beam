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
import java.util.List;
import org.apache.beam.runners.flink.translation.utils.SerializedPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper for executing {@link BoundedSource BoundedSources} as a Flink Source.
 */
public class BoundedSourceWrapper<OutputT>
    extends RichParallelSourceFunction<WindowedValue<OutputT>>
    implements StoppableFunction {

  private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceWrapper.class);

  /**
   * Keep the options so that we can initialize the readers.
   */
  private final SerializedPipelineOptions serializedOptions;

  /**
   * The split sources. We split them in the constructor to ensure that all parallel
   * sources are consistent about the split sources.
   */
  private List<? extends BoundedSource<OutputT>> splitSources;

  /**
   * Make it a field so that we can access it in {@link #close()}.
   */
  private transient List<BoundedSource.BoundedReader<OutputT>> readers;

  /**
   * Initialize here and not in run() to prevent races where we cancel a job before run() is
   * ever called or run() is called after cancel().
   */
  private volatile boolean isRunning = true;

  @SuppressWarnings("unchecked")
  public BoundedSourceWrapper(
      PipelineOptions pipelineOptions,
      BoundedSource<OutputT> source,
      int parallelism) throws Exception {
    this.serializedOptions = new SerializedPipelineOptions(pipelineOptions);

    long desiredBundleSize = source.getEstimatedSizeBytes(pipelineOptions) / parallelism;

    // get the splits early. we assume that the generated splits are stable,
    // this is necessary so that the mapping of state to source is correct
    // when restoring
    splitSources = source.split(desiredBundleSize, pipelineOptions);
  }

  @Override
  public void run(SourceContext<WindowedValue<OutputT>> ctx) throws Exception {

    // figure out which split sources we're responsible for
    int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();

    List<BoundedSource<OutputT>> localSources = new ArrayList<>();

    for (int i = 0; i < splitSources.size(); i++) {
      if (i % numSubtasks == subtaskIndex) {
        localSources.add(splitSources.get(i));
      }
    }

    LOG.info("Bounded Flink Source {}/{} is reading from sources: {}",
        subtaskIndex,
        numSubtasks,
        localSources);

    readers = new ArrayList<>();
    // initialize readers from scratch
    for (BoundedSource<OutputT> source : localSources) {
      readers.add(source.createReader(serializedOptions.getPipelineOptions()));
    }

   if (readers.size() == 1) {
      // the easy case, we just read from one reader
      BoundedSource.BoundedReader<OutputT> reader = readers.get(0);

      boolean dataAvailable = reader.start();
      if (dataAvailable) {
        emitElement(ctx, reader);
      }

      while (isRunning) {
        dataAvailable = reader.advance();

        if (dataAvailable)  {
          emitElement(ctx, reader);
        } else {
          break;
        }
      }
    } else {
      // a bit more complicated, we are responsible for several readers
      // loop through them and sleep if none of them had any data

      int currentReader = 0;

      // start each reader and emit data if immediately available
      for (BoundedSource.BoundedReader<OutputT> reader : readers) {
        boolean dataAvailable = reader.start();
        if (dataAvailable) {
          emitElement(ctx, reader);
        }
      }

      // a flag telling us whether any of the readers had data
      // if no reader had data, sleep for bit
      boolean hadData = false;
      while (isRunning && !readers.isEmpty()) {
        BoundedSource.BoundedReader<OutputT> reader = readers.get(currentReader);
        boolean dataAvailable = reader.advance();

        if (dataAvailable) {
          emitElement(ctx, reader);
          hadData = true;
        } else {
          readers.remove(currentReader);
          currentReader--;
          if (readers.isEmpty()) {
            break;
          }
        }

        currentReader = (currentReader + 1) % readers.size();
        if (currentReader == 0 && !hadData) {
          Thread.sleep(50);
        } else if (currentReader == 0) {
          hadData = false;
        }
      }

    }

    // emit final Long.MAX_VALUE watermark, just to be sure
    ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
  }

  /**
   * Emit the current element from the given Reader. The reader is guaranteed to have data.
   */
  private void emitElement(
      SourceContext<WindowedValue<OutputT>> ctx,
      BoundedSource.BoundedReader<OutputT> reader) {
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
      for (BoundedSource.BoundedReader<OutputT> reader: readers) {
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
    this.isRunning = false;
  }

  /**
   * Visible so that we can check this in tests. Must not be used for anything else.
   */
  @VisibleForTesting
  public List<? extends BoundedSource<OutputT>> getSplitSources() {
    return splitSources;
  }
}

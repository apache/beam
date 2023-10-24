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
package org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.bounded;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceReaderBase;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link org.apache.flink.api.connector.source.SourceReader SourceReader} implementation
 * that reads from the assigned {@link
 * org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceSplit
 * FlinkSourceSplits} by using Beam {@link org.apache.beam.sdk.io.BoundedSource.BoundedReader
 * BoundedReaders}.
 *
 * <p>This reader consumes the source splits one by one sequentially, instead of concurrently.
 *
 * @param <T> the output element type of the encapsulated Beam {@link
 *     org.apache.beam.sdk.io.BoundedSource.BoundedReader BoundedReader.}
 */
public class FlinkBoundedSourceReader<T> extends FlinkSourceReaderBase<T, WindowedValue<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkBoundedSourceReader.class);
  private @Nullable Source.Reader<T> currentReader;
  private int currentSplitId;

  public FlinkBoundedSourceReader(
      String stepName,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      @Nullable Function<WindowedValue<T>, Long> timestampExtractor) {
    super(stepName, context, pipelineOptions, timestampExtractor);
    currentSplitId = -1;
  }

  @VisibleForTesting
  protected FlinkBoundedSourceReader(
      String stepName,
      SourceReaderContext context,
      PipelineOptions pipelineOptions,
      ScheduledExecutorService executor,
      @Nullable Function<WindowedValue<T>, Long> timestampExtractor) {
    super(stepName, executor, context, pipelineOptions, timestampExtractor);
    currentSplitId = -1;
  }

  @Override
  public InputStatus pollNext(ReaderOutput<WindowedValue<T>> output) throws Exception {
    checkExceptionAndMaybeThrow();
    if (currentReader == null && !moveToNextNonEmptyReader()) {
      // Nothing to read for now.
      if (noMoreSplits() && checkIdleTimeoutAndMaybeStartCountdown()) {
        // All the source splits have been read and idle timeout has passed.
        LOG.info(
            "All splits have finished reading, and idle time {} ms has passed.", idleTimeoutMs);
        return InputStatus.END_OF_INPUT;
      } else {
        // This reader either hasn't received NoMoreSplitsEvent yet or it is waiting for idle
        // timeout.
        return InputStatus.NOTHING_AVAILABLE;
      }
    }
    Source.Reader<T> tempCurrentReader = currentReader;
    if (tempCurrentReader != null) {
      T record = tempCurrentReader.getCurrent();
      WindowedValue<T> windowedValue =
          WindowedValue.of(
              record,
              tempCurrentReader.getCurrentTimestamp(),
              GlobalWindow.INSTANCE,
              PaneInfo.NO_FIRING);
      if (timestampExtractor == null) {
        output.collect(windowedValue);
      } else {
        output.collect(windowedValue, timestampExtractor.apply(windowedValue));
      }
      numRecordsInCounter.inc();
      // If the advance() invocation throws exception here, the job will just fail over and read
      // everything again from
      // the beginning. So the failover granularity is the entire Flink job.
      if (!invocationUtil.invokeAdvance(tempCurrentReader)) {
        finishSplit(currentSplitId);
        currentReader = null;
        currentSplitId = -1;
        LOG.debug("Finished reading from {}", currentSplitId);
      }
      // Always return MORE_AVAILABLE here regardless of the availability of next record. If there
      // is no more
      // records available in the current split, the next invocation of pollNext() will handle that.
      return InputStatus.MORE_AVAILABLE;
    } else {
      throw new IllegalArgumentException(
          "If we reach here, the current beam reader should not be null");
    }
  }

  @Override
  protected CompletableFuture<Void> isAvailableForAliveReaders() {
    // For bounded source, as long as there are active readers, the data is available.
    return AVAILABLE_NOW;
  }

  // ------------------------- private helper methods --------------------------

  private boolean moveToNextNonEmptyReader() throws IOException {
    Optional<ReaderAndOutput> readerAndOutput;
    while ((readerAndOutput = createAndTrackNextReader()).isPresent()) {
      ReaderAndOutput rao = readerAndOutput.get();
      if (invocationUtil.invokeStart(rao.reader)) {
        currentSplitId = Integer.parseInt(rao.splitId);
        currentReader = rao.reader;
        return true;
      } else {
        finishSplit(Integer.parseInt(rao.splitId));
      }
    }
    return false;
  }
}

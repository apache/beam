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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Selects static or lazy assignment by estimated bounded-source size. */
final class SizeBasedFlinkSourceSplitEnumerator<T>
    implements SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(SizeBasedFlinkSourceSplitEnumerator.class);

  private final SplitEnumeratorContext<FlinkSourceSplit<T>> context;
  private final BoundedSource<T> boundedSource;
  private final PipelineOptions pipelineOptions;
  private final int numSplits;
  private final Map<Integer, Optional<String>> pendingSplitRequests;
  private final List<ReturnedSplits<T>> returnedSplits;

  private @Nullable SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> delegate;

  SizeBasedFlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      BoundedSource<T> boundedSource,
      PipelineOptions pipelineOptions,
      int numSplits) {
    this.context = context;
    this.boundedSource = boundedSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplitRequests = new LinkedHashMap<>();
    this.returnedSplits = new ArrayList<>();
  }

  @Override
  public void start() {
    context.callAsync(
        this::selectAndSplit,
        (initialState, error) -> {
          if (error != null) {
            throw new RuntimeException("Failed to select a source split assignment mode.", error);
          }

          SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> selectedDelegate =
              createDelegate(initialState);
          delegate = selectedDelegate;
          returnedSplits.forEach(
              returned -> selectedDelegate.addSplitsBack(returned.splits, returned.subtaskId));
          returnedSplits.clear();
          selectedDelegate.start();
          pendingSplitRequests.forEach(
              (subtaskId, hostname) ->
                  selectedDelegate.handleSplitRequest(subtaskId, hostname.orElse(null)));
          pendingSplitRequests.clear();
        });
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (delegate == null) {
      pendingSplitRequests.put(subtaskId, Optional.ofNullable(requesterHostname));
    } else {
      delegate.handleSplitRequest(subtaskId, requesterHostname);
    }
  }

  @Override
  public void addSplitsBack(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    if (delegate == null) {
      returnedSplits.add(new ReturnedSplits<>(new ArrayList<>(splits), subtaskId));
    } else {
      delegate.addSplitsBack(splits, subtaskId);
    }
  }

  @Override
  public void addReader(int subtaskId) {
    if (delegate != null) {
      delegate.addReader(subtaskId);
    }
  }

  @Override
  public FlinkSourceEnumeratorState<T> snapshotState(long checkpointId) throws Exception {
    if (delegate == null) {
      return new FlinkSourceEnumeratorState<>(
          FlinkSourceSplitAssignmentMode.UNDECIDED, new ArrayList<>());
    }
    return delegate.snapshotState(checkpointId);
  }

  @Override
  public void close() throws IOException {
    if (delegate != null) {
      delegate.close();
    }
  }

  private FlinkSourceEnumeratorState<T> selectAndSplit() throws Exception {
    long estimatedSizeBytes =
        FlinkSourceSplitUtils.estimateBoundedSourceSize(boundedSource, pipelineOptions);
    FlinkSourceSplitAssignmentMode selectedMode = selectAssignmentMode(estimatedSizeBytes);
    ArrayList<FlinkSourceSplit<T>> splits =
        FlinkSourceSplitUtils.splitBoundedSource(
            boundedSource, pipelineOptions, numSplits, estimatedSizeBytes);
    LOG.info(
        "Split bounded source {} into {} splits using {} assignment",
        boundedSource,
        splits.size(),
        selectedMode);
    return new FlinkSourceEnumeratorState<>(selectedMode, splits);
  }

  private FlinkSourceSplitAssignmentMode selectAssignmentMode(long estimatedSizeBytes) {
    long thresholdMb =
        pipelineOptions
            .as(FlinkPipelineOptions.class)
            .getLazySourceSplitAssignmentMinSizeMbPerReader();
    if (thresholdMb <= 0) {
      throw new IllegalArgumentException(
          "Size-based source assignment requires a positive threshold, but received "
              + thresholdMb
              + ".");
    }
    if (estimatedSizeBytes < 0 || estimatedSizeBytes == Long.MAX_VALUE) {
      LOG.info(
          "Estimated size of bounded source {} is unknown. Using lazy split assignment.",
          boundedSource);
      return FlinkSourceSplitAssignmentMode.LAZY;
    }

    int sourceParallelism = context.currentParallelism();
    if (sourceParallelism <= 0) {
      throw new IllegalStateException(
          "Source parallelism must be positive, but was " + sourceParallelism + ".");
    }
    long estimatedBytesPerReader = estimatedSizeBytes / sourceParallelism;
    long thresholdBytes = FlinkSourceSplitUtils.mebibytesToBytes(thresholdMb);
    FlinkSourceSplitAssignmentMode selectedMode =
        estimatedBytesPerReader >= thresholdBytes
            ? FlinkSourceSplitAssignmentMode.LAZY
            : FlinkSourceSplitAssignmentMode.STATIC;
    LOG.info(
        "Using {} split assignment for bounded source {}: estimated size {} bytes, source "
            + "parallelism {}, estimated bytes per reader {}, lazy assignment threshold {} bytes",
        selectedMode,
        boundedSource,
        estimatedSizeBytes,
        sourceParallelism,
        estimatedBytesPerReader,
        thresholdBytes);
    return selectedMode;
  }

  private SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> createDelegate(
      FlinkSourceEnumeratorState<T> initialState) {
    if (initialState.getAssignmentMode() == FlinkSourceSplitAssignmentMode.LAZY) {
      return new LazyFlinkSourceSplitEnumerator<>(
          context, boundedSource, pipelineOptions, numSplits, initialState);
    }
    if (initialState.getAssignmentMode() == FlinkSourceSplitAssignmentMode.STATIC) {
      return new FlinkSourceSplitEnumerator<>(
          context, boundedSource, pipelineOptions, numSplits, initialState);
    }
    throw new IllegalArgumentException(
        "Cannot create a source enumerator for "
            + initialState.getAssignmentMode()
            + " assignment.");
  }

  private static final class ReturnedSplits<T> {
    private final List<FlinkSourceSplit<T>> splits;
    private final int subtaskId;

    private ReturnedSplits(List<FlinkSourceSplit<T>> splits, int subtaskId) {
      this.splits = splits;
      this.subtaskId = subtaskId;
    }
  }
}

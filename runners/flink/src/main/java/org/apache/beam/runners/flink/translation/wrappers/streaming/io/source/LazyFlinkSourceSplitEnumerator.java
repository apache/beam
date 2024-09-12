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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link org.apache.flink.api.connector.source.SplitEnumerator SplitEnumerator}
 * implementation that holds a Beam {@link Source} and does the following:
 *
 * <ul>
 *   <li>Split the Beam {@link Source} to desired number of splits.
 *   <li>Lazily assign the splits to the Flink Source Reader.
 * </ul>
 *
 * @param <T> The output type of the encapsulated Beam {@link Source}.
 */
public class LazyFlinkSourceSplitEnumerator<T>
    implements SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyFlinkSourceSplitEnumerator.class);
  private final SplitEnumeratorContext<FlinkSourceSplit<T>> context;
  private final Source<T> beamSource;
  private final PipelineOptions pipelineOptions;
  private final int numSplits;
  private final List<FlinkSourceSplit<T>> pendingSplits;

  public LazyFlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      Source<T> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits) {
    this.context = context;
    this.beamSource = beamSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplits = new ArrayList<>(numSplits);
  }

  @Override
  public void start() {
    try {
      LOG.info("Starting source {}", beamSource);
      List<? extends Source<T>> beamSplitSourceList = splitBeamSource();
      int i = 0;
      for (Source<T> beamSplitSource : beamSplitSourceList) {
        pendingSplits.add(new FlinkSourceSplit<>(i, beamSplitSource));
        i++;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void handleSplitRequest(int subtask, @Nullable String hostname) {
    if (!context.registeredReaders().containsKey(subtask)) {
      // reader failed between sending the request and now. skip this request.
      return;
    }

    if (LOG.isInfoEnabled()) {
      final String hostInfo =
          hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
      LOG.info("Subtask {} {} is requesting a file source split", subtask, hostInfo);
    }

    if (!pendingSplits.isEmpty()) {
      final FlinkSourceSplit<T> split = pendingSplits.remove(pendingSplits.size() - 1);
      context.assignSplit(split, subtask);
      LOG.info("Assigned split to subtask {} : {}", subtask, split);
    } else {
      context.signalNoMoreSplits(subtask);
      LOG.info("No more splits available for subtask {}", subtask);
    }
  }

  @Override
  public void addSplitsBack(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    LOG.info("Adding splits {} back from subtask {}", splits, subtaskId);
    pendingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // this source is purely lazy-pull-based, nothing to do upon registration
  }

  @Override
  public Map<Integer, List<FlinkSourceSplit<T>>> snapshotState(long checkpointId) throws Exception {
    LOG.info("Taking snapshot for checkpoint {}", checkpointId);
    return snapshotState();
  }

  public Map<Integer, List<FlinkSourceSplit<T>>> snapshotState() throws Exception {
    // For type compatibility reasons, we return a Map but we do not actually care about the key
    Map<Integer, List<FlinkSourceSplit<T>>> state = new HashMap<>(1);
    state.put(1, pendingSplits);
    return state;
  }

  @Override
  public void close() throws IOException {
    // NoOp
  }

  private long getDesiredSizeBytes(int numSplits, BoundedSource<T> boundedSource) throws Exception {
    long totalSize = boundedSource.getEstimatedSizeBytes(pipelineOptions);
    long defaultSplitSize = totalSize / numSplits;
    long maxSplitSize = 0;
    if (pipelineOptions != null) {
      maxSplitSize = pipelineOptions.as(FlinkPipelineOptions.class).getFileInputSplitMaxSizeMB();
    }
    if (beamSource instanceof FileBasedSource && maxSplitSize > 0) {
      // Most of the time parallelism is < number of files in source.
      // Each file becomes a unique split which commonly create skew.
      // This limits the size of splits to reduce skew.
      return Math.min(defaultSplitSize, maxSplitSize * 1024 * 1024);
    } else {
      return defaultSplitSize;
    }
  }

  // -------------- Private helper methods ----------------------
  private List<? extends Source<T>> splitBeamSource() throws Exception {
    if (beamSource instanceof BoundedSource) {
      BoundedSource<T> boundedSource = (BoundedSource<T>) beamSource;
      long desiredSizeBytes = getDesiredSizeBytes(numSplits, boundedSource);
      List<? extends BoundedSource<T>> splits =
          ((BoundedSource<T>) beamSource).split(desiredSizeBytes, pipelineOptions);
      LOG.info("Split bounded source {} in {} splits", beamSource, splits.size());
      return splits;
    } else if (beamSource instanceof UnboundedSource) {
      List<? extends UnboundedSource<T, ?>> splits =
          ((UnboundedSource<T, ?>) beamSource).split(numSplits, pipelineOptions);
      LOG.info("Split source {} to {} splits", beamSource, splits);
      return splits;
    } else {
      throw new IllegalStateException("Unknown source type " + beamSource.getClass());
    }
  }
}

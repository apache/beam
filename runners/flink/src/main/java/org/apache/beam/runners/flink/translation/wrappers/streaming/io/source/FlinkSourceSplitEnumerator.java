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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink {@link org.apache.flink.api.connector.source.SplitEnumerator SplitEnumerator}
 * implementation that holds a Beam {@link Source} and does the following:
 *
 * <ul>
 *   <li>Split the Beam {@link Source} to desired number of splits.
 *   <li>Assign the splits to the Flink Source Reader.
 * </ul>
 *
 * <p>Note that at this point, this class has a static round-robin split assignment strategy.
 *
 * @param <T> The output type of the encapsulated Beam {@link Source}.
 */
public class FlinkSourceSplitEnumerator<T>
    implements SplitEnumerator<FlinkSourceSplit<T>, Map<Integer, List<FlinkSourceSplit<T>>>> {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkSourceSplitEnumerator.class);
  private final SplitEnumeratorContext<FlinkSourceSplit<T>> context;
  private final Source<T> beamSource;
  private final PipelineOptions pipelineOptions;
  private final int numSplits;
  private final Map<Integer, List<FlinkSourceSplit<T>>> pendingSplits;
  private boolean splitsInitialized;

  public FlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      Source<T> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits) {
    this.context = context;
    this.beamSource = beamSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplits = new HashMap<>(numSplits);
    this.splitsInitialized = false;
  }

  @Override
  public void start() {
    context.callAsync(
        () -> {
          try {
            LOG.info("Starting source {}", beamSource);
            List<? extends Source<T>> beamSplitSourceList = splitBeamSource();
            Map<Integer, List<FlinkSourceSplit<T>>> flinkSourceSplitsList = new HashMap<>();
            int i = 0;
            for (Source<T> beamSplitSource : beamSplitSourceList) {
              int targetSubtask = i % context.currentParallelism();
              List<FlinkSourceSplit<T>> splitsForTask =
                  flinkSourceSplitsList.computeIfAbsent(
                      targetSubtask, ignored -> new ArrayList<>());
              splitsForTask.add(new FlinkSourceSplit<>(i, beamSplitSource));
              i++;
            }
            return flinkSourceSplitsList;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        },
        (sourceSplits, error) -> {
          if (error != null) {
            throw new RuntimeException("Failed to start source enumerator.", error);
          } else {
            pendingSplits.putAll(sourceSplits);
            splitsInitialized = true;
            sendPendingSplitsToSourceReaders();
          }
        });
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // Not used.
  }

  @Override
  public void addSplitsBack(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    LOG.info("Adding splits {} back from subtask {}", splits, subtaskId);
    List<FlinkSourceSplit<T>> splitsForSubtask =
        pendingSplits.computeIfAbsent(subtaskId, ignored -> new ArrayList<>());
    splitsForSubtask.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    List<FlinkSourceSplit<T>> splitsForSubtask = pendingSplits.remove(subtaskId);
    if (splitsForSubtask != null) {
      assignSplitsAndLog(splitsForSubtask, subtaskId);
    } else {
      if (splitsInitialized) {
        LOG.info("There is no split for subtask {}. Signaling no more splits.", subtaskId);
        context.signalNoMoreSplits(subtaskId);
      }
    }
  }

  @Override
  public Map<Integer, List<FlinkSourceSplit<T>>> snapshotState(long checkpointId) throws Exception {
    LOG.info("Taking snapshot for checkpoint {}", checkpointId);
    return pendingSplits;
  }

  @Override
  public void close() throws IOException {
    // NoOp
  }

  // -------------- Private helper methods ----------------------
  private List<? extends Source<T>> splitBeamSource() throws Exception {
    if (beamSource instanceof BoundedSource) {
      BoundedSource<T> boundedSource = (BoundedSource<T>) beamSource;
      long desiredSizeBytes = boundedSource.getEstimatedSizeBytes(pipelineOptions) / numSplits;
      return boundedSource.split(desiredSizeBytes, pipelineOptions);
    } else if (beamSource instanceof UnboundedSource) {
      List<? extends UnboundedSource<T, ?>> splits =
          ((UnboundedSource<T, ?>) beamSource).split(numSplits, pipelineOptions);
      LOG.info("Split source {} to {} splits", beamSource, splits);
      return splits;
    } else {
      throw new IllegalStateException("Unknown source type " + beamSource.getClass());
    }
  }

  private void sendPendingSplitsToSourceReaders() {
    Iterator<Map.Entry<Integer, List<FlinkSourceSplit<T>>>> splitIter =
        pendingSplits.entrySet().iterator();
    while (splitIter.hasNext()) {
      Map.Entry<Integer, List<FlinkSourceSplit<T>>> entry = splitIter.next();
      int readerIndex = entry.getKey();
      int targetSubtask = readerIndex % context.currentParallelism();
      if (context.registeredReaders().containsKey(targetSubtask)) {
        assignSplitsAndLog(entry.getValue(), targetSubtask);
        splitIter.remove();
      }
    }
  }

  private void assignSplitsAndLog(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    context.signalNoMoreSplits(subtaskId);
    LOG.info("Assigned splits {} to subtask {}", splits, subtaskId);
  }
}

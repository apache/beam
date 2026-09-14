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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceEnumeratorState.AssignmentMode;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Splits a Beam source and assigns its splits to Flink source readers round-robin. */
public class FlinkSourceSplitEnumerator<T>
    implements SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> {
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
    this(context, beamSource, pipelineOptions, numSplits, null);
  }

  public FlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      Source<T> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits,
      @Nullable FlinkSourceEnumeratorState<T> restoredState) {
    this.context = context;
    this.beamSource = beamSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplits = new HashMap<>(numSplits);
    this.splitsInitialized = restoredState != null;

    if (restoredState != null) {
      if (restoredState.getAssignmentMode() != AssignmentMode.STATIC) {
        throw new IllegalArgumentException(
            "Cannot restore the static source enumerator from "
                + restoredState.getAssignmentMode()
                + " state.");
      }
      int parallelism = context.currentParallelism();
      for (FlinkSourceSplit<T> split : restoredState.getPendingSplits()) {
        int targetSubtask = split.splitIndex() % parallelism;
        pendingSplits.computeIfAbsent(targetSubtask, ignored -> new ArrayList<>()).add(split);
      }
    }

    LOG.info(
        "Created static source enumerator with parallelism {}, source {}, numSplits {}, "
            + "initialized {}",
        context.currentParallelism(),
        beamSource,
        numSplits,
        splitsInitialized);
  }

  @Override
  public void start() {
    if (!splitsInitialized) {
      initializeSplits();
    } else {
      sendPendingSplitsToSourceReaders();
    }
  }

  private void initializeSplits() {
    context.callAsync(
        this::splitBeamSource,
        (sourceSplits, error) -> {
          if (error != null) {
            throw new RuntimeException("Failed to start source enumerator.", error);
          }
          prepareAssignments(sourceSplits);
          splitsInitialized = true;
          sendPendingSplitsToSourceReaders();
        });
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    // Static assignment happens when readers register.
  }

  @Override
  public void addSplitsBack(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    LOG.info("Adding splits {} back from subtask {}", splits, subtaskId);
    pendingSplits.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    List<FlinkSourceSplit<T>> splitsForSubtask = pendingSplits.remove(subtaskId);
    if (splitsForSubtask != null) {
      assignSplitsAndLog(splitsForSubtask, subtaskId);
    } else if (splitsInitialized) {
      LOG.info("There is no split for subtask {}. Signaling no more splits.", subtaskId);
      context.signalNoMoreSplits(subtaskId);
    }
  }

  @Override
  public FlinkSourceEnumeratorState<T> snapshotState(long checkpointId) {
    LOG.info("Taking snapshot for checkpoint {}", checkpointId);
    ArrayList<FlinkSourceSplit<T>> checkpointSplits = new ArrayList<>();
    pendingSplits.values().forEach(checkpointSplits::addAll);
    AssignmentMode mode = splitsInitialized ? AssignmentMode.STATIC : AssignmentMode.UNDECIDED;
    return new FlinkSourceEnumeratorState<>(mode, checkpointSplits);
  }

  @Override
  public void close() throws IOException {
    // NoOp
  }

  private ArrayList<FlinkSourceSplit<T>> splitBeamSource() throws Exception {
    LOG.info("Starting source {}", beamSource);
    if (beamSource instanceof BoundedSource) {
      BoundedSource<T> boundedSource = (BoundedSource<T>) beamSource;
      long estimatedSizeBytes =
          FlinkSourceSplitUtils.estimateBoundedSourceSize(boundedSource, pipelineOptions);
      return FlinkSourceSplitUtils.splitBoundedSource(
          boundedSource, pipelineOptions, numSplits, estimatedSizeBytes);
    }
    if (beamSource instanceof UnboundedSource) {
      return FlinkSourceSplitUtils.splitUnboundedSource(
          (UnboundedSource<T, ?>) beamSource, pipelineOptions, numSplits);
    }
    throw new IllegalStateException("Unknown source type " + beamSource.getClass());
  }

  private void prepareAssignments(List<FlinkSourceSplit<T>> sourceSplits) {
    int parallelism = context.currentParallelism();
    for (FlinkSourceSplit<T> split : sourceSplits) {
      int targetSubtask = split.splitIndex() % parallelism;
      pendingSplits.computeIfAbsent(targetSubtask, ignored -> new ArrayList<>()).add(split);
    }
  }

  private void sendPendingSplitsToSourceReaders() {
    Set<Integer> assignedReaders = new HashSet<>();
    Iterator<Map.Entry<Integer, List<FlinkSourceSplit<T>>>> splitIter =
        pendingSplits.entrySet().iterator();
    while (splitIter.hasNext()) {
      Map.Entry<Integer, List<FlinkSourceSplit<T>>> entry = splitIter.next();
      int subtaskId = entry.getKey();
      if (context.registeredReaders().containsKey(subtaskId)) {
        assignSplitsAndLog(entry.getValue(), subtaskId);
        assignedReaders.add(subtaskId);
        splitIter.remove();
      }
    }

    for (int subtaskId : context.registeredReaders().keySet()) {
      if (!assignedReaders.contains(subtaskId) && !pendingSplits.containsKey(subtaskId)) {
        context.signalNoMoreSplits(subtaskId);
      }
    }
  }

  private void assignSplitsAndLog(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    context.assignSplits(new SplitsAssignment<>(Collections.singletonMap(subtaskId, splits)));
    context.signalNoMoreSplits(subtaskId);
    LOG.info("Assigned splits {} to subtask {}", splits, subtaskId);
  }
}

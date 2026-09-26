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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSourceEnumeratorState.AssignmentMode;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Splits a bounded Beam source and assigns one split for each reader request. */
@SuppressFBWarnings(
    value = "CT_CONSTRUCTOR_THROW",
    justification =
        "Public API, so it cannot be made final."
            + " A finalizer attack needs an attacker-supplied subclass on the classpath.")
public class LazyFlinkSourceSplitEnumerator<T>
    implements SplitEnumerator<FlinkSourceSplit<T>, FlinkSourceEnumeratorState<T>> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyFlinkSourceSplitEnumerator.class);

  private final SplitEnumeratorContext<FlinkSourceSplit<T>> context;
  private final Source<T> beamSource;
  private final PipelineOptions pipelineOptions;
  private final int numSplits;
  private final List<FlinkSourceSplit<T>> pendingSplits;
  private final Map<Integer, Optional<String>> pendingSplitRequests;

  private boolean splitsInitialized;

  public LazyFlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      Source<T> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits) {
    this(context, beamSource, pipelineOptions, numSplits, null);
  }

  public LazyFlinkSourceSplitEnumerator(
      SplitEnumeratorContext<FlinkSourceSplit<T>> context,
      Source<T> beamSource,
      PipelineOptions pipelineOptions,
      int numSplits,
      @Nullable FlinkSourceEnumeratorState<T> restoredState) {
    this.context = context;
    this.beamSource = beamSource;
    this.pipelineOptions = pipelineOptions;
    this.numSplits = numSplits;
    this.pendingSplits = new ArrayList<>(numSplits);
    this.pendingSplitRequests = new LinkedHashMap<>();
    this.splitsInitialized = restoredState != null;

    if (restoredState != null) {
      if (restoredState.getAssignmentMode() != AssignmentMode.LAZY) {
        throw new IllegalArgumentException(
            "Cannot restore the lazy source enumerator from "
                + restoredState.getAssignmentMode()
                + " state.");
      }
      pendingSplits.addAll(restoredState.getPendingSplits());
    }

    LOG.info(
        "Created lazy source enumerator with parallelism {}, source {}, numSplits {}, "
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
      sendPendingSplitRequests();
    }
  }

  private void initializeSplits() {
    context.callAsync(
        this::splitBeamSource,
        (sourceSplits, error) -> {
          if (error != null) {
            throw new RuntimeException("Failed to start source enumerator.", error);
          }
          pendingSplits.addAll(sourceSplits);
          splitsInitialized = true;
          sendPendingSplitRequests();
        });
  }

  @Override
  public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
    if (!context.registeredReaders().containsKey(subtaskId)) {
      return;
    }

    if (!splitsInitialized) {
      pendingSplitRequests.put(subtaskId, Optional.ofNullable(requesterHostname));
      return;
    }

    assignNextSplit(subtaskId, requesterHostname);
  }

  @Override
  public void addSplitsBack(List<FlinkSourceSplit<T>> splits, int subtaskId) {
    LOG.info("Adding splits {} back from subtask {}", splits, subtaskId);
    pendingSplits.addAll(splits);
  }

  @Override
  public void addReader(int subtaskId) {
    // Readers request lazy splits when they are ready for work.
  }

  @Override
  public FlinkSourceEnumeratorState<T> snapshotState(long checkpointId) {
    LOG.info("Taking snapshot for checkpoint {}", checkpointId);
    AssignmentMode mode = splitsInitialized ? AssignmentMode.LAZY : AssignmentMode.UNDECIDED;
    return new FlinkSourceEnumeratorState<>(mode, new ArrayList<>(pendingSplits));
  }

  @Override
  public void close() throws IOException {
    // NoOp
  }

  private ArrayList<FlinkSourceSplit<T>> splitBeamSource() throws Exception {
    if (!(beamSource instanceof BoundedSource)) {
      throw new IllegalStateException("Lazy assignment requires a bounded source.");
    }
    LOG.info("Starting source {}", beamSource);
    BoundedSource<T> boundedSource = (BoundedSource<T>) beamSource;
    long estimatedSizeBytes =
        FlinkSourceSplitUtils.estimateBoundedSourceSize(boundedSource, pipelineOptions);
    return FlinkSourceSplitUtils.splitBoundedSource(
        boundedSource, pipelineOptions, numSplits, estimatedSizeBytes);
  }

  private void sendPendingSplitRequests() {
    Map<Integer, Optional<String>> splitRequests = new LinkedHashMap<>(pendingSplitRequests);
    pendingSplitRequests.clear();
    splitRequests.forEach(
        (subtaskId, hostname) -> assignNextSplit(subtaskId, hostname.orElse(null)));
  }

  private void assignNextSplit(int subtaskId, @Nullable String requesterHostname) {
    if (!context.registeredReaders().containsKey(subtaskId)) {
      return;
    }
    if (pendingSplits.isEmpty()) {
      context.signalNoMoreSplits(subtaskId);
      LOG.info("No more splits available for subtask {}", subtaskId);
      return;
    }

    FlinkSourceSplit<T> split = pendingSplits.remove(pendingSplits.size() - 1);
    context.assignSplit(split, subtaskId);
    LOG.info("Assigned split to subtask {} on host {}: {}", subtaskId, requesterHostname, split);
  }
}

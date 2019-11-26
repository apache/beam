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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.DataflowOperationContext.DataflowExecutionState;
import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/** Counts the Bytes and MSECS spent within a shuffle read. */
public class ShuffleReadCounter {
  private final String originalShuffleStepName;
  private final boolean experimentEnabled;

  public CounterSet counterSet;

  private Counter<Long, Long> currentCounter;
  /**
   * Counter to increment with the bytes read from the underlying shuffle iterator, or null if no
   * counting is needed.
   */
  public final Counter<Long, Long> legacyPerOperationPerDatasetBytesCounter;

  public static final String SHUFFLE_BYTES_READ = "ShuffleBytesRead";

  public ShuffleReadCounter(
      String originalShuffleStepName,
      boolean experimentEnabled,
      Counter<Long, Long> legacyPerOperationPerDatasetBytesCounter) {
    this.originalShuffleStepName = originalShuffleStepName;
    this.experimentEnabled = experimentEnabled;
    this.legacyPerOperationPerDatasetBytesCounter = legacyPerOperationPerDatasetBytesCounter;
    this.counterSet = new CounterSet();
  }

  @SuppressWarnings("ReferenceEquality")
  @SuppressFBWarnings("ES_COMPARING_STRINGS_WITH_EQ")
  private void checkState() {
    if (this.experimentEnabled) {
      ExecutionStateTracker.ExecutionState currentState =
          ExecutionStateTracker.getCurrentExecutionState();
      String currentStateName = null;
      if (currentState instanceof DataflowExecutionState) {
        currentStateName = ((DataflowExecutionState) currentState).getStepName().originalName();
      }
      if (this.currentCounter != null
          && currentStateName == this.currentCounter.getName().originalRequestingStepName()) {
        // If the step name of the state has not changed do not do another lookup.
        return;
      }
      CounterName name =
          ShuffleReadCounter.generateCounterName(this.originalShuffleStepName, currentStateName);
      this.currentCounter = this.counterSet.longSum(name);
    } else {
      this.currentCounter = this.legacyPerOperationPerDatasetBytesCounter;
    }
  }

  public void addBytesRead(long n) {
    checkState();
    if (this.currentCounter == null) {
      return;
    }
    this.currentCounter.addValue(n);
  }

  public CounterSet getCounterSet() {
    return this.counterSet;
  }

  @VisibleForTesting
  public static CounterName generateCounterName(
      String originalShuffleStepName, String executingStepOriginalName) {
    return CounterName.named(SHUFFLE_BYTES_READ)
        .withOriginalName(originalShuffleStepName)
        .withOriginalRequestingStepName(executingStepOriginalName)
        .withOrigin("SYSTEM");
  }
}

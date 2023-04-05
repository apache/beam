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
package org.apache.beam.runners.dataflow.worker;

import com.google.auto.value.AutoValue;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ComparisonChain;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Ordering;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Execution states are uniquely identified by the step name, the state name, and for states
 * representing IO operations, also their requesting step name, and input index.
 */
@AutoValue
public abstract class DataflowExecutionStateKey implements Comparable<DataflowExecutionStateKey> {

  /** Return the {@link NameContext} identifying the step associated with this key. */
  public abstract NameContext getStepName();

  /** Return the activity within the step this key represents. */
  public abstract String getStateName();

  /**
   * Return the step that is associated to some IO operation, such as reading of Shuffle or side
   * inputs.
   */
  public abstract @Nullable String getRequestingStepName();

  /**
   * Return the collection index that is associated to some operation, such as reading side inputs.
   */
  public abstract @Nullable Integer getInputIndex();

  /**
   * Create a new {@link DataflowExecutionStateKey} representing the given step and activity.
   *
   * @param nameContext names describing the step associated with this state key.
   * @param stateName additional string identifying the activity within that step.
   * @param requestingStepName for states representing IO activity, this is the currently-running
   *     step name.
   * @param inputIndex for states representing IO activities related to a PCollection, this is the
   *     index of the PCollection that they represent. This is mainly used to represent side inputs
   *     (e.g. index 1, 2, etc).
   */
  public static DataflowExecutionStateKey create(
      NameContext nameContext,
      String stateName,
      @Nullable String requestingStepName,
      @Nullable Integer inputIndex) {
    return new AutoValue_DataflowExecutionStateKey(
        nameContext, stateName, requestingStepName, inputIndex);
  }

  @Override
  public int compareTo(DataflowExecutionStateKey o) {
    return ComparisonChain.start()
        .compare(getStateName(), o.getStateName())
        .compare(
            getStepName().originalName(),
            o.getStepName().originalName(),
            Ordering.natural().nullsFirst())
        .compare(
            getStepName().stageName(), o.getStepName().stageName(), Ordering.natural().nullsFirst())
        .compare(
            getRequestingStepName(), o.getRequestingStepName(), Ordering.natural().nullsFirst())
        .compare(getInputIndex(), o.getInputIndex(), Ordering.natural().nullsFirst())
        .result();
  }
}

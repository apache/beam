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

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation;

/**
 * Class for executing a MapTask via intrinsic Java function invocation in Dataflow.
 *
 * <p>Also known as the "legacy" or "pre-portability" method
 */
public class IntrinsicMapTaskExecutor extends DataflowMapTaskExecutor {

  /**
   * @deprecated subclasses should move to composition instead of inheritance, making this private
   */
  @Deprecated
  protected IntrinsicMapTaskExecutor(
      List<Operation> operations,
      CounterSet counterSet,
      ExecutionStateTracker executionStateTracker) {
    super(operations, counterSet, executionStateTracker);
  }

  /**
   * Creates a new MapTaskExecutor.
   *
   * @param operations the operations of the map task, in order of execution
   */
  public static IntrinsicMapTaskExecutor forOperations(
      List<Operation> operations, ExecutionStateTracker executionStateTracker) {
    return new IntrinsicMapTaskExecutor(operations, new CounterSet(), executionStateTracker);
  }

  /**
   * Creates a new MapTaskExecutor with a shared set of counters with its creator.
   *
   * @param operations the operations of the map task, in order of execution
   * @param counters a set of system counters associated with operations, which may get extended
   *     during execution
   */
  public static IntrinsicMapTaskExecutor withSharedCounterSet(
      List<Operation> operations,
      CounterSet counters,
      ExecutionStateTracker executionStateTracker) {
    return new IntrinsicMapTaskExecutor(operations, counters, executionStateTracker);
  }

  /**
   * {@inheritDoc}
   *
   * @return nothing; pre-portability Dataflow extracts Beam metrics via the execution context.
   */
  @Override
  public Iterable<CounterUpdate> extractMetricUpdates() {
    return Collections.emptyList();
  }
}

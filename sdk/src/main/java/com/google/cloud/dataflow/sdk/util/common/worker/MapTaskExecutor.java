/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.ListIterator;

/**
 * An executor for a map task, defined by a list of Operations.
 */
public class MapTaskExecutor extends WorkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MapTaskExecutor.class);

  /** The operations in the map task, in execution order. */
  public final List<Operation> operations;

  /** The StateSampler for tracking where time is being spent, or null. */
  protected final StateSampler stateSampler;

  /**
   * Creates a new MapTaskExecutor.
   *
   * @param operations the operations of the map task, in order of execution
   * @param counters a set of system counters associated with
   * operations, which may get extended during execution
   * @param stateSampler a state sampler for tracking where time is being spent
   */
  public MapTaskExecutor(
      List<Operation> operations, CounterSet counters, StateSampler stateSampler) {
    super(counters);
    this.operations = operations;
    this.stateSampler = stateSampler;
  }

  @Override
  public void execute() throws Exception {
    LOG.debug("executing map task");

    // Start operations, in reverse-execution-order, so that a
    // consumer is started before a producer might output to it.
    // Starting a root operation such as a ReadOperation does the work
    // of processing the input dataset.
    LOG.debug("starting operations");
    ListIterator<Operation> iterator = operations.listIterator(operations.size());
    while (iterator.hasPrevious()) {
      Operation op = iterator.previous();
      op.start();
    }

    // Finish operations, in forward-execution-order, so that a
    // producer finishes outputting to its consumers before those
    // consumers are themselves finished.
    LOG.debug("finishing operations");
    for (Operation op : operations) {
      op.finish();
    }

    LOG.debug("map task execution complete");

    // TODO: support for success / failure ports?
  }

  @Override
  public Reader.Progress getWorkerProgress() throws Exception {
    return getReadOperation().getProgress();
  }

  @Override
  public Reader.ForkResult requestFork(Reader.ForkRequest forkRequest) throws Exception {
    return getReadOperation().requestFork(forkRequest);
  }

  ReadOperation getReadOperation() throws Exception {
    if (operations == null || operations.isEmpty()) {
      throw new IllegalStateException("Map task has no operation.");
    }

    Operation readOperation = operations.get(0);
    if (!(readOperation instanceof ReadOperation)) {
      throw new IllegalStateException("First operation in the map task is not a ReadOperation.");
    }

    return (ReadOperation) readOperation;
  }

  @Override
  public void close() throws Exception {
    stateSampler.close();
    super.close();
  }
}

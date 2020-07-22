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

import java.io.Closeable;
import java.util.List;
import java.util.ListIterator;
import org.apache.beam.runners.core.metrics.ExecutionStateTracker;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An executor for a map task, defined by a list of Operations. */
public class MapTaskExecutor implements WorkExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MapTaskExecutor.class);

  /** The operations in the map task, in execution order. */
  public final List<Operation> operations;

  private final ExecutionStateTracker executionStateTracker;

  private final CounterSet counters;

  /**
   * Creates a new MapTaskExecutor.
   *
   * @param operations the operations of the map task, in order of execution
   * @param counters a set of system counters associated with operations, which may get extended
   *     during execution
   */
  public MapTaskExecutor(
      List<Operation> operations,
      CounterSet counters,
      ExecutionStateTracker executionStateTracker) {
    this.counters = counters;
    this.operations = operations;
    this.executionStateTracker = executionStateTracker;
  }

  @Override
  public CounterSet getOutputCounters() {
    return counters;
  }

  @Override
  public void execute() throws Exception {
    LOG.debug("Executing map task");

    try (Closeable stateCloser = executionStateTracker.activate()) {
      try {
        // Start operations, in reverse-execution-order, so that a
        // consumer is started before a producer might output to it.
        // Starting a root operation such as a ReadOperation does the work
        // of processing the input dataset.
        LOG.debug("Starting operations");
        ListIterator<Operation> iterator = operations.listIterator(operations.size());
        while (iterator.hasPrevious()) {
          Operation op = iterator.previous();
          op.start();
        }

        // Finish operations, in forward-execution-order, so that a
        // producer finishes outputting to its consumers before those
        // consumers are themselves finished.
        LOG.debug("Finishing operations");
        for (Operation op : operations) {
          op.finish();
        }
      } catch (Exception | Error exn) {
        LOG.debug("Aborting operations", exn);
        for (Operation op : operations) {
          try {
            op.abort();
          } catch (Exception | Error exn2) {
            exn.addSuppressed(exn2);
            if (exn2 instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            }
          }
        }
        throw exn;
      }
    }

    LOG.debug("Map task execution complete");

    // TODO: support for success / failure ports?
  }

  @Override
  public NativeReader.Progress getWorkerProgress() throws Exception {
    return getReadOperation().getProgress();
  }

  @Override
  public NativeReader.@Nullable DynamicSplitResult requestCheckpoint() throws Exception {
    return getReadOperation().requestCheckpoint();
  }

  @Override
  public NativeReader.DynamicSplitResult requestDynamicSplit(
      NativeReader.DynamicSplitRequest splitRequest) throws Exception {
    return getReadOperation().requestDynamicSplit(splitRequest);
  }

  public ReadOperation getReadOperation() throws Exception {
    if (operations == null || operations.isEmpty()) {
      throw new IllegalStateException("Map task has no operation.");
    }

    Operation readOperation = operations.get(0);
    if (!(readOperation instanceof ReadOperation)) {
      throw new IllegalStateException("First operation in the map task is not a ReadOperation.");
    }

    return (ReadOperation) readOperation;
  }

  public boolean supportsRestart() {
    for (Operation op : operations) {
      if (!op.supportsRestart()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void abort() {
    // Signal the read loop to abort on the next record.
    // TODO: Also interrupt the execution thread.
    for (Operation op : operations) {
      Preconditions.checkState(op instanceof ReadOperation || op instanceof ReceivingOperation);
      if (op instanceof ReadOperation) {
        ((ReadOperation) op).abortReadLoop();
      }
    }
  }

  @Override
  public List<Integer> reportProducedEmptyOutput() {
    List<Integer> emptyOutputSinkIndexes = Lists.newArrayList();
    for (Operation op : operations) {
      if (op instanceof WriteOperation) {
        WriteOperation write = (WriteOperation) op;
        int sinkIndex = write.reportSinkIndexIfEmptyOutput();
        if (sinkIndex != WriteOperation.INVALID_SINK_INDEX) {
          emptyOutputSinkIndexes.add(sinkIndex);
        }
      }
    }
    return emptyOutputSinkIndexes;
  }
}

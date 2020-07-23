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

import java.util.List;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Abstract executor for WorkItem tasks. */
public interface WorkExecutor extends AutoCloseable {

  /** Returns the set of output counters for this task. */
  CounterSet getOutputCounters();

  /** Executes the task. */
  public abstract void execute() throws Exception;

  /**
   * Returns the worker's current progress.
   *
   * <p>May be called at any time, but generally called concurrently to execute().
   */
  default NativeReader.Progress getWorkerProgress() throws Exception {
    // By default, return null indicating worker progress not available.
    return null;
  }

  /** See {@link NativeReader.NativeReaderIterator#requestCheckpoint}. */
  default NativeReader.@Nullable DynamicSplitResult requestCheckpoint() throws Exception {
    // By default, checkpointing does nothing.
    return null;
  }

  /**
   * See {@link NativeReader.NativeReaderIterator#requestDynamicSplit}. Makes sense only for tasks
   * that read input.
   */
  default NativeReader.@Nullable DynamicSplitResult requestDynamicSplit(
      NativeReader.DynamicSplitRequest splitRequest) throws Exception {
    // By default, dynamic splitting is unsupported.
    return null;
  }

  @Override
  default void close() throws Exception {
    // By default, nothing to close or shut down.
  }

  /**
   * Requests that the executor abort as soon as possible.
   *
   * <p>Thread-safe. May be called at any time after execute() begins.
   */
  default void abort() {
    // By default, does nothing. Expensive operations should override this.
  }

  /**
   * Reports the sink index of any WriteOperation that did not produce output. NOTE this is only
   * used for FlumeWriteOperaton for now.
   */
  default List<Integer> reportProducedEmptyOutput() {
    return Lists.newArrayList();
  }
}

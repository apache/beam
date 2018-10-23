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
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;

/**
 * The context in which an operation is executed. This includes information about the operation as
 * well as methods to be called around specific threads of execution.
 *
 * <p>The {@link OperationContext} should be unique for each operation (instruction) in the map task
 * and each worker thread currently executing.
 *
 * <p>Note: The Batch Dataflow Worker creates a new MapTaskExecutor (as well as a new execution
 * context and set of operation contexts) for every work item, while the Streaming Dataflow Worker
 * reuses the MapTaskExecutor along with the execution context and operation contexts. The Streaming
 * worker still makes sure that each MapTaskExecutor is only in use by one thread at a time,
 * creating additional executors if necessary.
 */
public interface OperationContext {

  /**
   * Activate the initialization/start state for the operation.
   *
   * <p>Closing the returned {@link Closeable} will deactivate the state.
   */
  Closeable enterStart();

  /**
   * Activate the element processing state for the operation.
   *
   * <p>Closing the returned {@link Closeable} will deactivate the state.
   */
  Closeable enterProcess();

  /**
   * Activate the timer processing state for the operation.
   *
   * <p>Closing the returned {@link Closeable} will deactivate the state.
   */
  Closeable enterProcessTimers();

  /**
   * Activate the finalization/finishing state for the operation.
   *
   * <p>Closing the returned {@link Closeable} will deactivate the state.
   */
  Closeable enterFinish();

  /**
   * Activate the abort state for the operation.
   *
   * <p>Closing the returned {@link Closeable} will deactivate the state.
   */
  Closeable enterAbort();

  /** The factory for creating counters associated with this operation. */
  CounterFactory counterFactory();

  /** The {@link NameContext} describing this operation. */
  NameContext nameContext();
}

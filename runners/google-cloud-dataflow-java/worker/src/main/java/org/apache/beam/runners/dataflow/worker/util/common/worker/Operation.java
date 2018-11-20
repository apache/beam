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

/**
 * The abstract base class for Operations, which correspond to Instructions in the original MapTask
 * InstructionGraph.
 *
 * <p>Call start() to start the operation.
 *
 * <p>A read operation's start() method actually reads the data, and in effect runs the pipeline.
 *
 * <p>Call finish() to finish the operation.
 *
 * <p>Call abort() if the Operation's execution needs to be aborted, e.g., because an exception is
 * thrown during any of the above, to do clean up that might be done as part of finish() in a
 * successful execution.
 *
 * <p>Since both start() and finish() may call process() on this operation's consumers, start an
 * operation after starting its consumers, and finish an operation before finishing its consumers.
 * Operations can be abort()ed in any order.
 */
public abstract class Operation {

  protected final OperationContext context;

  /**
   * The array of consuming receivers, one per operation output "port" (e.g., DoFn main or side
   * output). A receiver might be null if that output isn't being consumed.
   */
  public final OutputReceiver[] receivers;

  /** The possible initialization states of an Operation. For internal self-checking purposes. */
  public enum InitializationState {
    // start() hasn't yet been called.
    UNSTARTED,

    // start() has been called, but finish() hasn't yet been called.
    STARTED,

    // finish() has been called.
    FINISHED,

    // abort() has been called.
    ABORTED,
  }

  /**
   * The initialization state of this Operation.
   *
   * <p>Written from one thread, but can be read by concurrent threads.
   */
  public InitializationState initializationState = InitializationState.UNSTARTED;

  /**
   * The lock protecting the initialization state.
   *
   * <p>Subclasses can use this lock to protect their own state. However, this lock should be held
   * only for short, bounded amounts of time.
   */
  protected final Object initializationStateLock = new Object();

  public Operation(OutputReceiver[] receivers, OperationContext context) {
    this.receivers = receivers;
    this.context = context;
  }

  /** Checks that this operation is not yet started, throwing an exception otherwise. */
  void checkUnstarted() {
    if (!(initializationState == InitializationState.UNSTARTED
        || (initializationState == InitializationState.FINISHED && supportsRestart()))) {
      throw new AssertionError("expecting this instruction to not yet be started");
    }
  }

  /**
   * Checks that this operation has been started but not yet finished, throwing an exception
   * otherwise.
   */
  void checkStarted() {
    if (initializationState != InitializationState.STARTED) {
      throw new AssertionError("expecting this instruction to be started");
    }
  }

  /** Checks that this operation has been finished, throwing an exception otherwise. */
  void checkFinished() {
    if (initializationState != InitializationState.FINISHED) {
      throw new AssertionError("expecting this instruction to be finished");
    }
  }

  /** Returns true if this Operation has been finished. */
  boolean isFinished() {
    return (initializationState == InitializationState.FINISHED);
  }

  /** Returns true if this Operation has been aborted. */
  boolean isAborted() {
    return (initializationState == InitializationState.ABORTED);
  }

  /**
   * Starts this Operation's execution. Called after all successsor consuming operations have been
   * started.
   */
  public void start() throws Exception {
    synchronized (initializationStateLock) {
      checkUnstarted();
      initializationState = InitializationState.STARTED;
    }
  }

  /**
   * Finishes this Operation's execution. Called after all predecessor producing operations have
   * been finished.
   */
  public void finish() throws Exception {
    synchronized (initializationStateLock) {
      checkStarted();
      initializationState = InitializationState.FINISHED;
    }
  }

  /** Aborts this Operation's execution. */
  public void abort() throws Exception {
    synchronized (initializationStateLock) {
      initializationState = InitializationState.ABORTED;
    }
  }

  /** Returns true if this Operation can be started again after it is finished. */
  public boolean supportsRestart() {
    return false;
  }

  public OperationContext getContext() {
    return context;
  }
}

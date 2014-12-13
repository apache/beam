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

/**
 * The abstract base class for Operations, which correspond to
 * Instructions in the original MapTask InstructionGraph.
 *
 * Call start() to start the operation.
 *
 * A read operation's start() method actually reads the data, and in
 * effect runs the pipeline.
 *
 * Call finish() to finish the operation.
 *
 * Since both start() and finish() may call process() on
 * this operation's consumers, start an operation after
 * starting its consumers, and finish an operation before
 * finishing its consumers.
 */
public abstract class Operation {
  /**
   * The array of consuming receivers, one per operation output
   * "port" (e.g., DoFn main or side output).  A receiver might be
   * null if that output isn't being consumed.
   */
  public final OutputReceiver[] receivers;

  /**
   * The possible initialization states of an Operation.
   * For internal self-checking purposes.
   */
  public enum InitializationState {
    // start() hasn't yet been called.
    UNSTARTED,

    // start() has been called, but finish() hasn't yet been called.
    STARTED,

    // finish() has been called.
    FINISHED
  }

  /** The initialization state of this Operation. */
  public InitializationState initializationState =
      InitializationState.UNSTARTED;

  protected final StateSampler stateSampler;

  protected final int startState;
  protected final int processState;
  protected final int finishState;

  public Operation(String operationName,
                   OutputReceiver[] receivers,
                   String counterPrefix,
                   CounterSet.AddCounterMutator addCounterMutator,
                   StateSampler stateSampler) {
    this.receivers = receivers;
    this.stateSampler = stateSampler;
    startState = stateSampler.stateForName(operationName + "-start");
    processState = stateSampler.stateForName(operationName + "-process");
    finishState = stateSampler.stateForName(operationName + "-finish");
  }

  /**
   * Checks that this oepration is not yet started, throwing an
   * exception otherwise.
   */
  void checkUnstarted() {
    if (initializationState != InitializationState.UNSTARTED) {
      throw new AssertionError(
          "expecting this instruction to not yet be started");
    }
  }

  /**
   * Checks that this oepration has been started but not yet finished,
   * throwing an exception otherwise.
   */
  void checkStarted() {
    if (initializationState != InitializationState.STARTED) {
      throw new AssertionError(
          "expecting this instruction to be started");
    }
  }

  /**
   * Checks that this oepration has been finished, throwing an
   * exception otherwise.
   */
  void checkFinished() {
    if (initializationState != InitializationState.FINISHED) {
      throw new AssertionError(
          "expecting this instruction to be finished");
    }
  }

  /**
   * Starts this Operation's execution.  Called after all successsor
   * consuming operations have been started.
   */
  public void start() throws Exception {
    checkUnstarted();
    initializationState = InitializationState.STARTED;
  }

  /**
   * Finishes this Operation's execution.  Called after all
   * predecessor producing operations have been finished.
   */
  public void finish() throws Exception {
    checkStarted();
    initializationState = InitializationState.FINISHED;
  }
}

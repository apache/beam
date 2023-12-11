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
package org.apache.beam.io.requestresponse;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.util.function.Supplier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Performs {@link CallShouldBackoff} computations but within a windowed {@link Duration}.
 * Reinstantiates {@link CallShouldBackoff} using a {@link CallShouldBackoffSupplier} at {@link
 * #update} after a check for whether a windowed {@link Duration} elapsed.
 */
class WindowedCallShouldBackoff<ResponseT> implements CallShouldBackoff<ResponseT> {

  /**
   * Instantiates a {@link CallShouldBackoffSupplier} with a {@link
   * CallShouldBackoffBasedOnRejectionProbability}.
   */
  static <ResponseT> CallShouldBackoffSupplier<ResponseT> getDefaultCallShouldBackoffSupplier() {
    return CallShouldBackoffBasedOnRejectionProbability::new;
  }

  private final Duration window;
  private final CallShouldBackoffSupplier<ResponseT> callShouldBackoffSupplier;
  private @MonotonicNonNull CallShouldBackoff<ResponseT> basis;
  private Instant nextReset;

  /**
   * Instantiates a {@link WindowedCallShouldBackoff} with a {@link Duration} window and a {@link
   * CallShouldBackoffSupplier}. Within the constructor, sets the clock to {@link Instant#now()} and
   * instantiates {@link CallShouldBackoff} using the {@link CallShouldBackoffSupplier}.
   */
  WindowedCallShouldBackoff(
      Duration window, CallShouldBackoffSupplier<ResponseT> callShouldBackoffSupplier) {
    this.window = window;
    this.callShouldBackoffSupplier = callShouldBackoffSupplier;
    this.basis = callShouldBackoffSupplier.get();
    this.nextReset = Instant.now().plus(window);
  }

  private void resetIfNeeded() {
    if (nextReset.isBeforeNow()) {
      basis = callShouldBackoffSupplier.get();
      nextReset = nextReset.plus(window);
    }
  }

  @Override
  public void update(UserCodeExecutionException exception) {
    resetIfNeeded();
    checkStateNotNull(basis).update(exception);
  }

  @Override
  public void update(ResponseT response) {
    resetIfNeeded();
    checkStateNotNull(basis).update(response);
  }

  @Override
  public boolean value() {
    resetIfNeeded();
    return checkStateNotNull(basis).value();
  }

  /**
   * A {@link Serializable} {@link Supplier} of a {@link CallShouldBackoff} computation. Used by
   * {@link WindowedCallShouldBackoff} to instantiate a {@link CallShouldBackoff} after a window
   * {@link Duration} elapses.
   */
  interface CallShouldBackoffSupplier<ResponseT>
      extends Supplier<CallShouldBackoff<ResponseT>>, Serializable {
    @Override
    CallShouldBackoff<ResponseT> get();
  }
}

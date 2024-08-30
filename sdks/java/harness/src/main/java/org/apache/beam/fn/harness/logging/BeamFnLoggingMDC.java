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
package org.apache.beam.fn.harness.logging;

import org.apache.beam.fn.harness.control.ExecutionStateSampler.ExecutionStateTracker;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Mapped diagnostic context to be consumed and set on LogEntry protos in BeamFnLoggingClient. */
public class BeamFnLoggingMDC {
  private static final ThreadLocal<@Nullable String> instructionId = new ThreadLocal<>();

  private static final ThreadLocal<@Nullable ExecutionStateTracker> stateTracker =
      new ThreadLocal<>();

  /** Sets the Instruction ID of the current thread, which will be inherited by child threads. */
  public static void setInstructionId(@Nullable String newInstructionId) {
    instructionId.set(newInstructionId);
  }

  /** Gets the Instruction ID of the current thread. */
  public static @Nullable String getInstructionId() {
    return instructionId.get();
  }

  /** Sets the State Tracker of the current thread, which will be inherited by child threads. */
  public static void setStateTracker(@Nullable ExecutionStateTracker newStateTracker) {
    stateTracker.set(newStateTracker);
  }

  /** Gets the State Tracker of the current thread. */
  public static @Nullable ExecutionStateTracker getStateTracker() {
    return stateTracker.get();
  }

  /** Resets to a default state. */
  public static void reset() {
    instructionId.set(null);
    stateTracker.set(null);
  }
}

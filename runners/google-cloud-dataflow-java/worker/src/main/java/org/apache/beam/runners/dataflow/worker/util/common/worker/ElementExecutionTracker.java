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

import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;

/** Tracks per-element execution time of a single execution thread. */
public abstract class ElementExecutionTracker {
  public static final ElementExecutionTracker NO_OP_INSTANCE = new NoopElementTracker();
  public static final CounterName COUNTER_NAME = CounterName.named("per-element-processing-time");

  /**
   * Indicates that the execution thread has entered the process method for a new input element in
   * the specified step.
   */
  public abstract void enter(NameContext step);

  /** Indicates that the execution thread has returned from process method. */
  public abstract void exit();

  /** Attribute processing time to executing elements since the last sampling period. */
  public abstract void takeSample(long millisSinceLastSample);

  /** Retrieve a no-op {@link ElementExecutionTracker} instance for testing. */
  public static ElementExecutionTracker newForTest() {
    return NO_OP_INSTANCE;
  }

  /** No-op implementation to stub in when functionality is disabled. */
  private static final class NoopElementTracker extends ElementExecutionTracker {
    @Override
    public void takeSample(long millisSinceLastSample) {}

    @Override
    public void enter(NameContext step) {}

    @Override
    public void exit() {}
  }
}

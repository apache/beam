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
package org.apache.beam.runners.dataflow.worker.profiler;

/**
 * API for enabling/disabling dataflow profiling agent.
 *
 * <p>The profiling agent profiles all application threads. Enabling or disabling the profiler is a
 * global action that affects all threads.
 *
 * <p>This MUST stay in this package. It is implemented via JNI.
 */
public class Profiler {

  /**
   * Enable collection of profiling data.
   *
   * <p>Profiling is done on a preset schedule, it may not start immediately.
   *
   * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
   */
  public static native void enable();

  /**
   * Disable collection of profiling data.
   *
   * <p>If a profile is currently being collected, it will run to completion.
   *
   * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
   */
  public static native void disable();

  /**
   * Registers a nonzero index to be used in a subsequent call to {@link #setAttribute}. The value
   * is permanently registered for the lifetime of the JVM. Registering the same string multiple
   * times will return the same value.
   *
   * @return a nonzero index representing the string, to be used on calls to {@link #setAttribute}.
   * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
   */
  public static native int registerAttribute(String value);

  /**
   * Sets attribute for the current thread. Can be called before profiling is enabled or while
   * profiling is active. Attributes must be either 0 (to clear attributes), or a value returned by
   * {@link #registerAttribute}. The current attribute persists during profile {@link
   * #enable}/{@link #disable}.
   *
   * <p>Samples collected by this thread will be annotated with the string associated to this value.
   *
   * @return the previous value of the thread attribute.
   * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
   */
  public static native int setAttribute(int value);

  /**
   * Gets attribute for the current thread. Can be called before profiling is enabled or while
   * profiling is active. Will return the value passed to the most recent call to setAttribute from
   * this thread.
   *
   * @return the previous value of the thread attribute.
   * @throws java.lang.UnsatisfiedLinkError if the agent hasn't been loaded.
   */
  public static native int getAttribute();
}

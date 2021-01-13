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
package org.apache.beam.runners.core;

import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** An wrapper interface that represents the execution of a {@link DoFn}. */
public interface DoFnRunner<InputT extends @Nullable Object, OutputT extends @Nullable Object> {
  /** Prepares and calls a {@link DoFn DoFn's} {@link DoFn.StartBundle @StartBundle} method. */
  void startBundle();

  /**
   * Calls a {@link DoFn DoFn's} {@link DoFn.ProcessElement @ProcessElement} method with a {@link
   * DoFn.ProcessContext} containing the provided element.
   */
  void processElement(WindowedValue<InputT> elem);

  /**
   * Calls a {@link DoFn DoFn's} {@link DoFn.OnTimer @OnTimer} method for the given timer in the
   * given window.
   */
  <KeyT extends @Nullable Object> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain);

  /**
   * Calls a {@link DoFn DoFn's} {@link DoFn.FinishBundle @FinishBundle} method and performs
   * additional tasks, such as flushing in-memory states.
   */
  void finishBundle();

  /**
   * Calls a {@link DoFn DoFn's} {@link DoFn.OnWindowExpiration @OnWindowExpiration} method and
   * performs additional task, such as extracts a value saved in a state before garbage collection.
   */
  <KeyT extends @Nullable Object> void onWindowExpiration(
      BoundedWindow window, Instant timestamp, KeyT key);

  /**
   * @since 2.5.0
   * @return the underlying fn instance.
   */
  DoFn<InputT, OutputT> getFn();
}

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
import org.joda.time.Instant;

/**
 * Interface for runners of {@link DoFn}'s that support pushback when reading side inputs, i.e.
 * return elements that could not be processed because they require reading a side input window that
 * is not ready.
 */
public interface PushbackSideInputDoFnRunner<InputT, OutputT> {
  /** Calls the underlying {@link DoFn.StartBundle} method. */
  void startBundle();

  /**
   * Call the underlying {@link DoFn.ProcessElement} method for the provided element for each window
   * the element is in that is ready.
   *
   * @param elem the element to process in all ready windows
   * @return each element that could not be processed because it requires a side input window that
   *     is not ready.
   */
  Iterable<WindowedValue<InputT>> processElementInReadyWindows(WindowedValue<InputT> elem);

  /** Calls the underlying {@link DoFn.OnTimer} method. */
  <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain);

  /** Calls the underlying {@link DoFn.FinishBundle} method. */
  void finishBundle();

  /**
   * @since 2.5.0
   * @return the underlying fn instance.
   */
  DoFn<InputT, OutputT> getFn();
}

/*
 * Copyright (C) 2015 Google Inc.
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
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;

import org.joda.time.Instant;

/**
 * Encapsulate interaction with time within the execution environment.
 *
 * <p> This class allows setting and deleting timers, and also retrieving an
 * estimate of the current time.
 */
public interface TimerManager {

  /**
   * Writes out a timer to be fired when the watermark reaches the given
   * timestamp.  Timers are identified by their name, and can be moved
   * by calling {@code setTimer} again, or deleted with {@link #deleteTimer}.
   */
  void setTimer(String timer, Instant timestamp, Trigger.TimeDomain domain);

  /**
   * Deletes the given timer.
   */
  void deleteTimer(String timer, Trigger.TimeDomain domain);

  /**
   * @return the current timestamp in the
   * {@link com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.TimeDomain#PROCESSING_TIME}
   * time domain.
   */
  Instant currentProcessingTime();
}

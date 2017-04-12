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
package org.apache.beam.sdk.util;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * A timer for a specified time domain that can be set to register the desire for further processing
 * at particular time in its specified time domain.
 *
 * <p>See {@link TimeDomain} for details on the time domains available.
 *
 * <p>In a {@link DoFn}, a {@link Timer} is specified by a {@link TimerSpec} annotated with {@link
 * DoFn.TimerId}.
 *
 * <p>An implementation of {@link Timer} is implicitly scoped - it may be scoped to a key and
 * window, or a key, window, and trigger, etc.
 *
 * <p>A timer exists in one of two states: set or unset. A timer can be set only for a single time
 * per scope.
 *
 * <p>Timer callbacks are not guaranteed to be called immediately according to the local view of the
 * {@link TimeDomain}, but will be called at some time after the requested time, in timestamp
 * order.
 */
@Experimental(Experimental.Kind.TIMERS)
public interface Timer {
  /**
   * Sets or resets the time in the timer's {@link TimeDomain} at which it should fire. If the timer
   * was already set, resets it to the new requested time.
   *
   * <p>For {@link TimeDomain#PROCESSING_TIME}, the behavior is be unpredictable, since processing
   * time timers are ignored after a window has expired. Instead, it is recommended to use
   * {@link #setForNowPlus(Duration)}.
   */
  void set(Instant instant);

  /**
   * Sets or resets the time relative to the current time in the timer's {@link TimeDomain} at which
   * this it should fire. If the timer was already set, resets it to the new requested time.
   */
  void setForNowPlus(Duration durationFromNow);

  /**
   * Unsets this timer. It is permitted to {@code cancel()} whether or not the timer was actually
   * set.
   */
  void cancel();
}

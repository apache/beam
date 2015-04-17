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

import com.google.api.client.util.BackOff;
import com.google.api.client.util.NanoClock;
import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;

/**
 * Extension of {@link AttemptBoundedExponentialBackOff} that bounds the total time that the backoff
 * is happening as well as the amount of retries. Acts exactly as a AttemptBoundedExponentialBackOff
 * unless the time interval has expired since the object was created. At this point, it will always
 * return BackOff.STOP. Note that reset does not reset the timer.
 * <p>
 * Implementation is not thread-safe.
 */
public class AttemptAndTimeBoundedExponentialBackOff extends AttemptBoundedExponentialBackOff {
  private long endTimeMillis;
  private final NanoClock nanoClock;
  // NanoClock.SYSTEM has a max elapsed time of 292 years or 2^63 ns.
  private static final long MAX_ELAPSED_TIME_MILLIS = 1L << 53;

  /**
   * Constructs an instance of AttemptAndTimeBoundedExponentialBackoff.
   *
   * @param maximumNumberOfAttempts The maximum number of attempts it will make.
   * @param initialIntervalMillis The original interval to wait between attempts in milliseconds.
   * @param maximumTotalWaitTimeMillis The maximum total time that this object will
   *    allow more attempts in milliseconds.
   */
  public AttemptAndTimeBoundedExponentialBackOff(
      int maximumNumberOfAttempts, long initialIntervalMillis, long maximumTotalWaitTimeMillis) {
    this(maximumNumberOfAttempts,
        initialIntervalMillis,
        maximumTotalWaitTimeMillis,
        NanoClock.SYSTEM);
  }

  /**
   * Constructs an instance of AttemptAndTimeBoundedExponentialBackoff.
   *
   * @param maximumNumberOfAttempts The maximum number of attempts it will make.
   * @param initialIntervalMillis The original interval to wait between attempts in milliseconds.
   * @param maximumTotalWaitTimeMillis The maximum total time that this object will
   *    allow more attempts in milliseconds.
   * @param nanoClock clock used to measure the time that has passed.
   */
  public AttemptAndTimeBoundedExponentialBackOff(
      int maximumNumberOfAttempts, long initialIntervalMillis,
      long maximumTotalWaitTimeMillis, NanoClock nanoClock) {
    super(maximumNumberOfAttempts, initialIntervalMillis);
    Preconditions.checkArgument(nanoClock != null, "NanoClock may not be null");
    Preconditions.checkArgument(maximumTotalWaitTimeMillis > 0,
        "Maximum total wait time must be greater than zero.");
    Preconditions.checkArgument(maximumTotalWaitTimeMillis < MAX_ELAPSED_TIME_MILLIS,
        "Maximum total wait time must be less than " + MAX_ELAPSED_TIME_MILLIS + " milliseconds");
    this.nanoClock = nanoClock;
    endTimeMillis = getTimeMillis() + maximumTotalWaitTimeMillis;
  }

  public void setEndtimeMillis(long endTimeMillis) {
    this.endTimeMillis = endTimeMillis;
  }

  @Override
  public long nextBackOffMillis() {
    if (atMaxAttempts()) {
      return BackOff.STOP;
    }
    long backoff = Math.min(super.nextBackOffMillis(), endTimeMillis - getTimeMillis());
    return (backoff > 0 ? backoff : BackOff.STOP);
  }

  private long getTimeMillis() {
    return TimeUnit.NANOSECONDS.toMillis(nanoClock.nanoTime());
  }

  @Override
  public boolean atMaxAttempts() {
    return super.atMaxAttempts() || getTimeMillis() >= endTimeMillis;
  }
}

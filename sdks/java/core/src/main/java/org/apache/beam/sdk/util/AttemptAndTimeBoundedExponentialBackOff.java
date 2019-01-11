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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.NanoClock;
import java.util.concurrent.TimeUnit;

/**
 * Extension of {@link AttemptBoundedExponentialBackOff} that bounds the total time that the backoff
 * is happening as well as the amount of retries. Acts exactly as a AttemptBoundedExponentialBackOff
 * unless the time interval has expired since the object was created. At this point, it will always
 * return BackOff.STOP. Calling reset() resets both the timer and the number of retry attempts,
 * unless a custom ResetPolicy (ResetPolicy.ATTEMPTS or ResetPolicy.TIMER) is passed to the
 * constructor.
 *
 * <p>Implementation is not thread-safe.
 */
@Deprecated
public class AttemptAndTimeBoundedExponentialBackOff extends AttemptBoundedExponentialBackOff {
  private long endTimeMillis;
  private long maximumTotalWaitTimeMillis;
  private ResetPolicy resetPolicy;
  private final NanoClock nanoClock;
  // NanoClock.SYSTEM has a max elapsed time of 292 years or 2^63 ns.  Here, we choose 2^53 ns as
  // a smaller but still huge limit.
  private static final long MAX_ELAPSED_TIME_MILLIS = 1L << 53;

  /**
   * A ResetPolicy controls the behavior of this BackOff when reset() is called.  By default, both
   * the number of attempts and the time bound for the BackOff are reset, but an alternative
   * ResetPolicy may be set to only reset one of these two.
   */
  public enum ResetPolicy {
    ALL,
    ATTEMPTS,
    TIMER
  }

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
    this(
        maximumNumberOfAttempts,
        initialIntervalMillis,
        maximumTotalWaitTimeMillis,
        ResetPolicy.ALL,
        NanoClock.SYSTEM);
  }

  /**
   * Constructs an instance of AttemptAndTimeBoundedExponentialBackoff.
   *
   * @param maximumNumberOfAttempts The maximum number of attempts it will make.
   * @param initialIntervalMillis The original interval to wait between attempts in milliseconds.
   * @param maximumTotalWaitTimeMillis The maximum total time that this object will
   *    allow more attempts in milliseconds.
   * @param resetPolicy The ResetPolicy specifying the properties of this BackOff that are subject
   *    to being reset.
   */
  public AttemptAndTimeBoundedExponentialBackOff(
      int maximumNumberOfAttempts,
      long initialIntervalMillis,
      long maximumTotalWaitTimeMillis,
      ResetPolicy resetPolicy) {
    this(
        maximumNumberOfAttempts,
        initialIntervalMillis,
        maximumTotalWaitTimeMillis,
        resetPolicy,
        NanoClock.SYSTEM);
  }

  /**
   * Constructs an instance of AttemptAndTimeBoundedExponentialBackoff.
   *
   * @param maximumNumberOfAttempts The maximum number of attempts it will make.
   * @param initialIntervalMillis The original interval to wait between attempts in milliseconds.
   * @param maximumTotalWaitTimeMillis The maximum total time that this object will
   *    allow more attempts in milliseconds.
   * @param resetPolicy The ResetPolicy specifying the properties of this BackOff that are subject
   *    to being reset.
   * @param nanoClock clock used to measure the time that has passed.
   */
  public AttemptAndTimeBoundedExponentialBackOff(
      int maximumNumberOfAttempts,
      long initialIntervalMillis,
      long maximumTotalWaitTimeMillis,
      ResetPolicy resetPolicy,
      NanoClock nanoClock) {
    super(maximumNumberOfAttempts, initialIntervalMillis);
    checkArgument(
        maximumTotalWaitTimeMillis > 0, "Maximum total wait time must be greater than zero.");
    checkArgument(
        maximumTotalWaitTimeMillis < MAX_ELAPSED_TIME_MILLIS,
        "Maximum total wait time must be less than " + MAX_ELAPSED_TIME_MILLIS + " milliseconds");
    checkArgument(resetPolicy != null, "resetPolicy may not be null");
    checkArgument(nanoClock != null, "nanoClock may not be null");
    this.maximumTotalWaitTimeMillis = maximumTotalWaitTimeMillis;
    this.resetPolicy = resetPolicy;
    this.nanoClock = nanoClock;
    // Set the end time for this BackOff.  Note that we cannot simply call reset() here since the
    // resetPolicy may not be set to reset the time bound.
    endTimeMillis = getTimeMillis() + maximumTotalWaitTimeMillis;
  }

  @Override
  public void reset() {
    // reset() is called in the constructor of the parent class before resetPolicy and nanoClock are
    // set.  In this case, we call the parent class's reset() method and return.
    if (resetPolicy == null) {
      super.reset();
      return;
    }
    // Reset the number of attempts.
    if (resetPolicy == ResetPolicy.ALL || resetPolicy == ResetPolicy.ATTEMPTS) {
      super.reset();
    }
    // Reset the time bound.
    if (resetPolicy == ResetPolicy.ALL || resetPolicy == ResetPolicy.TIMER) {
      endTimeMillis = getTimeMillis() + maximumTotalWaitTimeMillis;
    }
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

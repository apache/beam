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
import org.joda.time.Duration;


/**
 * A {@link BackOff} for which the retry algorithm is customizable. By default the
 * {@link FlexibleBackoff} uses exponential backoff (base exponent 1.5) with an initial backoff of
 * 1 second. These parameters can be overridden with {@link FlexibleBackoff#withExponent(double)}
 * and {@link FlexibleBackoff#withInitialBackoff(Duration)}, respectively, and the maximum backoff
 * after exponential increase can be capped using {@link FlexibleBackoff#withMaxBackoff(Duration)}.
 *
 * <p>By default, and after modifying any of the above parameters, {@link FlexibleBackoff} does not
 * limit the number of retries. To limit the backoff, the maximum total number of retries can be set
 * using {@link FlexibleBackoff#withMaxRetries(int)}. The total time spent in backoff can be
 * time-bounded as well by configuring {@link FlexibleBackoff#withMaxCumulativeBackoff(Duration)}.
 * If either of these limits are reached, the {@link FlexibleBackoff#nextBackOffMillis()} will
 * return {@link BackOff#STOP} to signal that no more retries should continue.
 */
public final class FlexibleBackoff implements BackOff {
  private static final double DEFAULT_EXPONENT = 1.5;
  private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
  private static final Duration DEFAULT_MIN_BACKOFF = Duration.standardSeconds(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardDays(1000);
  private static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;
  private static final Duration DEFAULT_MAX_CUM_BACKOFF = Duration.standardDays(1000);
  // Customization of this backoff.
  private final double exponent;
  private final Duration initialBackoff;
  private final Duration maxBackoff;
  private final Duration maxCumulativeBackoff;
  private final int maxRetries;
  // Current state
  private Duration currentCumulativeBackoff;
  private int currentRetry;

  /**
   * Instantiates a {@link FlexibleBackoff} with the default exponential backoff and initial
   * backoff.
   *
   * @see FlexibleBackoff
   */
  public static FlexibleBackoff of() {
    return new FlexibleBackoff(
        DEFAULT_EXPONENT,
        DEFAULT_MIN_BACKOFF, DEFAULT_MAX_BACKOFF, DEFAULT_MAX_CUM_BACKOFF,
        DEFAULT_MAX_RETRIES);
  }

  /**
   * Returns a copy of this {@link FlexibleBackoff} that instead uses the specified exponent to
   * control the exponential growth of delay.
   *
   * <p>Does not modify this object.
   *
   * @see FlexibleBackoff
   */
  public FlexibleBackoff withExponent(double exponent) {
    checkArgument(exponent > 0, "exponent %s must be greater than 0", exponent);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  /**
   * Returns a copy of this {@link FlexibleBackoff} that instead uses the specified initial backoff
   * duration.
   *
   * <p>Does not modify this object.
   *
   * @see FlexibleBackoff
   */
  public FlexibleBackoff withInitialBackoff(Duration initialBackoff) {
    checkArgument(
        initialBackoff.isLongerThan(Duration.ZERO),
        "initialBackoff %s must be at least 1 millisecond",
        initialBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  /**
   * Returns a copy of this {@link FlexibleBackoff} that limits the maximum backoff of an individual
   * attempt to the specified duration.
   *
   * <p>Does not modify this object.
   *
   * @see FlexibleBackoff
   */
  public FlexibleBackoff withMaxBackoff(Duration maxBackoff) {
    checkArgument(
        maxBackoff.getMillis() > 0,
        "maxBackoff %s must be at least 1 millisecond",
        maxBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  /**
   * Returns a copy of this {@link FlexibleBackoff} that limits the total time spent in backoff
   * returned across all calls to {@link #nextBackOffMillis()}.
   *
   * <p>Does not modify this object.
   *
   * @see FlexibleBackoff
   */
  public FlexibleBackoff withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
    checkArgument(maxCumulativeBackoff.isLongerThan(Duration.ZERO),
        "maxCumulativeBackoff %s must be at least 1 millisecond", maxCumulativeBackoff);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  /**
   * Returns a copy of this {@link FlexibleBackoff} that limits the total number of retries, aka
   * the total number of calls to {@link #nextBackOffMillis()} before returning
   * {@link BackOff#STOP}.
   *
   * <p>Does not modify this object.
   *
   * @see FlexibleBackoff
   */
  public FlexibleBackoff withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries %s cannot be negative", maxRetries);
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  @Override
  public void reset() {
    currentRetry = 0;
    currentCumulativeBackoff = Duration.ZERO;
  }

  @Override
  public long nextBackOffMillis() {
    // Maximum number of retries reached.
    if (currentRetry >= maxRetries) {
      return BackOff.STOP;
    }
    // Maximum cumulative backoff reached.
    if (currentCumulativeBackoff.compareTo(maxCumulativeBackoff) >= 0) {
      return BackOff.STOP;
    }

    double currentIntervalMillis =
        Math.min(
            initialBackoff.getMillis() * Math.pow(exponent, currentRetry),
            maxBackoff.getMillis());
    double randomOffset =
        (Math.random() * 2 - 1) * DEFAULT_RANDOMIZATION_FACTOR * currentIntervalMillis;
    long nextBackoffMillis = Math.round(currentIntervalMillis + randomOffset);
    // Cap to limit on cumulative backoff
    Duration remainingCumulative = maxCumulativeBackoff.minus(currentCumulativeBackoff);
    nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.getMillis());

    // Update state and return backoff.
    currentCumulativeBackoff = currentCumulativeBackoff.plus(nextBackoffMillis);
    currentRetry += 1;
    return nextBackoffMillis;
  }

  /**
   * Makes an initialized copy of this {@link FlexibleBackoff}.
   *
   * <p>Does not modify this object.
   */
  public FlexibleBackoff copy() {
    return new FlexibleBackoff(
        exponent, initialBackoff, maxBackoff, maxCumulativeBackoff, maxRetries);
  }

  private FlexibleBackoff(
      double exponent, Duration initialBackoff, Duration maxBackoff, Duration maxCumulativeBackoff,
      int maxRetries) {
    this.exponent = exponent;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
    this.maxRetries = maxRetries;
    this.maxCumulativeBackoff = maxCumulativeBackoff;
    reset();
  }
}

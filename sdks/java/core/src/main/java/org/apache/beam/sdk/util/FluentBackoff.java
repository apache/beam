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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.NoOpCounter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.joda.time.Duration;

/**
 * A fluent builder for {@link BackOff} objects that allows customization of the retry algorithm.
 *
 * @see #DEFAULT for the default configuration parameters.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public final class FluentBackoff {

  private static final double DEFAULT_EXPONENT = 1.5;
  private static final double DEFAULT_RANDOMIZATION_FACTOR = 0.5;
  private static final Duration DEFAULT_MIN_BACKOFF = Duration.standardSeconds(1);
  private static final Duration DEFAULT_MAX_BACKOFF = Duration.standardDays(1000);
  private static final int DEFAULT_MAX_RETRIES = Integer.MAX_VALUE;
  private static final Duration DEFAULT_MAX_CUM_BACKOFF = Duration.standardDays(1000);
  private static final Counter DEFAULT_THROTTLED_TIME_COUNTER = NoOpCounter.getInstance();

  private final double exponent;
  private final Duration initialBackoff;
  private final Duration maxBackoff;
  private final Duration maxCumulativeBackoff;
  private final int maxRetries;
  private final Counter throttledTimeCounter;

  /**
   * By default the {@link BackOff} created by this builder will use exponential backoff (base
   * exponent 1.5) with an initial backoff of 1 second. These parameters can be overridden with
   * {@link #withExponent(double)} and {@link #withInitialBackoff(Duration)}, respectively, and the
   * maximum backoff after exponential increase can be capped using {@link
   * FluentBackoff#withMaxBackoff(Duration)}.
   *
   * <p>The default {@link BackOff} does not limit the number of retries. To limit the backoff, the
   * maximum total number of retries can be set using {@link #withMaxRetries(int)}. The total time
   * spent in backoff can be time-bounded as well by configuring {@link
   * #withMaxCumulativeBackoff(Duration)}. If either of these limits are reached, calls to {@link
   * BackOff#nextBackOffMillis()} will return {@link BackOff#STOP} to signal that no more retries
   * should continue.
   */
  public static final FluentBackoff DEFAULT =
      new FluentBackoff(
          DEFAULT_EXPONENT,
          DEFAULT_MIN_BACKOFF,
          DEFAULT_MAX_BACKOFF,
          DEFAULT_MAX_CUM_BACKOFF,
          DEFAULT_MAX_RETRIES,
          DEFAULT_THROTTLED_TIME_COUNTER);

  /**
   * Instantiates a {@link BackOff} that will obey the current configuration.
   *
   * @see FluentBackoff
   */
  public BackOff backoff() {
    return new BackoffImpl(this);
  }

  /**
   * Returns a copy of this {@link FluentBackoff} that instead uses the specified exponent to
   * control the exponential growth of delay.
   *
   * <p>Does not modify this object.
   *
   * @see FluentBackoff
   */
  public FluentBackoff withExponent(double exponent) {
    checkArgument(exponent > 0, "exponent %s must be greater than 0", exponent);
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  /**
   * Returns a copy of this {@link FluentBackoff} that instead uses the specified initial backoff
   * duration.
   *
   * <p>Does not modify this object.
   *
   * @see FluentBackoff
   */
  public FluentBackoff withInitialBackoff(Duration initialBackoff) {
    checkArgument(
        initialBackoff.isLongerThan(Duration.ZERO),
        "initialBackoff %s must be at least 1 millisecond",
        initialBackoff);
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  /**
   * Returns a copy of this {@link FluentBackoff} that limits the maximum backoff of an individual
   * attempt to the specified duration.
   *
   * <p>Does not modify this object.
   *
   * @see FluentBackoff
   */
  public FluentBackoff withMaxBackoff(Duration maxBackoff) {
    checkArgument(
        maxBackoff.getMillis() > 0, "maxBackoff %s must be at least 1 millisecond", maxBackoff);
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  /**
   * Returns a copy of this {@link FluentBackoff} that limits the total time spent in backoff
   * returned across all calls to {@link BackOff#nextBackOffMillis()}.
   *
   * <p>Does not modify this object.
   *
   * @see FluentBackoff
   */
  public FluentBackoff withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
    checkArgument(
        maxCumulativeBackoff.isLongerThan(Duration.ZERO),
        "maxCumulativeBackoff %s must be at least 1 millisecond",
        maxCumulativeBackoff);
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  /**
   * Returns a copy of this {@link FluentBackoff} that limits the total number of retries, aka the
   * total number of calls to {@link BackOff#nextBackOffMillis()} before returning {@link
   * BackOff#STOP}.
   *
   * <p>Does not modify this object.
   *
   * @see FluentBackoff
   */
  public FluentBackoff withMaxRetries(int maxRetries) {
    checkArgument(maxRetries >= 0, "maxRetries %s cannot be negative", maxRetries);
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  public FluentBackoff withThrottledTimeCounter(Counter throttledTimeCounter) {
    return new FluentBackoff(
        exponent,
        initialBackoff,
        maxBackoff,
        maxCumulativeBackoff,
        maxRetries,
        throttledTimeCounter);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(FluentBackoff.class)
        .add("exponent", exponent)
        .add("initialBackoff", initialBackoff)
        .add("maxBackoff", maxBackoff)
        .add("maxRetries", maxRetries)
        .add("maxCumulativeBackoff", maxCumulativeBackoff)
        .toString();
  }

  private static class BackoffImpl implements BackOff {

    // Customization of this backoff.
    private final FluentBackoff backoffConfig;
    // Current state
    private Duration currentCumulativeBackoff;
    private int currentRetry;

    @Override
    public void reset() {
      currentRetry = 0;
      currentCumulativeBackoff = Duration.ZERO;
    }

    @Override
    public long nextBackOffMillis() {
      // Maximum number of retries reached.
      if (currentRetry >= backoffConfig.maxRetries) {
        return BackOff.STOP;
      }
      // Maximum cumulative backoff reached.
      if (currentCumulativeBackoff.compareTo(backoffConfig.maxCumulativeBackoff) >= 0) {
        return BackOff.STOP;
      }

      double currentIntervalMillis =
          Math.min(
              backoffConfig.initialBackoff.getMillis()
                  * Math.pow(backoffConfig.exponent, currentRetry),
              backoffConfig.maxBackoff.getMillis());
      double randomOffset =
          (Math.random() * 2 - 1) * DEFAULT_RANDOMIZATION_FACTOR * currentIntervalMillis;
      long nextBackoffMillis = Math.round(currentIntervalMillis + randomOffset);
      // Cap to limit on cumulative backoff
      Duration remainingCumulative =
          backoffConfig.maxCumulativeBackoff.minus(currentCumulativeBackoff);
      nextBackoffMillis = Math.min(nextBackoffMillis, remainingCumulative.getMillis());

      // Update state and return backoff.
      currentCumulativeBackoff = currentCumulativeBackoff.plus(Duration.millis(nextBackoffMillis));
      currentRetry += 1;
      backoffConfig.throttledTimeCounter.inc(nextBackoffMillis);
      return nextBackoffMillis;
    }

    private BackoffImpl(FluentBackoff backoffConfig) {
      this.backoffConfig = backoffConfig;
      this.reset();
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(BackoffImpl.class)
          .add("backoffConfig", backoffConfig)
          .add("currentRetry", currentRetry)
          .add("currentCumulativeBackoff", currentCumulativeBackoff)
          .toString();
    }
  }

  private FluentBackoff(
      double exponent,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxCumulativeBackoff,
      int maxRetries,
      Counter throttledTimeCounter) {
    this.exponent = exponent;
    this.initialBackoff = initialBackoff;
    this.maxBackoff = maxBackoff;
    this.maxRetries = maxRetries;
    this.maxCumulativeBackoff = maxCumulativeBackoff;
    this.throttledTimeCounter = throttledTimeCounter;
  }
}

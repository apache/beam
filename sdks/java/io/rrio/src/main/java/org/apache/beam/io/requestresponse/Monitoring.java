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
package org.apache.beam.io.requestresponse;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.CaseFormat;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Configures {@link Metric}s throughout various features of {@link RequestResponseIO}. By default,
 * all monitoring is turned off.
 *
 * <pre>
 * Cache metrics are not yet supported. See <a href="https://github.com/apache/beam/issues/29888">https://github.com/apache/beam/issues/29888</a>
 * </pre>
 */
@AutoValue
public abstract class Monitoring implements Serializable {

  private static final String SEPARATOR = "_";

  /** Counter name for the number of requests received by the {@link Call} transform. */
  static final String REQUESTS_COUNTER_NAME = "requests";

  /** Counter name for the number of responses emitted by the {@link Call} transform. */
  static final String RESPONSES_COUNTER_NAME = "responses";

  /** Counter name for the number of failures emitted by the {@link Call} transform. */
  static final String FAILURES_COUNTER_NAME = "failures";

  /** Counter name for the number of cache read attempts. */
  static final String CACHE_READ_REQUESTS_COUNTER_NAME = metricNameOf("cache", "read", "requests");

  /**
   * Counter name for the number of null requests and response associations received during cache
   * reads.
   */
  static final String CACHE_READ_NULL_COUNTER_NAME = metricNameOf("cache", "read", "nulls");

  /**
   * Counter name for the number of non-null requests and response associations received during
   * cache reads.
   */
  static final String CACHE_READ_NON_NULL_COUNTER_NAME =
      metricNameOf("cache", "read", "non", "nulls");

  /** Counter name for the number of failures encountered during cache reads. */
  static final String CACHE_READ_FAILURES_COUNTER_NAME = metricNameOf("cache", "read", "failures");

  /** Counter name for the number of cache write attempts. */
  static final String CACHE_WRITE_REQUESTS_COUNTER_NAME =
      metricNameOf("cache", "write", "requests");

  /** Counter name for the number of successful cache writes. */
  static final String CACHE_WRITE_SUCCESSES_COUNTER_NAME =
      metricNameOf("cache", "write", "successes");

  /** Counter name for the number of failures encountered during cache writes. */
  static final String CACHE_WRITE_FAILURES_COUNTER_NAME =
      metricNameOf("cache", "write", "failures");

  private static final String CALL_COUNTER_NAME = metricNameOf("call", "invocations");
  private static final String SETUP_COUNTER_NAME = metricNameOf("setup", "invocations");
  private static final String TEARDOWN_COUNTER_NAME = metricNameOf("teardowns", "invocations");
  private static final String BACKOFF_COUNTER_NAME = "backoffs";
  private static final String SLEEPER_COUNTER_NAME = "sleeps";
  private static final String SHOULD_BACKOFF_COUNTER_NAME =
      metricNameOf("should", "backoff", "count");

  /** Derives a {@link Counter} name for a {@link Caller#call} invocation count. */
  static String callCounterNameOf(Caller<?, ?> instance) {
    return metricNameOf(instance.getClass(), CALL_COUNTER_NAME);
  }

  /** Derives a {@link Counter} name for a {@link SetupTeardown#setup} invocation count. */
  static String setupCounterNameOf(SetupTeardown instance) {
    return metricNameOf(instance.getClass(), SETUP_COUNTER_NAME);
  }

  /** Derives a {@link Counter} name for a {@link SetupTeardown#teardown} invocation count. */
  static String teardownCounterNameOf(SetupTeardown instance) {
    return metricNameOf(instance.getClass(), TEARDOWN_COUNTER_NAME);
  }

  /** Derives a {@link Counter} name for a {@link BackOff#nextBackOffMillis} invocation count. */
  static String backoffCounterNameOf(BackOff instance) {
    return metricNameOf(instance.getClass(), BACKOFF_COUNTER_NAME);
  }

  /** Derives a {@link Counter} name for a {@link Sleeper#sleep} invocation count. */
  static String sleeperCounterNameOf(Sleeper instance) {
    return metricNameOf(instance.getClass(), SLEEPER_COUNTER_NAME);
  }

  /**
   * Derives a {@link Counter} name for counts of when {@link CallShouldBackoff#isTrue} is found
   * true.
   */
  static String shouldBackoffCounterName(CallShouldBackoff<?> instance) {
    return metricNameOf(instance.getClass(), SHOULD_BACKOFF_COUNTER_NAME);
  }

  private static String metricNameOf(String... segments) {
    return String.join(SEPARATOR, Arrays.asList(segments));
  }

  private static String metricNameOf(Class<?> clazz, String suffix) {
    String simpleName =
        CaseFormat.UPPER_CAMEL
            .converterTo(CaseFormat.LOWER_UNDERSCORE)
            .convert(clazz.getSimpleName());
    return simpleName + SEPARATOR + suffix;
  }

  public static Builder builder() {
    return new AutoValue_Monitoring.Builder();
  }

  /** Count incoming request elements processed by {@link Call}'s {@link DoFn}. */
  public abstract Boolean getCountRequests();

  /**
   * Count outgoing responses resulting from {@link Call}'s successful {@link Caller} invocation.
   */
  public abstract Boolean getCountResponses();

  /** Count invocations of {@link Caller#call}. */
  public abstract Boolean getCountCalls();

  /** Count failures resulting from {@link Call}'s successful {@link Caller} invocation. */
  public abstract Boolean getCountFailures();

  /** Count invocations of {@link SetupTeardown#setup}. */
  public abstract Boolean getCountSetup();

  /** Count invocations of {@link SetupTeardown#teardown}. */
  public abstract Boolean getCountTeardown();

  /** Count invocations of {@link BackOff#nextBackOffMillis}. */
  public abstract Boolean getCountBackoffs();

  /** Count invocations of {@link Sleeper#sleep}. */
  public abstract Boolean getCountSleeps();

  /** Count when {@link CallShouldBackoff#isTrue} is found true. */
  public abstract Boolean getCountShouldBackoff();

  /** Count number of attempts to read from the {@link Cache}. */
  public abstract Boolean getCountCacheReadRequests();

  /** Count associated null values resulting from {@link Cache} reads. */
  public abstract Boolean getCountCacheReadNulls();

  /** Count associated non-null values resulting from {@link Cache} reads. */
  public abstract Boolean getCountCacheReadNonNulls();

  /** Count {@link Cache} read failures. */
  public abstract Boolean getCountCacheReadFailures();

  /** Count number of attempts to write to the {@link Cache}. */
  public abstract Boolean getCountCacheWriteRequests();

  /** Count {@link Cache} write successes. */
  public abstract Boolean getCountCacheWriteSuccesses();

  /** Count {@link Cache} write failures. */
  public abstract Boolean getCountCacheWriteFailures();

  /**
   * Turns on all monitoring. The purpose of this method is, when used with {@link #toBuilder} and
   * other setters, to turn everything on except for a few select counters.
   */
  public Monitoring withEverythingCounted() {
    return toBuilder()
        .setCountRequests(true)
        .setCountResponses(true)
        .setCountCalls(true)
        .setCountFailures(true)
        .setCountSetup(true)
        .setCountTeardown(true)
        .setCountBackoffs(true)
        .setCountSleeps(true)
        .setCountShouldBackoff(true)
        .build()
        .withCountCaching(true);
  }

  /** Turns on all monitoring except for cache related metrics. */
  public Monitoring withEverythingCountedExceptedCaching() {
    return withEverythingCounted().withCountCaching(false);
  }

  private Monitoring withCountCaching(boolean value) {
    return toBuilder()
        .setCountCacheReadRequests(value)
        .setCountCacheReadNulls(value)
        .setCountCacheReadNonNulls(value)
        .setCountCacheReadFailures(value)
        .setCountCacheWriteRequests(value)
        .setCountCacheWriteSuccesses(value)
        .setCountCacheWriteFailures(value)
        .build();
  }

  public abstract Builder toBuilder();

  /**
   * Convenience wrapper to minimize the number of if statements in the code to check for nullness
   * prior to invoking {@link Counter#inc}.
   */
  static void incIfPresent(@Nullable Counter counter) {
    if (counter != null) {
      counter.inc();
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCountRequests(Boolean value);

    public abstract Builder setCountResponses(Boolean value);

    public abstract Builder setCountCalls(Boolean value);

    public abstract Builder setCountFailures(Boolean value);

    public abstract Builder setCountSetup(Boolean value);

    public abstract Builder setCountTeardown(Boolean value);

    public abstract Builder setCountBackoffs(Boolean value);

    public abstract Builder setCountSleeps(Boolean value);

    public abstract Builder setCountShouldBackoff(Boolean value);

    public abstract Builder setCountCacheReadRequests(Boolean value);

    public abstract Builder setCountCacheReadNulls(Boolean value);

    public abstract Builder setCountCacheReadNonNulls(Boolean value);

    public abstract Builder setCountCacheReadFailures(Boolean value);

    public abstract Builder setCountCacheWriteRequests(Boolean value);

    public abstract Builder setCountCacheWriteSuccesses(Boolean value);

    public abstract Builder setCountCacheWriteFailures(Boolean value);

    abstract Optional<Boolean> getCountRequests();

    abstract Optional<Boolean> getCountResponses();

    abstract Optional<Boolean> getCountCalls();

    abstract Optional<Boolean> getCountFailures();

    abstract Optional<Boolean> getCountSetup();

    abstract Optional<Boolean> getCountTeardown();

    abstract Optional<Boolean> getCountBackoffs();

    abstract Optional<Boolean> getCountSleeps();

    abstract Optional<Boolean> getCountShouldBackoff();

    abstract Optional<Boolean> getCountCacheReadRequests();

    abstract Optional<Boolean> getCountCacheReadNulls();

    abstract Optional<Boolean> getCountCacheReadNonNulls();

    abstract Optional<Boolean> getCountCacheReadFailures();

    abstract Optional<Boolean> getCountCacheWriteRequests();

    abstract Optional<Boolean> getCountCacheWriteSuccesses();

    abstract Optional<Boolean> getCountCacheWriteFailures();

    abstract Monitoring autoBuild();

    public final Monitoring build() {
      if (!getCountRequests().isPresent()) {
        setCountRequests(false);
      }
      if (!getCountResponses().isPresent()) {
        setCountResponses(false);
      }
      if (!getCountCalls().isPresent()) {
        setCountCalls(false);
      }
      if (!getCountFailures().isPresent()) {
        setCountFailures(false);
      }
      if (!getCountSetup().isPresent()) {
        setCountSetup(false);
      }
      if (!getCountTeardown().isPresent()) {
        setCountTeardown(false);
      }
      if (!getCountBackoffs().isPresent()) {
        setCountBackoffs(false);
      }
      if (!getCountSleeps().isPresent()) {
        setCountSleeps(false);
      }
      if (!getCountShouldBackoff().isPresent()) {
        setCountShouldBackoff(false);
      }
      if (!getCountCacheReadRequests().isPresent()) {
        setCountCacheReadRequests(false);
      }
      if (!getCountCacheReadNulls().isPresent()) {
        setCountCacheReadNulls(false);
      }
      if (!getCountCacheReadNonNulls().isPresent()) {
        setCountCacheReadNonNulls(false);
      }
      if (!getCountCacheReadFailures().isPresent()) {
        setCountCacheReadFailures(false);
      }
      if (!getCountCacheWriteRequests().isPresent()) {
        setCountCacheWriteRequests(false);
      }
      if (!getCountCacheWriteSuccesses().isPresent()) {
        setCountCacheWriteSuccesses(false);
      }
      if (!getCountCacheWriteFailures().isPresent()) {
        setCountCacheWriteFailures(false);
      }

      return autoBuild();
    }
  }
}

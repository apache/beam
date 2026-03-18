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
    return new AutoValue_Monitoring.Builder()
        .setCountRequests(false)
        .setCountResponses(false)
        .setCountCalls(false)
        .setCountFailures(false)
        .setCountSetup(false)
        .setCountTeardown(false)
        .setCountBackoffs(false)
        .setCountSleeps(false)
        .setCountShouldBackoff(false)
        .setCountCacheReadRequests(false)
        .setCountCacheReadNulls(false)
        .setCountCacheReadNonNulls(false)
        .setCountCacheReadFailures(false)
        .setCountCacheWriteRequests(false)
        .setCountCacheWriteSuccesses(false)
        .setCountCacheWriteFailures(false);
  }

  /** Count incoming request elements processed by {@link Call}'s {@link DoFn}. */
  public abstract boolean getCountRequests();

  /**
   * Count outgoing responses resulting from {@link Call}'s successful {@link Caller} invocation.
   */
  public abstract boolean getCountResponses();

  /** Count invocations of {@link Caller#call}. */
  public abstract boolean getCountCalls();

  /** Count failures resulting from {@link Call}'s successful {@link Caller} invocation. */
  public abstract boolean getCountFailures();

  /** Count invocations of {@link SetupTeardown#setup}. */
  public abstract boolean getCountSetup();

  /** Count invocations of {@link SetupTeardown#teardown}. */
  public abstract boolean getCountTeardown();

  /** Count invocations of {@link BackOff#nextBackOffMillis}. */
  public abstract boolean getCountBackoffs();

  /** Count invocations of {@link Sleeper#sleep}. */
  public abstract boolean getCountSleeps();

  /** Count when {@link CallShouldBackoff#isTrue} is found true. */
  public abstract boolean getCountShouldBackoff();

  /** Count number of attempts to read from the {@link Cache}. */
  public abstract boolean getCountCacheReadRequests();

  /** Count associated null values resulting from {@link Cache} reads. */
  public abstract boolean getCountCacheReadNulls();

  /** Count associated non-null values resulting from {@link Cache} reads. */
  public abstract boolean getCountCacheReadNonNulls();

  /** Count {@link Cache} read failures. */
  public abstract boolean getCountCacheReadFailures();

  /** Count number of attempts to write to the {@link Cache}. */
  public abstract boolean getCountCacheWriteRequests();

  /** Count {@link Cache} write successes. */
  public abstract boolean getCountCacheWriteSuccesses();

  /** Count {@link Cache} write failures. */
  public abstract boolean getCountCacheWriteFailures();

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

    public abstract Builder setCountRequests(boolean value);

    public abstract Builder setCountResponses(boolean value);

    public abstract Builder setCountCalls(boolean value);

    public abstract Builder setCountFailures(boolean value);

    public abstract Builder setCountSetup(boolean value);

    public abstract Builder setCountTeardown(boolean value);

    public abstract Builder setCountBackoffs(boolean value);

    public abstract Builder setCountSleeps(boolean value);

    public abstract Builder setCountShouldBackoff(boolean value);

    public abstract Builder setCountCacheReadRequests(boolean value);

    public abstract Builder setCountCacheReadNulls(boolean value);

    public abstract Builder setCountCacheReadNonNulls(boolean value);

    public abstract Builder setCountCacheReadFailures(boolean value);

    public abstract Builder setCountCacheWriteRequests(boolean value);

    public abstract Builder setCountCacheWriteSuccesses(boolean value);

    public abstract Builder setCountCacheWriteFailures(boolean value);

    public abstract Monitoring build();
  }
}

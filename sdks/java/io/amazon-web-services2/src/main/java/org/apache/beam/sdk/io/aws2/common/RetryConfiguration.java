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
package org.apache.beam.sdk.io.aws2.common;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.joda.time.Duration.ZERO;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.util.StdConverter;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import org.joda.time.Duration;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetrySetting;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.EqualJitterBackoffStrategy;

/**
 * Configuration of the retry behavior for AWS SDK clients.
 *
 * <p>Defaults here correspond to AWS SDK defaults used in {@link RetryPolicy}. See {@link
 * SdkDefaultRetrySetting} for further details.
 */
@AutoValue
@JsonInclude(value = JsonInclude.Include.NON_EMPTY)
@JsonDeserialize(builder = RetryConfiguration.Builder.class)
public abstract class RetryConfiguration implements Serializable {
  private static final java.time.Duration BASE_BACKOFF = java.time.Duration.ofMillis(100);
  private static final java.time.Duration THROTTLED_BASE_BACKOFF = java.time.Duration.ofSeconds(1);
  private static final java.time.Duration MAX_BACKOFF = java.time.Duration.ofSeconds(20);

  @JsonProperty
  public abstract @Pure int numRetries();

  @JsonProperty
  @JsonSerialize(converter = DurationToMillis.class)
  public abstract @Nullable @Pure Duration baseBackoff();

  @JsonProperty
  @JsonSerialize(converter = DurationToMillis.class)
  public abstract @Nullable @Pure Duration throttledBaseBackoff();

  @JsonProperty
  @JsonSerialize(converter = DurationToMillis.class)
  public abstract @Nullable @Pure Duration maxBackoff();

  public abstract RetryConfiguration.Builder toBuilder();

  public static Builder builder() {
    return Builder.builder().numRetries(3);
  }

  @AutoValue.Builder
  @JsonPOJOBuilder(withPrefix = "")
  public abstract static class Builder {
    @JsonCreator
    static Builder builder() {
      return new AutoValue_RetryConfiguration.Builder();
    }

    public abstract Builder numRetries(int numRetries);

    @JsonDeserialize(converter = MillisToDuration.class)
    public abstract Builder baseBackoff(Duration baseBackoff);

    @JsonDeserialize(converter = MillisToDuration.class)
    public abstract Builder throttledBaseBackoff(Duration baseBackoff);

    @JsonDeserialize(converter = MillisToDuration.class)
    public abstract Builder maxBackoff(Duration maxBackoff);

    abstract RetryConfiguration autoBuild();

    public RetryConfiguration build() {
      RetryConfiguration config = autoBuild();
      Duration baseBackoff = config.baseBackoff();
      Duration throttledBaseBackoff = config.throttledBaseBackoff();
      Duration maxBackoff = config.maxBackoff();

      checkArgument(config.numRetries() >= 0, "numRetries must not be negative");
      checkArgument(
          baseBackoff == null || baseBackoff.isLongerThan(ZERO),
          "baseBackoff must be greater than 0");
      checkArgument(
          throttledBaseBackoff == null || throttledBaseBackoff.isLongerThan(ZERO),
          "throttledBaseBackoff must be greater than 0");
      checkArgument(
          maxBackoff == null || maxBackoff.isLongerThan(ZERO), "maxBackoff must be greater than 0");
      return config;
    }
  }

  RetryPolicy toClientRetryPolicy() {
    if (numRetries() == 0) {
      return RetryPolicy.none();
    }
    RetryPolicy.Builder builder = RetryPolicy.builder().numRetries(numRetries());

    java.time.Duration base = toJava(baseBackoff());
    java.time.Duration throttledBase = toJava(throttledBaseBackoff());
    java.time.Duration max = toJava(maxBackoff());

    if (base != null || max != null) {
      builder.backoffStrategy(
          EqualJitterBackoffStrategy.builder()
              .baseDelay(base != null ? base : BASE_BACKOFF)
              .maxBackoffTime(max != null ? max : MAX_BACKOFF)
              .build());
    }

    if (throttledBaseBackoff() != null || maxBackoff() != null) {
      builder.throttlingBackoffStrategy(
          EqualJitterBackoffStrategy.builder()
              .baseDelay(throttledBase != null ? throttledBase : THROTTLED_BASE_BACKOFF)
              .maxBackoffTime(max != null ? max : MAX_BACKOFF)
              .build());
    }
    return builder.build();
  }

  private @Nullable static java.time.Duration toJava(@Nullable Duration duration) {
    return duration == null ? null : java.time.Duration.ofMillis(duration.getMillis());
  }

  static class DurationToMillis extends StdConverter<Duration, Long> {
    @Override
    public Long convert(Duration duration) {
      return duration.getMillis();
    }
  }

  static class MillisToDuration extends StdConverter<Long, Duration> {
    @Override
    public Duration convert(Long millis) {
      return Duration.millis(millis);
    }
  }
}

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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

@AutoValue
public abstract class RetryConfiguration implements Serializable {
  private static final Integer DEFAULT_MAX_ATTEMPTS = 5;
  private static final Duration DEFAULT_INITIAL_BACKOFF = Duration.standardSeconds(15);
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardDays(1000);

  abstract int getMaxAttempts();

  abstract @Nullable Duration getMaxDuration();

  abstract @Nullable Duration getInitialDuration();

  public static RetryConfiguration create() {
    return create(DEFAULT_MAX_ATTEMPTS, null, null);
  }

  public static RetryConfiguration create(int maxAttempts) {
    return create(maxAttempts, null, null);
  }

  public static RetryConfiguration create(
      int maxAttempts, @Nullable Duration maxDuration, @Nullable Duration initialDuration) {
    checkArgument(maxAttempts > 0, "maxAttempts should be greater than 0");

    if (maxDuration == null || maxDuration.equals(Duration.ZERO)) {
      maxDuration = DEFAULT_MAX_CUMULATIVE_BACKOFF;
    }

    if (initialDuration == null || initialDuration.equals(Duration.ZERO)) {
      initialDuration = DEFAULT_INITIAL_BACKOFF;
    }

    return new AutoValue_RetryConfiguration.Builder()
        .setMaxAttempts(maxAttempts)
        .setInitialDuration(initialDuration)
        .setMaxDuration(maxDuration)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setMaxAttempts(int maxAttempts);

    abstract Builder setMaxDuration(Duration maxDuration);

    abstract Builder setInitialDuration(Duration initialDuration);

    abstract RetryConfiguration build();
  }
}

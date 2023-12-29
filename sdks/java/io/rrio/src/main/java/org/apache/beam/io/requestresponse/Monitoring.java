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
import java.util.Optional;
import org.apache.beam.sdk.metrics.Metric;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;

/**
 * Configures {@link Metric}s throughout various features of {@link RequestResponseIO}. By default,
 * all monitoring is turned off.
 */
@AutoValue
public abstract class Monitoring implements Serializable {

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
        .setCountCacheReadRequests(true)
        .setCountCacheReadNulls(true)
        .setCountCacheReadNonNulls(true)
        .setCountCacheReadFailures(true)
        .setCountCacheWriteRequests(true)
        .setCountCacheWriteSuccesses(true)
        .setCountCacheWriteFailures(true)
        .build();
  }

  public abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setCountRequests(Boolean value);

    public abstract Builder setCountResponses(Boolean value);

    public abstract Builder setCountCalls(Boolean value);

    public abstract Builder setCountFailures(Boolean value);

    public abstract Builder setCountSetup(Boolean value);

    public abstract Builder setCountTeardown(Boolean value);

    public abstract Builder setCountBackoffs(Boolean value);

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

    abstract Optional<Boolean> getCountCacheReadRequests();

    abstract Optional<Boolean> getCountCacheReadNulls();

    abstract Optional<Boolean> getCountCacheReadNonNulls();

    abstract Optional<Boolean> getCountCacheReadFailures();

    abstract Optional<Boolean> getCountCacheWriteRequests();

    abstract Optional<Boolean> getCountCacheWriteSuccesses();

    abstract Optional<Boolean> getCountCacheWriteFailures();

    abstract Monitoring autoBuild();

    final Monitoring build() {
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

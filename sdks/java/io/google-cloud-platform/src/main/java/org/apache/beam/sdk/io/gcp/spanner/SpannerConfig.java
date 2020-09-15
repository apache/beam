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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Configuration for a Cloud Spanner client. */
@AutoValue
public abstract class SpannerConfig implements Serializable {
  // A default host name for batch traffic.
  private static final String DEFAULT_HOST = "https://batch-spanner.googleapis.com/";
  // Deadline for Commit API call.
  private static final Duration DEFAULT_COMMIT_DEADLINE = Duration.standardSeconds(15);
  // Total allowable backoff time.
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardMinutes(15);

  public abstract @Nullable ValueProvider<String> getProjectId();

  public abstract @Nullable ValueProvider<String> getInstanceId();

  public abstract @Nullable ValueProvider<String> getDatabaseId();

  public abstract @Nullable ValueProvider<String> getHost();

  public abstract @Nullable ValueProvider<Duration> getCommitDeadline();

  public abstract @Nullable ValueProvider<Duration> getMaxCumulativeBackoff();

  @Nullable
  @VisibleForTesting
  abstract ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

  abstract Builder toBuilder();

  public static SpannerConfig create() {
    return builder()
        .setHost(ValueProvider.StaticValueProvider.of(DEFAULT_HOST))
        .setCommitDeadline(ValueProvider.StaticValueProvider.of(DEFAULT_COMMIT_DEADLINE))
        .setMaxCumulativeBackoff(
            ValueProvider.StaticValueProvider.of(DEFAULT_MAX_CUMULATIVE_BACKOFF))
        .build();
  }

  static Builder builder() {
    return new AutoValue_SpannerConfig.Builder();
  }

  public void validate() {
    checkNotNull(
        getInstanceId(),
        "SpannerIO.read() requires instance id to be set with withInstanceId method");
    checkNotNull(
        getDatabaseId(),
        "SpannerIO.read() requires database id to be set with withDatabaseId method");
  }

  public void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Output Project"))
        .addIfNotNull(DisplayData.item("instanceId", getInstanceId()).withLabel("Output Instance"))
        .addIfNotNull(DisplayData.item("databaseId", getDatabaseId()).withLabel("Output Database"));

    if (getServiceFactory() != null) {
      builder.addIfNotNull(
          DisplayData.item("serviceFactory", getServiceFactory().getClass().getName())
              .withLabel("Service Factory"));
    }
  }

  /** Builder for {@link SpannerConfig}. */
  @AutoValue.Builder
  public abstract static class Builder {

    abstract Builder setProjectId(ValueProvider<String> projectId);

    abstract Builder setInstanceId(ValueProvider<String> instanceId);

    abstract Builder setDatabaseId(ValueProvider<String> databaseId);

    abstract Builder setHost(ValueProvider<String> host);

    abstract Builder setCommitDeadline(ValueProvider<Duration> commitDeadline);

    abstract Builder setMaxCumulativeBackoff(ValueProvider<Duration> maxCumulativeBackoff);

    abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

    public abstract SpannerConfig build();
  }

  public SpannerConfig withProjectId(ValueProvider<String> projectId) {
    return toBuilder().setProjectId(projectId).build();
  }

  public SpannerConfig withProjectId(String projectId) {
    return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
  }

  public SpannerConfig withInstanceId(ValueProvider<String> instanceId) {
    return toBuilder().setInstanceId(instanceId).build();
  }

  public SpannerConfig withInstanceId(String instanceId) {
    return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
  }

  public SpannerConfig withDatabaseId(ValueProvider<String> databaseId) {
    return toBuilder().setDatabaseId(databaseId).build();
  }

  public SpannerConfig withDatabaseId(String databaseId) {
    return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
  }

  public SpannerConfig withHost(ValueProvider<String> host) {
    return toBuilder().setHost(host).build();
  }

  public SpannerConfig withCommitDeadline(Duration commitDeadline) {
    return withCommitDeadline(ValueProvider.StaticValueProvider.of(commitDeadline));
  }

  public SpannerConfig withCommitDeadline(ValueProvider<Duration> commitDeadline) {
    return toBuilder().setCommitDeadline(commitDeadline).build();
  }

  public SpannerConfig withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
    return withMaxCumulativeBackoff(ValueProvider.StaticValueProvider.of(maxCumulativeBackoff));
  }

  public SpannerConfig withMaxCumulativeBackoff(ValueProvider<Duration> maxCumulativeBackoff) {
    return toBuilder().setMaxCumulativeBackoff(maxCumulativeBackoff).build();
  }

  @VisibleForTesting
  SpannerConfig withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
    return toBuilder().setServiceFactory(serviceFactory).build();
  }
}

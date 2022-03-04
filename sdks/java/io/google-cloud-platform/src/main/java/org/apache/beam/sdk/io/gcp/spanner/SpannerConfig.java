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

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import java.io.Serializable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/** Configuration for a Cloud Spanner client. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public abstract class SpannerConfig implements Serializable {
  // A default host name for batch traffic.
  private static final String DEFAULT_HOST = "https://batch-spanner.googleapis.com/";
  // Deadline for Commit API call.
  private static final Duration DEFAULT_COMMIT_DEADLINE = Duration.standardSeconds(15);
  // Total allowable backoff time.
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardMinutes(15);
  // A default priority for batch traffic.
  private static final RpcPriority DEFAULT_RPC_PRIORITY = RpcPriority.MEDIUM;

  public abstract @Nullable ValueProvider<String> getProjectId();

  public abstract @Nullable ValueProvider<String> getInstanceId();

  public abstract @Nullable ValueProvider<String> getDatabaseId();

  public abstract @Nullable ValueProvider<String> getHost();

  public abstract @Nullable ValueProvider<String> getEmulatorHost();

  public abstract @Nullable ValueProvider<Boolean> getIsLocalChannelProvider();

  public abstract @Nullable ValueProvider<Duration> getCommitDeadline();

  public abstract @Nullable ValueProvider<Duration> getMaxCumulativeBackoff();

  public abstract @Nullable RetrySettings getExecuteStreamingSqlRetrySettings();

  public abstract @Nullable RetrySettings getCommitRetrySettings();

  public abstract @Nullable ImmutableSet<Code> getRetryableCodes();

  public abstract @Nullable ValueProvider<RpcPriority> getRpcPriority();

  @VisibleForTesting
  abstract @Nullable ServiceFactory<Spanner, SpannerOptions> getServiceFactory();

  abstract Builder toBuilder();

  public static SpannerConfig create() {
    return builder()
        .setHost(ValueProvider.StaticValueProvider.of(DEFAULT_HOST))
        .setCommitDeadline(ValueProvider.StaticValueProvider.of(DEFAULT_COMMIT_DEADLINE))
        .setMaxCumulativeBackoff(
            ValueProvider.StaticValueProvider.of(DEFAULT_MAX_CUMULATIVE_BACKOFF))
        .setRpcPriority(ValueProvider.StaticValueProvider.of(DEFAULT_RPC_PRIORITY))
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

    abstract Builder setEmulatorHost(ValueProvider<String> emulatorHost);

    abstract Builder setIsLocalChannelProvider(ValueProvider<Boolean> isLocalChannelProvider);

    abstract Builder setCommitDeadline(ValueProvider<Duration> commitDeadline);

    abstract Builder setMaxCumulativeBackoff(ValueProvider<Duration> maxCumulativeBackoff);

    abstract Builder setExecuteStreamingSqlRetrySettings(
        RetrySettings executeStreamingSqlRetrySettings);

    abstract Builder setCommitRetrySettings(RetrySettings commitRetrySettings);

    abstract Builder setRetryableCodes(ImmutableSet<Code> retryableCodes);

    abstract Builder setServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory);

    abstract Builder setRpcPriority(ValueProvider<RpcPriority> rpcPriority);

    public abstract SpannerConfig build();
  }

  /** Specifies the Cloud Spanner project ID. */
  public SpannerConfig withProjectId(ValueProvider<String> projectId) {
    return toBuilder().setProjectId(projectId).build();
  }

  /** Specifies the Cloud Spanner project ID. */
  public SpannerConfig withProjectId(String projectId) {
    return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
  }

  /** Specifies the Cloud Spanner instance ID. */
  public SpannerConfig withInstanceId(ValueProvider<String> instanceId) {
    return toBuilder().setInstanceId(instanceId).build();
  }

  /** Specifies the Cloud Spanner instance ID. */
  public SpannerConfig withInstanceId(String instanceId) {
    return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
  }

  /** Specifies the Cloud Spanner database ID. */
  public SpannerConfig withDatabaseId(ValueProvider<String> databaseId) {
    return toBuilder().setDatabaseId(databaseId).build();
  }

  /** Specifies the Cloud Spanner database ID. */
  public SpannerConfig withDatabaseId(String databaseId) {
    return withDatabaseId(ValueProvider.StaticValueProvider.of(databaseId));
  }

  /** Specifies the Cloud Spanner host. */
  public SpannerConfig withHost(ValueProvider<String> host) {
    return toBuilder().setHost(host).build();
  }

  /** Specifies the Cloud Spanner host, when an emulator is used. */
  public SpannerConfig withEmulatorHost(ValueProvider<String> emulatorHost) {
    return toBuilder().setEmulatorHost(emulatorHost).build();
  }

  /**
   * Specifies whether a local channel provider should be used. This should be set to True when an
   * emulator is used.
   */
  public SpannerConfig withIsLocalChannelProvider(ValueProvider<Boolean> isLocalChannelProvider) {
    return toBuilder().setIsLocalChannelProvider(isLocalChannelProvider).build();
  }

  /** Specifies the commit deadline. This is overridden if the CommitRetrySettings is specified. */
  public SpannerConfig withCommitDeadline(Duration commitDeadline) {
    return withCommitDeadline(ValueProvider.StaticValueProvider.of(commitDeadline));
  }

  /** Specifies the commit deadline. This is overridden if the CommitRetrySettings is specified. */
  public SpannerConfig withCommitDeadline(ValueProvider<Duration> commitDeadline) {
    return toBuilder().setCommitDeadline(commitDeadline).build();
  }

  /** Specifies the maximum cumulative backoff. */
  public SpannerConfig withMaxCumulativeBackoff(Duration maxCumulativeBackoff) {
    return withMaxCumulativeBackoff(ValueProvider.StaticValueProvider.of(maxCumulativeBackoff));
  }

  /** Specifies the maximum cumulative backoff. */
  public SpannerConfig withMaxCumulativeBackoff(ValueProvider<Duration> maxCumulativeBackoff) {
    return toBuilder().setMaxCumulativeBackoff(maxCumulativeBackoff).build();
  }

  /**
   * Specifies the ExecuteStreamingSql retry settings. If not set, the default timeout is set to 2
   * hours.
   */
  public SpannerConfig withExecuteStreamingSqlRetrySettings(
      RetrySettings executeStreamingSqlRetrySettings) {
    return toBuilder()
        .setExecuteStreamingSqlRetrySettings(executeStreamingSqlRetrySettings)
        .build();
  }

  /** Specifies the commit retry settings. Setting this overrides the commit deadline. */
  public SpannerConfig withCommitRetrySettings(RetrySettings commitRetrySettings) {
    return toBuilder().setCommitRetrySettings(commitRetrySettings).build();
  }

  /** Specifies the errors that will be retried by the client library for all operations. */
  public SpannerConfig withRetryableCodes(ImmutableSet<Code> retryableCodes) {
    return toBuilder().setRetryableCodes(retryableCodes).build();
  }

  /** Specifies the service factory to create instance of Spanner. */
  @VisibleForTesting
  SpannerConfig withServiceFactory(ServiceFactory<Spanner, SpannerOptions> serviceFactory) {
    return toBuilder().setServiceFactory(serviceFactory).build();
  }

  /** Specifies the RPC priority. */
  public SpannerConfig withRpcPriority(RpcPriority rpcPriority) {
    return withRpcPriority(ValueProvider.StaticValueProvider.of(rpcPriority));
  }

  /** Specifies the RPC priority. */
  public SpannerConfig withRpcPriority(ValueProvider<RpcPriority> rpcPriority) {
    return toBuilder().setRpcPriority(rpcPriority).build();
  }
}

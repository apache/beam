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

import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.auto.value.AutoValue;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

/** Configuration for a Cloud Spanner client. */
@AutoValue
public abstract class SpannerConfig implements Serializable {
  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";
  // A default host name for batch traffic.
  private static final String DEFAULT_HOST = "https://batch-spanner.googleapis.com/";
  // Deadline for Commit API call.
  private static final Duration DEFAULT_COMMIT_DEADLINE = Duration.standardSeconds(15);
  // Total allowable backoff time.
  private static final Duration DEFAULT_MAX_CUMULATIVE_BACKOFF = Duration.standardMinutes(15);

  @Nullable
  public abstract ValueProvider<String> getProjectId();

  @Nullable
  public abstract ValueProvider<String> getInstanceId();

  @Nullable
  public abstract ValueProvider<String> getDatabaseId();

  @Nullable
  public abstract ValueProvider<String> getHost();

  @Nullable
  public abstract ValueProvider<Duration> getCommitDeadline();

  @Nullable
  public abstract ValueProvider<Duration> getMaxCumulativeBackoff();

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

  public SpannerAccessor connectToSpanner() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();

    if (getCommitDeadline() != null && getCommitDeadline().get().getMillis() > 0) {

      // In Spanner API version 1.21 or above, we can set the deadline / total Timeout on an API
      // call using the following code:
      //
      // UnaryCallSettings.Builder commitSettings =
      // builder.getSpannerStubSettingsBuilder().commitSettings();
      // RetrySettings.Builder commitRetrySettings = commitSettings.getRetrySettings().toBuilder()
      // commitSettings.setRetrySettings(
      //     commitRetrySettings.setTotalTimeout(
      //         Duration.ofMillis(getCommitDeadlineMillis().get()))
      //     .build());
      //
      // However, at time of this commit, the Spanner API is at only at v1.6.0, where the only
      // method to set a deadline is with GRPC Interceptors, so we have to use that...
      SpannerInterceptorProvider interceptorProvider =
          SpannerInterceptorProvider.createDefault()
              .with(new CommitDeadlineSettingInterceptor(getCommitDeadline().get()));
      builder.setInterceptorProvider(interceptorProvider);
    }

    if (getProjectId() != null) {
      builder.setProjectId(getProjectId().get());
    }
    if (getServiceFactory() != null) {
      builder.setServiceFactory(this.getServiceFactory());
    }
    if (getHost() != null) {
      builder.setHost(getHost().get());
    }
    String userAgentString = USER_AGENT_PREFIX + "/" + ReleaseInfo.getReleaseInfo().getVersion();
    builder.setHeaderProvider(FixedHeaderProvider.create("user-agent", userAgentString));
    SpannerOptions options = builder.build();
    Spanner spanner = options.getService();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(
            DatabaseId.of(options.getProjectId(), getInstanceId().get(), getDatabaseId().get()));
    BatchClient batchClient =
        spanner.getBatchClient(
            DatabaseId.of(options.getProjectId(), getInstanceId().get(), getDatabaseId().get()));
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    return new SpannerAccessor(spanner, databaseClient, databaseAdminClient, batchClient);
  }

  private static class CommitDeadlineSettingInterceptor implements ClientInterceptor {
    private final long commitDeadlineMilliseconds;

    private CommitDeadlineSettingInterceptor(Duration commitDeadline) {
      this.commitDeadlineMilliseconds = commitDeadline.getMillis();
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      if (method.getFullMethodName().equals("google.spanner.v1.Spanner/Commit")) {
        callOptions =
            callOptions.withDeadlineAfter(commitDeadlineMilliseconds, TimeUnit.MILLISECONDS);
      }
      return next.newCall(method, callOptions);
    }
  }
}

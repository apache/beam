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

import com.google.api.gax.grpc.testing.LocalChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.v1.stub.SpannerStubSettings;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CommitResponse;
import com.google.spanner.v1.ExecuteSqlRequest;
import com.google.spanner.v1.PartialResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class SpannerAccessor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerAccessor.class);

  /* A common user agent token that indicates that this request was originated from
   * Apache Beam. Setting the user-agent allows Cloud Spanner to detect that the
   * workload is coming from Dataflow and to potentially apply performance optimizations
   */
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";

  // Only create one SpannerAccessor for each different SpannerConfig.
  private static final ConcurrentHashMap<SpannerConfig, SpannerAccessor> spannerAccessors =
      new ConcurrentHashMap<>();

  private final Spanner spanner;
  private final DatabaseClient databaseClient;
  private final BatchClient batchClient;
  private final DatabaseAdminClient databaseAdminClient;
  private final SpannerConfig spannerConfig;
  private final String instanceConfigId;
  private int refcount = 0;

  private SpannerAccessor(
      Spanner spanner,
      DatabaseClient databaseClient,
      DatabaseAdminClient databaseAdminClient,
      BatchClient batchClient,
      SpannerConfig spannerConfig,
      String instanceConfigId) {
    this.spanner = spanner;
    this.databaseClient = databaseClient;
    this.databaseAdminClient = databaseAdminClient;
    this.batchClient = batchClient;
    this.spannerConfig = spannerConfig;
    this.instanceConfigId = instanceConfigId;
  }

  public static SpannerAccessor getOrCreate(SpannerConfig spannerConfig) {

    synchronized (spannerAccessors) {
      SpannerAccessor self = spannerAccessors.get(spannerConfig);
      if (self == null) {
        // Connect to spanner for this SpannerConfig.
        LOG.info("Connecting to {}", spannerConfig);
        self = SpannerAccessor.createAndConnect(spannerConfig);
        LOG.info("Successfully connected to {}", spannerConfig);
        spannerAccessors.put(spannerConfig, self);
      }
      // Add refcount for this spannerConfig.
      self.refcount++;
      LOG.debug("getOrCreate(): refcount={} for {}", self.refcount, spannerConfig);
      return self;
    }
  }

  @VisibleForTesting
  static SpannerOptions buildSpannerOptions(SpannerConfig spannerConfig) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();

    Set<Code> retryableCodes = new HashSet<>();
    if (spannerConfig.getRetryableCodes() != null) {
      retryableCodes.addAll(spannerConfig.getRetryableCodes());
    }
    if (spannerConfig.getDataBoostEnabled() != null && spannerConfig.getDataBoostEnabled().get()) {
      retryableCodes.add(Code.RESOURCE_EXHAUSTED);
    }
    // Add default retryable codes for unary methods
    Set<Code> unaryMethodRetryableCodes = new HashSet<>(retryableCodes);
    unaryMethodRetryableCodes.addAll(
        builder.getSpannerStubSettingsBuilder().getSessionSettings().getRetryableCodes());
    // Set retryable codes for all API methods
    builder
        .getSpannerStubSettingsBuilder()
        .applyToAllUnaryMethods(
            input -> {
              input.setRetryableCodes(unaryMethodRetryableCodes);
              return null;
            });
    // Add default retryable codes for streaming methods
    Set<Code> streamingMethodRetryableCodes = new HashSet<>(retryableCodes);
    streamingMethodRetryableCodes.addAll(
        builder.getSpannerStubSettingsBuilder().executeStreamingSqlSettings().getRetryableCodes());
    builder
        .getSpannerStubSettingsBuilder()
        .executeStreamingSqlSettings()
        .setRetryableCodes(streamingMethodRetryableCodes);
    builder
        .getSpannerStubSettingsBuilder()
        .streamingReadSettings()
        .setRetryableCodes(streamingMethodRetryableCodes);

    // Set commit retry settings
    UnaryCallSettings.Builder<CommitRequest, CommitResponse> commitSettings =
        builder.getSpannerStubSettingsBuilder().commitSettings();
    ValueProvider<Duration> commitDeadline = spannerConfig.getCommitDeadline();
    if (spannerConfig.getCommitRetrySettings() != null) {
      commitSettings.setRetrySettings(spannerConfig.getCommitRetrySettings());
    } else if (commitDeadline != null && commitDeadline.get().getMillis() > 0) {
      // Set the GRPC deadline on the Commit API call.
      RetrySettings.Builder commitRetrySettingsBuilder =
          commitSettings.getRetrySettings().toBuilder();
      commitSettings.setRetrySettings(
          commitRetrySettingsBuilder
              .setTotalTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setMaxRpcTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setInitialRpcTimeout(
                  org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .build());
    }

    // Set execute streaming sql retry settings
    ServerStreamingCallSettings.Builder<ExecuteSqlRequest, PartialResultSet>
        executeStreamingSqlSettings =
            builder.getSpannerStubSettingsBuilder().executeStreamingSqlSettings();
    if (spannerConfig.getExecuteStreamingSqlRetrySettings() != null) {
      executeStreamingSqlSettings.setRetrySettings(
          spannerConfig.getExecuteStreamingSqlRetrySettings());
    } else {
      // Setting the timeout for streaming read to 2 hours. This is 1 hour by default
      // after BEAM 2.20.
      RetrySettings.Builder executeSqlStreamingRetrySettings =
          executeStreamingSqlSettings.getRetrySettings().toBuilder();
      executeStreamingSqlSettings.setRetrySettings(
          executeSqlStreamingRetrySettings
              .setInitialRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
              .setMaxRpcTimeout(org.threeten.bp.Duration.ofMinutes(120))
              .setTotalTimeout(org.threeten.bp.Duration.ofMinutes(120))
              .build());
    }

    SpannerStubSettings.Builder spannerStubSettingsBuilder =
        builder.getSpannerStubSettingsBuilder();
    ValueProvider<Duration> partitionQueryTimeout = spannerConfig.getPartitionQueryTimeout();
    if (partitionQueryTimeout != null
        && partitionQueryTimeout.get() != null
        && partitionQueryTimeout.get().getMillis() > 0) {
      spannerStubSettingsBuilder
          .partitionQuerySettings()
          .setSimpleTimeoutNoRetries(
              org.threeten.bp.Duration.ofMillis(partitionQueryTimeout.get().getMillis()));
    }
    ValueProvider<Duration> partitionReadTimeout = spannerConfig.getPartitionReadTimeout();
    if (partitionReadTimeout != null
        && partitionReadTimeout.get() != null
        && partitionReadTimeout.get().getMillis() > 0) {
      spannerStubSettingsBuilder
          .partitionReadSettings()
          .setSimpleTimeoutNoRetries(
              org.threeten.bp.Duration.ofMillis(partitionReadTimeout.get().getMillis()));
    }

    ValueProvider<String> projectId = spannerConfig.getProjectId();
    if (projectId != null) {
      builder.setProjectId(projectId.get());
    }
    ServiceFactory<Spanner, SpannerOptions> serviceFactory = spannerConfig.getServiceFactory();
    if (serviceFactory != null) {
      builder.setServiceFactory(serviceFactory);
    }
    ValueProvider<String> host = spannerConfig.getHost();
    if (host != null) {
      builder.setHost(host.get());
    }
    ValueProvider<String> emulatorHost = spannerConfig.getEmulatorHost();
    if (emulatorHost != null) {
      builder.setEmulatorHost(emulatorHost.get());
      if (spannerConfig.getIsLocalChannelProvider() != null
          && spannerConfig.getIsLocalChannelProvider().get()) {
        builder.setChannelProvider(LocalChannelProvider.create(emulatorHost.get()));
      }
      builder.setCredentials(NoCredentials.getInstance());
    }
    String userAgentString = USER_AGENT_PREFIX + "/" + ReleaseInfo.getReleaseInfo().getVersion();
    builder.setHeaderProvider(FixedHeaderProvider.create("user-agent", userAgentString));
    ValueProvider<String> databaseRole = spannerConfig.getDatabaseRole();
    if (databaseRole != null && databaseRole.get() != null && !databaseRole.get().isEmpty()) {
      builder.setDatabaseRole(databaseRole.get());
    }
    ValueProvider<Credentials> credentials = spannerConfig.getCredentials();
    if (credentials != null && credentials.get() != null) {
      builder.setCredentials(credentials.get());
    }
    SessionPoolOptions sessionPoolOptions =
        SessionPoolOptions.newBuilder().setFailIfPoolExhausted().build();
    builder.setSessionPoolOption(sessionPoolOptions);
    return builder.build();
  }

  private static SpannerAccessor createAndConnect(SpannerConfig spannerConfig) {
    SpannerOptions options = buildSpannerOptions(spannerConfig);
    Spanner spanner = options.getService();
    String instanceId = spannerConfig.getInstanceId().get();
    String databaseId = spannerConfig.getDatabaseId().get();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    BatchClient batchClient =
        spanner.getBatchClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();
    String instanceConfigId = "unknown";
    try {
      instanceConfigId =
          spanner
              .getInstanceAdminClient()
              .getInstance(instanceId)
              .getInstanceConfigId()
              .getInstanceConfig();
    } catch (Exception e) {
      // fetch instanceConfigId is fail-free.
      // Do not emit warning when serviceFactory is overridden (e.g. in tests).
      if (spannerConfig.getServiceFactory() == null) {
        LOG.warn("unable to get Spanner instanceConfigId for {}: {}", instanceId, e.getMessage());
      }
    }

    return new SpannerAccessor(
        spanner, databaseClient, databaseAdminClient, batchClient, spannerConfig, instanceConfigId);
  }

  public DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  public BatchClient getBatchClient() {
    return batchClient;
  }

  public DatabaseAdminClient getDatabaseAdminClient() {
    return databaseAdminClient;
  }

  public String getInstanceConfigId() {
    return instanceConfigId;
  }

  @Override
  public void close() {
    // Only close Spanner when present in map and refcount == 0
    synchronized (spannerAccessors) {
      refcount--;
      LOG.debug("close(): refcount={} for {}", refcount, spannerConfig);
      if (refcount <= 0) {
        spannerAccessors.remove(spannerConfig);
        LOG.info("Closing {} ", spannerConfig);
        spanner.close();
      }
    }
  }
}

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

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.CommitResponse;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.MethodDescriptor;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
class SpannerAccessor implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(SpannerAccessor.class);

  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";

  // Only create one SpannerAccessor for each different SpannerConfig.
  private static final ConcurrentHashMap<SpannerConfig, SpannerAccessor> spannerAccessors =
      new ConcurrentHashMap<>();

  // Keep reference counts of each SpannerAccessor's usage so that we can close
  // it when it is no longer in use.
  private static final ConcurrentHashMap<SpannerConfig, AtomicInteger> refcounts =
      new ConcurrentHashMap<>();

  private final Spanner spanner;
  private final DatabaseClient databaseClient;
  private final BatchClient batchClient;
  private final DatabaseAdminClient databaseAdminClient;
  private final SpannerConfig spannerConfig;

  private SpannerAccessor(
      Spanner spanner,
      DatabaseClient databaseClient,
      DatabaseAdminClient databaseAdminClient,
      BatchClient batchClient,
      SpannerConfig spannerConfig) {
    this.spanner = spanner;
    this.databaseClient = databaseClient;
    this.databaseAdminClient = databaseAdminClient;
    this.batchClient = batchClient;
    this.spannerConfig = spannerConfig;
  }

  static SpannerAccessor getOrCreate(SpannerConfig spannerConfig) {

    SpannerAccessor self = spannerAccessors.get(spannerConfig);
    if (self == null) {
      synchronized (spannerAccessors) {
        // Re-check that it has not been created before we got the lock.
        self = spannerAccessors.get(spannerConfig);
        if (self == null) {
          // Connect to spanner for this SpannerConfig.
          LOG.info("Connecting to {}", spannerConfig);
          self = SpannerAccessor.createAndConnect(spannerConfig);
          spannerAccessors.put(spannerConfig, self);
          refcounts.putIfAbsent(spannerConfig, new AtomicInteger(0));
        }
      }
    }
    // Add refcount for this spannerConfig.
    int refcount = refcounts.get(spannerConfig).incrementAndGet();
    LOG.debug("getOrCreate(): refcount={} for {}", refcount, spannerConfig);
    return self;
  }

  private static SpannerAccessor createAndConnect(SpannerConfig spannerConfig) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();

    ValueProvider<Duration> commitDeadline = spannerConfig.getCommitDeadline();
    if (commitDeadline != null && commitDeadline.get().getMillis() > 0) {

      // Set the GRPC deadline on the Commit API call.
      UnaryCallSettings.Builder<CommitRequest, CommitResponse> commitSettings =
          builder.getSpannerStubSettingsBuilder().commitSettings();
      RetrySettings.Builder commitRetrySettings = commitSettings.getRetrySettings().toBuilder();
      commitSettings.setRetrySettings(
          commitRetrySettings
              .setTotalTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setMaxRpcTimeout(org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .setInitialRpcTimeout(
                  org.threeten.bp.Duration.ofMillis(commitDeadline.get().getMillis()))
              .build());
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
    String userAgentString = USER_AGENT_PREFIX + "/" + ReleaseInfo.getReleaseInfo().getVersion();
    builder.setHeaderProvider(FixedHeaderProvider.create("user-agent", userAgentString));
    SpannerOptions options = builder.build();

    Spanner spanner = options.getService();
    String instanceId = spannerConfig.getInstanceId().get();
    String databaseId = spannerConfig.getDatabaseId().get();
    DatabaseClient databaseClient =
        spanner.getDatabaseClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    BatchClient batchClient =
        spanner.getBatchClient(DatabaseId.of(options.getProjectId(), instanceId, databaseId));
    DatabaseAdminClient databaseAdminClient = spanner.getDatabaseAdminClient();

    return new SpannerAccessor(
        spanner, databaseClient, databaseAdminClient, batchClient, spannerConfig);
  }

  DatabaseClient getDatabaseClient() {
    return databaseClient;
  }

  BatchClient getBatchClient() {
    return batchClient;
  }

  DatabaseAdminClient getDatabaseAdminClient() {
    return databaseAdminClient;
  }

  @Override
  public void close() {
    // Only close Spanner when present in map and refcount == 0
    int refcount = refcounts.getOrDefault(spannerConfig, new AtomicInteger(0)).decrementAndGet();
    LOG.debug("close(): refcount={} for {}", refcount, spannerConfig);

    if (refcount == 0) {
      synchronized (spannerAccessors) {
        // Re-check refcount in case it has increased outside the lock.
        if (refcounts.get(spannerConfig).get() <= 0) {
          spannerAccessors.remove(spannerConfig);
          refcounts.remove(spannerConfig);
          LOG.info("Closing {} ", spannerConfig);
          spanner.close();
        }
      }
    }
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

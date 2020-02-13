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

import com.google.api.gax.rpc.FixedHeaderProvider;
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
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.joda.time.Duration;

/** Manages lifecycle of {@link DatabaseClient} and {@link Spanner} instances. */
class SpannerAccessor implements AutoCloseable {
  // A common user agent token that indicates that this request was originated from Apache Beam.
  private static final String USER_AGENT_PREFIX = "Apache_Beam_Java";

  private final Spanner spanner;
  private final DatabaseClient databaseClient;
  private final BatchClient batchClient;
  private final DatabaseAdminClient databaseAdminClient;

  private SpannerAccessor(
      Spanner spanner,
      DatabaseClient databaseClient,
      DatabaseAdminClient databaseAdminClient,
      BatchClient batchClient) {
    this.spanner = spanner;
    this.databaseClient = databaseClient;
    this.databaseAdminClient = databaseAdminClient;
    this.batchClient = batchClient;
  }

  static SpannerAccessor create(SpannerConfig spannerConfig) {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder();

    ValueProvider<Duration> commitDeadline = spannerConfig.getCommitDeadline();
    if (commitDeadline != null && commitDeadline.get().getMillis() > 0) {

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
              .with(new CommitDeadlineSettingInterceptor(commitDeadline.get()));
      builder.setInterceptorProvider(interceptorProvider);
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

    return new SpannerAccessor(spanner, databaseClient, databaseAdminClient, batchClient);
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
    spanner.close();
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

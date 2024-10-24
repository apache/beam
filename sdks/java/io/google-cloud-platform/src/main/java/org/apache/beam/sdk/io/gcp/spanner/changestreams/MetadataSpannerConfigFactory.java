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
package org.apache.beam.sdk.io.gcp.spanner.changestreams;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.auth.Credentials;
import com.google.cloud.spanner.Options.RpcPriority;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;

/**
 * This class generates a SpannerConfig for the change stream metadata database by copying only the
 * necessary fields from the SpannerConfig of the primary database.
 */
public class MetadataSpannerConfigFactory {

  /**
   * Generates a SpannerConfig that can be used to access the change stream metadata database by
   * copying only the necessary fields from the given primary database SpannerConfig and setting the
   * instance ID and database ID to the supplied metadata values.
   *
   * @param primaryConfig The SpannerConfig for accessing the primary database
   * @param metadataInstanceId The instance ID of the metadata database
   * @param metadataDatabaseId The database ID of the metadata database
   * @return the metadata SpannerConfig
   */
  public static SpannerConfig create(
      SpannerConfig primaryConfig, String metadataInstanceId, String metadataDatabaseId) {

    checkNotNull(
        metadataInstanceId,
        "MetadataSpannerConfigFactory.create requires non-null metadata instance id");
    checkNotNull(
        metadataDatabaseId,
        "MetadataSpannerConfigFactory.create requires non-null metadata database id");

    // NOTE: databaseRole should NOT be copied to the metadata config

    SpannerConfig config =
        SpannerConfig.create()
            .withInstanceId(StaticValueProvider.of(metadataInstanceId))
            .withDatabaseId(StaticValueProvider.of(metadataDatabaseId));

    ValueProvider<String> projectId = primaryConfig.getProjectId();
    if (projectId != null) {
      config = config.withProjectId(StaticValueProvider.of(projectId.get()));
    }

    ValueProvider<String> host = primaryConfig.getHost();
    if (host != null) {
      config = config.withHost(StaticValueProvider.of(host.get()));
    }

    ValueProvider<String> emulatorHost = primaryConfig.getEmulatorHost();
    if (emulatorHost != null) {
      config = config.withEmulatorHost(StaticValueProvider.of(emulatorHost.get()));
    }

    ValueProvider<Boolean> isLocalChannelProvider = primaryConfig.getIsLocalChannelProvider();
    if (isLocalChannelProvider != null) {
      config =
          config.withIsLocalChannelProvider(StaticValueProvider.of(isLocalChannelProvider.get()));
    }

    ValueProvider<Duration> commitDeadline = primaryConfig.getCommitDeadline();
    if (commitDeadline != null) {
      config = config.withCommitDeadline(StaticValueProvider.of(Duration.standardSeconds(60)));
    }

    ValueProvider<Duration> maxCumulativeBackoff = primaryConfig.getMaxCumulativeBackoff();
    if (maxCumulativeBackoff != null) {
      config = config.withMaxCumulativeBackoff(StaticValueProvider.of(maxCumulativeBackoff.get()));
    }

    RetrySettings executeStreamingSqlRetrySettings =
        primaryConfig.getExecuteStreamingSqlRetrySettings();
    if (executeStreamingSqlRetrySettings != null) {
      config = config.withExecuteStreamingSqlRetrySettings(executeStreamingSqlRetrySettings);
    }

    RetrySettings commitRetrySettings = primaryConfig.getCommitRetrySettings();
    if (commitRetrySettings != null) {
      config = config.withCommitRetrySettings(commitRetrySettings);
    }

    ImmutableSet<Code> retryableCodes = primaryConfig.getRetryableCodes();
    if (retryableCodes != null) {
      config = config.withRetryableCodes(retryableCodes);
    }

    ValueProvider<RpcPriority> rpcPriority = primaryConfig.getRpcPriority();
    if (rpcPriority != null) {
      config = config.withRpcPriority(StaticValueProvider.of(rpcPriority.get()));
    }

    ValueProvider<Credentials> credentials = primaryConfig.getCredentials();
    if (credentials != null) {
      config = config.withCredentials(credentials);
    }

    return config;
  }
}

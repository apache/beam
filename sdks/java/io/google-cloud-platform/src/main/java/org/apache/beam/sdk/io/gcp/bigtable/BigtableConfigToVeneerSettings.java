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
package org.apache.beam.sdk.io.gcp.bigtable;

import com.google.api.core.ApiFunction;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.StubSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import io.grpc.ManagedChannelBuilder;
import io.grpc.internal.GrpcUtil;
import java.io.IOException;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.initialization.qual.UnderInitialization;

/** Helper class to translate {@link BigtableOptions} to Bigtable Veneer settings. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableConfigToVeneerSettings {
  private static final String DEFAULT_DATA_ENDPOINT = "bigtable.googleapis.com:443";
  private static final String DEFAULT_ADMIN_ENDPOINT = "bigtableadmin.googleapis.com:443";

  private final BigtableDataSettings dataSettings;
  private final BigtableTableAdminSettings tableAdminSettings;

  static BigtableConfigToVeneerSettings create(@Nonnull BigtableConfig config) throws IOException {
    return new BigtableConfigToVeneerSettings(config);
  }

  private BigtableConfigToVeneerSettings(@Nonnull BigtableConfig config) throws IOException {
    if (config.getProjectId() == null || config.getInstanceId() == null) {
      throw new IOException("can't find project or instance id");
    }

    // Build configs for veneer
    this.dataSettings = buildBigtableDataSettings(config);
    this.tableAdminSettings = buildBigtableTableAdminSettings(config);
  }

  // ************** Getters **************
  /** Utility to convert {@link BigtableOptions} to {@link BigtableDataSettings}. */
  BigtableDataSettings getDataSettings() {
    return dataSettings;
  }

  /** Utility to convert {@link BigtableOptions} to {@link BigtableTableAdminSettings}. */
  BigtableTableAdminSettings getTableAdminSettings() {
    return tableAdminSettings;
  }

  // ************** Private Helpers **************
  private BigtableDataSettings buildBigtableDataSettings(
      @UnderInitialization BigtableConfigToVeneerSettings this, BigtableConfig config)
      throws IOException {
    BigtableDataSettings.Builder dataBuilder;

    // Configure the Data connection
    dataBuilder = BigtableDataSettings.newBuilder();
    if (config.getEmulatorHost() != null) {
      configureConnection(
          dataBuilder.stubSettings(), config, Objects.requireNonNull(config.getEmulatorHost()));
    } else {
      configureConnection(dataBuilder.stubSettings(), config, DEFAULT_DATA_ENDPOINT);
    }
    configureCredentialProvider(dataBuilder.stubSettings(), config);
    configureHeaderProvider(dataBuilder.stubSettings(), config);

    // Configure the target
    dataBuilder
        .setProjectId(Objects.requireNonNull(config.getProjectId().get()))
        .setInstanceId(Objects.requireNonNull(config.getInstanceId().get()));
    if (config.getAppProfileId() != null) {
      dataBuilder.setAppProfileId(Objects.requireNonNull(config.getAppProfileId().get()));
    }

    return dataBuilder.build();
  }

  private BigtableTableAdminSettings buildBigtableTableAdminSettings(
      @UnderInitialization BigtableConfigToVeneerSettings this, BigtableConfig config)
      throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder;

    // Configure connection
    adminBuilder = BigtableTableAdminSettings.newBuilder();
    if (config.getEmulatorHost() != null) {
      configureConnection(
          adminBuilder.stubSettings(), config, Objects.requireNonNull(config.getEmulatorHost()));
    } else {
      configureConnection(adminBuilder.stubSettings(), config, DEFAULT_ADMIN_ENDPOINT);
    }

    configureCredentialProvider(adminBuilder.stubSettings(), config);

    adminBuilder
        .setProjectId(Objects.requireNonNull(config.getProjectId().get()))
        .setInstanceId(Objects.requireNonNull(config.getInstanceId().get()));

    return adminBuilder.build();
  }

  @SuppressWarnings("rawtypes")
  private void configureConnection(
      @UnderInitialization BigtableConfigToVeneerSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableConfig config,
      String endpoint) {
    stubSettings.setEndpoint(endpoint);

    final InstantiatingGrpcChannelProvider.Builder channelProvider =
        ((InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider()).toBuilder();

    if (config.getEmulatorHost() != null) {
      @SuppressWarnings("rawtypes")
      final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> prevConfigurator =
          channelProvider.getChannelConfigurator();
      //noinspection rawtypes
      channelProvider.setChannelConfigurator(
          new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
            @Override
            public ManagedChannelBuilder apply(ManagedChannelBuilder channelBuilder) {
              if (prevConfigurator != null) {
                channelBuilder = prevConfigurator.apply(channelBuilder);
              }
              return channelBuilder.usePlaintext();
            }
          });
    }

    channelProvider.setChannelPoolSettings(
        ChannelPoolSettings.builder()
            .setMaxRpcsPerChannel(50)
            .setMinRpcsPerChannel(1)
            .setPreemptiveRefreshEnabled(true)
            .setInitialChannelCount(1)
            .setMinChannelCount(1)
            .setMaxChannelCount(4)
            .build());
    stubSettings.setTransportChannelProvider(channelProvider.build());
  }

  private void configureHeaderProvider(
      @UnderInitialization BigtableConfigToVeneerSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableConfig config) {

    ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.<String, String>builder();
    headersBuilder.putAll(stubSettings.getHeaderProvider().getHeaders());
    headersBuilder.put(
        GrpcUtil.USER_AGENT_KEY.name(), Objects.requireNonNull(config.getUserAgent()));

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()));
  }

  private void configureCredentialProvider(
      @UnderInitialization BigtableConfigToVeneerSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableConfig config) {
    stubSettings.setCredentialsProvider(FixedCredentialsProvider.create(config.getCredentials()));
  }
}

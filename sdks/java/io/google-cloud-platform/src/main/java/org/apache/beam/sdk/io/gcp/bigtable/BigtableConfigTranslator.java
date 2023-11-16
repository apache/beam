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

import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.grpc.ChannelPoolSettings;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.StubSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import io.grpc.internal.GrpcUtil;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Objects;
import org.apache.beam.sdk.extensions.gcp.auth.CredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.threeten.bp.Duration;

/**
 * Helper class to translate {@link BigtableConfig}, {@link BigtableReadOptions}, {@link
 * BigtableWriteOptions} and {@link PipelineOptions} to Bigtable Veneer settings.
 *
 * <p>Also translate {@link BigtableOptions} to {@link BigtableConfig} for backward compatibility.
 * If the values are set on {@link BigtableConfig} directly, ignore the settings in {@link
 * BigtableOptions}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableConfigTranslator {

  /** Translate BigtableConfig and BigtableReadOptions to Veneer settings. */
  static BigtableDataSettings translateReadToVeneerSettings(
      @NonNull BigtableConfig config,
      @NonNull BigtableReadOptions options,
      @NonNull PipelineOptions pipelineOptions)
      throws IOException {
    BigtableDataSettings.Builder settings = buildBigtableDataSettings(config, pipelineOptions);
    return configureReadSettings(settings, options);
  }

  /** Translate BigtableConfig and BigtableWriteOptions to Veneer settings. */
  static BigtableDataSettings translateWriteToVeneerSettings(
      @NonNull BigtableConfig config,
      @NonNull BigtableWriteOptions options,
      @NonNull PipelineOptions pipelineOptions)
      throws IOException {

    BigtableDataSettings.Builder settings = buildBigtableDataSettings(config, pipelineOptions);
    return configureWriteSettings(settings, options);
  }

  /** Translate BigtableConfig and BigtableWriteOptions to Veneer settings. */
  static BigtableDataSettings translateToVeneerSettings(
      @NonNull BigtableConfig config, @NonNull PipelineOptions pipelineOptions) throws IOException {

    return buildBigtableDataSettings(config, pipelineOptions).build();
  }

  private static BigtableDataSettings.Builder buildBigtableDataSettings(
      BigtableConfig config, PipelineOptions pipelineOptions) throws IOException {
    BigtableDataSettings.Builder dataBuilder;
    boolean emulator = false;
    if (!Strings.isNullOrEmpty(config.getEmulatorHost())) {
      emulator = true;
      String hostAndPort = config.getEmulatorHost();
      try {
        int lastIndexOfCol = hostAndPort.lastIndexOf(":");
        int port = Integer.parseInt(hostAndPort.substring(lastIndexOfCol + 1));
        dataBuilder =
            BigtableDataSettings.newBuilderForEmulator(
                hostAndPort.substring(0, lastIndexOfCol), port);
      } catch (NumberFormatException | IndexOutOfBoundsException ex) {
        throw new RuntimeException("Invalid host/port in BigtableConfig " + hostAndPort);
      }
    } else {
      dataBuilder = BigtableDataSettings.newBuilder();
    }

    // Configure target
    dataBuilder
        .setProjectId(Objects.requireNonNull(config.getProjectId().get()))
        .setInstanceId(Objects.requireNonNull(config.getInstanceId().get()));
    if (config.getAppProfileId() != null
        && !Strings.isNullOrEmpty(config.getAppProfileId().get())) {
      dataBuilder.setAppProfileId(Objects.requireNonNull(config.getAppProfileId().get()));
    }

    // Skip resetting the credentials if it's connected to an emulator
    if (!emulator) {
      if (pipelineOptions.as(GcpOptions.class).getGcpCredential() != null) {
        dataBuilder
            .stubSettings()
            .setCredentialsProvider(
                FixedCredentialsProvider.create(
                    (pipelineOptions.as(GcpOptions.class)).getGcpCredential()));
      }

      if (config.getCredentialFactory() != null) {
        CredentialFactory credentialFactory = config.getCredentialFactory();
        try {
          dataBuilder
              .stubSettings()
              .setCredentialsProvider(
                  FixedCredentialsProvider.create(credentialFactory.getCredential()));
        } catch (GeneralSecurityException e) {
          throw new RuntimeException("Exception getting credentials ", e);
        }
      }
    }

    configureChannelPool(dataBuilder.stubSettings(), config);
    configureHeaderProvider(dataBuilder.stubSettings(), pipelineOptions);

    return dataBuilder;
  }

  private static void configureHeaderProvider(
      StubSettings.Builder<?, ?> stubSettings, PipelineOptions pipelineOptions) {

    ImmutableMap.Builder<String, String> headersBuilder =
        ImmutableMap.<String, String>builder()
            .put(
                GrpcUtil.USER_AGENT_KEY.name(),
                Objects.requireNonNull(pipelineOptions.getUserAgent()));

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()));
  }

  private static void configureChannelPool(
      StubSettings.Builder<?, ?> stubSettings, BigtableConfig config) {
    if (config.getChannelCount() != null) {
      InstantiatingGrpcChannelProvider grpcChannelProvider =
          (InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider();
      stubSettings.setTransportChannelProvider(
          grpcChannelProvider
              .toBuilder()
              .setChannelPoolSettings(ChannelPoolSettings.staticallySized(config.getChannelCount()))
              .build());
    }
  }

  private static BigtableDataSettings configureWriteSettings(
      BigtableDataSettings.Builder settings, BigtableWriteOptions writeOptions) {
    BigtableBatchingCallSettings.Builder callSettings =
        settings.stubSettings().bulkMutateRowsSettings();
    RetrySettings.Builder retrySettings = callSettings.getRetrySettings().toBuilder();
    BatchingSettings.Builder batchingSettings = callSettings.getBatchingSettings().toBuilder();

    // The default attempt timeout for version <= 2.46.0 is 6 minutes. Reset the timeout to align
    // with the old behavior.
    Duration attemptTimeout = Duration.ofMinutes(6);

    if (writeOptions.getAttemptTimeout() != null) {
      attemptTimeout = Duration.ofMillis(writeOptions.getAttemptTimeout().getMillis());
    }
    retrySettings.setInitialRpcTimeout(attemptTimeout).setMaxRpcTimeout(attemptTimeout);
    // Expand the operation timeout if it's shorter
    retrySettings.setTotalTimeout(
        Duration.ofMillis(
            Math.max(retrySettings.getTotalTimeout().toMillis(), attemptTimeout.toMillis())));

    if (writeOptions.getOperationTimeout() != null) {
      retrySettings.setTotalTimeout(
          Duration.ofMillis(writeOptions.getOperationTimeout().getMillis()));
    }

    if (writeOptions.getMaxElementsPerBatch() != null) {
      batchingSettings.setElementCountThreshold(writeOptions.getMaxElementsPerBatch());
    }

    if (writeOptions.getMaxBytesPerBatch() != null) {
      batchingSettings.setRequestByteThreshold(writeOptions.getMaxBytesPerBatch());
    }

    FlowControlSettings.Builder flowControlSettings =
        callSettings.getBatchingSettings().getFlowControlSettings().toBuilder();
    if (writeOptions.getMaxOutstandingElements() != null) {
      flowControlSettings.setMaxOutstandingElementCount(writeOptions.getMaxOutstandingElements());
    }
    if (writeOptions.getMaxOutstandingBytes() != null) {
      flowControlSettings.setMaxOutstandingRequestBytes(writeOptions.getMaxOutstandingBytes());
    }
    batchingSettings = batchingSettings.setFlowControlSettings(flowControlSettings.build());

    if (writeOptions.getThrottlingTargetMs() != null) {
      settings.enableBatchMutationLatencyBasedThrottling(writeOptions.getThrottlingTargetMs());
    }

    if (Boolean.TRUE.equals(writeOptions.getFlowControl())) {
      settings.setBulkMutationFlowControl(true);
    }

    settings
        .stubSettings()
        .bulkMutateRowsSettings()
        .setRetrySettings(retrySettings.build())
        .setBatchingSettings(batchingSettings.build());

    return settings.build();
  }

  private static BigtableDataSettings configureReadSettings(
      BigtableDataSettings.Builder settings, BigtableReadOptions readOptions) {

    RetrySettings.Builder retrySettings =
        settings.stubSettings().readRowsSettings().getRetrySettings().toBuilder();

    if (readOptions.getAttemptTimeout() != null) {
      // Set the user specified attempt timeout and expand the operation timeout if it's shorter
      retrySettings.setInitialRpcTimeout(
          Duration.ofMillis(readOptions.getAttemptTimeout().getMillis()));
      retrySettings.setTotalTimeout(
          Duration.ofMillis(
              Math.max(
                  retrySettings.getTotalTimeout().toMillis(),
                  readOptions.getAttemptTimeout().getMillis())));
    }

    if (readOptions.getOperationTimeout() != null) {
      retrySettings.setTotalTimeout(
          Duration.ofMillis(readOptions.getOperationTimeout().getMillis()));
    }

    settings.stubSettings().readRowsSettings().setRetrySettings(retrySettings.build());

    return settings.build();
  }

  /**
   * Translate BigtableOptions to BigtableConfig for backward compatibility. If the values are set
   * on BigtableConfig, ignore the settings in BigtableOptions.
   */
  static BigtableConfig translateToBigtableConfig(BigtableConfig config, BigtableOptions options) {
    BigtableConfig.Builder builder = config.toBuilder();

    if (options.getProjectId() != null && config.getProjectId() == null) {
      builder.setProjectId(ValueProvider.StaticValueProvider.of(options.getProjectId()));
    }

    if (options.getInstanceId() != null && config.getInstanceId() == null) {
      builder.setInstanceId(ValueProvider.StaticValueProvider.of(options.getInstanceId()));
    }

    if (options.getAppProfileId() != null && config.getAppProfileId() == null) {
      builder.setAppProfileId(ValueProvider.StaticValueProvider.of(options.getAppProfileId()));
    }

    if (options.getCredentialOptions().getCredentialType() == CredentialOptions.CredentialType.None
        && config.getEmulatorHost() == null) {
      builder.setEmulatorHost(String.format("%s:%s", options.getDataHost(), options.getPort()));
    }

    builder.setChannelCount(options.getChannelCount());

    if (options.getCredentialOptions() != null) {
      try {
        CredentialOptions credOptions = options.getCredentialOptions();
        switch (credOptions.getCredentialType()) {
          case DefaultCredentials:
            // Veneer uses default credentials, so no need to reset here
            break;
          case P12:
            String keyFile = ((CredentialOptions.P12CredentialOptions) credOptions).getKeyFile();
            String serviceAccount =
                ((CredentialOptions.P12CredentialOptions) credOptions).getServiceAccount();
            try {
              KeyStore keyStore = KeyStore.getInstance("PKCS12");

              try (FileInputStream fin = new FileInputStream(keyFile)) {
                keyStore.load(fin, "notasecret".toCharArray());
              }
              PrivateKey privateKey =
                  (PrivateKey) keyStore.getKey("privatekey", "notasecret".toCharArray());

              if (privateKey == null) {
                throw new IllegalStateException("private key cannot be null");
              }
              Credentials credentials =
                  ServiceAccountJwtAccessCredentials.newBuilder()
                      .setClientEmail(serviceAccount)
                      .setPrivateKey(privateKey)
                      .build();
              builder.setCredentialFactory(FixedCredentialFactory.create(credentials));
            } catch (GeneralSecurityException exception) {
              throw new RuntimeException("exception while retrieving credentials", exception);
            }
            break;
          case SuppliedCredentials:
            Credentials credentials =
                ((CredentialOptions.UserSuppliedCredentialOptions) credOptions).getCredential();
            builder.setCredentialFactory(FixedCredentialFactory.create(credentials));
            break;
          case SuppliedJson:
            CredentialOptions.JsonCredentialsOptions jsonCredentialsOptions =
                (CredentialOptions.JsonCredentialsOptions) credOptions;
            builder.setCredentialFactory(
                FixedCredentialFactory.create(
                    GoogleCredentials.fromStream(jsonCredentialsOptions.getInputStream())));
            break;
          case None:
            // pipelineOptions is ignored
            PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
            builder.setCredentialFactory(NoopCredentialFactory.fromOptions(pipelineOptions));
            break;
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to translate BigtableOptions to BigtableConfig", e);
      }
    }

    return builder.build();
  }

  /** Translate BigtableOptions to BigtableReadOptions. */
  static BigtableReadOptions translateToBigtableReadOptions(
      BigtableReadOptions readOptions, BigtableOptions options) {
    BigtableReadOptions.Builder builder = readOptions.toBuilder();
    builder.setWaitTimeout(
        org.joda.time.Duration.millis(options.getRetryOptions().getReadPartialRowTimeoutMillis()));
    if (options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().isPresent()) {
      builder.setAttemptTimeout(
          org.joda.time.Duration.millis(
              options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().get()));
    }
    builder.setOperationTimeout(
        org.joda.time.Duration.millis(options.getCallOptionsConfig().getReadStreamRpcTimeoutMs()));
    return builder.build();
  }

  /** Translate BigtableOptions to BigtableWriteOptions. */
  static BigtableWriteOptions translateToBigtableWriteOptions(
      BigtableWriteOptions writeOptions, BigtableOptions options) {

    BigtableWriteOptions.Builder builder = writeOptions.toBuilder();
    // configure timeouts
    if (options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().isPresent()) {
      builder.setAttemptTimeout(
          org.joda.time.Duration.millis(
              options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().get()));
    }
    if (options.getBulkOptions().isEnableBulkMutationThrottling()) {
      builder.setThrottlingTargetMs(options.getBulkOptions().getBulkMutationRpcTargetMs());
    }
    builder.setOperationTimeout(
        org.joda.time.Duration.millis(options.getCallOptionsConfig().getMutateRpcTimeoutMs()));
    // configure batch size
    builder.setMaxElementsPerBatch(options.getBulkOptions().getBulkMaxRowKeyCount());
    builder.setMaxBytesPerBatch(options.getBulkOptions().getBulkMaxRequestSize());
    builder.setMaxOutstandingElements(
        options.getBulkOptions().getMaxInflightRpcs()
            * (long) options.getBulkOptions().getBulkMaxRowKeyCount());
    builder.setMaxOutstandingBytes(
        options.getBulkOptions().getMaxInflightRpcs()
            * options.getBulkOptions().getBulkMaxRequestSize());

    return builder.build();
  }
}

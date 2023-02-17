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
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
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
import javax.annotation.Nonnull;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.threeten.bp.Duration;

/** Helper class to translate {@link BigtableOptions} to Bigtable Veneer settings. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class BigtableConfigTranslator {

  /** Translate BigtableConfig and BigtableReadOptions to Veneer settings. */
  static BigtableDataSettings translateReadToVeneerSettings(
      @Nonnull BigtableConfig config,
      @Nonnull BigtableReadOptions options,
      @Nonnull PipelineOptions pipelineOptions) {
    BigtableDataSettings.Builder settings = buildBigtableDataSettings(config, pipelineOptions);
    return configureReadSettings(settings, options);
  }

  /** Translate BigtableConfig and BigtableWriteOptions to Veneer settings. */
  static BigtableDataSettings translateWriteToVeneerSettings(
      @Nonnull BigtableConfig config,
      @Nonnull BigtableWriteOptions options,
      @Nonnull PipelineOptions pipelineOptions) {

    BigtableDataSettings.Builder settings = buildBigtableDataSettings(config, pipelineOptions);
    return configureWriteSettings(settings, options);
  }

  /** Translate BigtableConfig and BigtableWriteOptions to Veneer settings. */
  static BigtableDataSettings translateToVeneerSettings(
      @Nonnull BigtableConfig config, @Nonnull PipelineOptions pipelineOptions) {

    return buildBigtableDataSettings(config, pipelineOptions).build();
  }

  private static BigtableDataSettings.Builder buildBigtableDataSettings(
      BigtableConfig config, PipelineOptions pipelineOptions) {
    BigtableDataSettings.Builder dataBuilder;
    if (!Strings.isNullOrEmpty(config.getEmulatorHost())) {
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

    // Configure credentials, check both PipelineOptions and BigtableConfig
    if (pipelineOptions instanceof GcpOptions) {
      dataBuilder
          .stubSettings()
          .setCredentialsProvider(
              FixedCredentialsProvider.create(((GcpOptions) pipelineOptions).getGcpCredential()));
    }

    if (config.getCredentialsProvider() != null) {
      dataBuilder.stubSettings().setCredentialsProvider(config.getCredentialsProvider());
    }

    configureHeaderProvider(dataBuilder.stubSettings(), pipelineOptions);

    return dataBuilder;
  }

  private static void configureHeaderProvider(
      StubSettings.Builder<?, ?> stubSettings, PipelineOptions pipelineOptions) {

    ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.<String, String>builder();
    headersBuilder.putAll(stubSettings.getHeaderProvider().getHeaders());
    headersBuilder.put(
        GrpcUtil.USER_AGENT_KEY.name(), Objects.requireNonNull(pipelineOptions.getUserAgent()));

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()));
  }

  private static BigtableDataSettings configureWriteSettings(
      BigtableDataSettings.Builder settings, BigtableWriteOptions writeOptions) {
    BigtableBatchingCallSettings.Builder callSettings =
        settings.stubSettings().bulkMutateRowsSettings();
    RetrySettings.Builder retrySettings = callSettings.getRetrySettings().toBuilder();
    BatchingSettings.Builder batchingSettings = callSettings.getBatchingSettings().toBuilder();
    if (writeOptions.getAttemptTimeout() != null) {
      retrySettings.setInitialRpcTimeout(Duration.ofMillis(writeOptions.getAttemptTimeout()));

      if (writeOptions.getOperationTimeout() == null) {
        retrySettings.setTotalTimeout(
            Duration.ofMillis(
                Math.max(
                    retrySettings.getTotalTimeout().toMillis(), writeOptions.getAttemptTimeout())));
      }
    }

    if (writeOptions.getOperationTimeout() != null) {
      retrySettings.setTotalTimeout(Duration.ofMillis(writeOptions.getOperationTimeout()));
    }

    if (writeOptions.getRetryInitialDelay() != null) {
      retrySettings.setInitialRetryDelay(Duration.ofMillis(writeOptions.getRetryInitialDelay()));
    }

    if (writeOptions.getRetryDelayMultiplier() != null) {
      retrySettings.setRetryDelayMultiplier(writeOptions.getRetryDelayMultiplier());
    }

    if (writeOptions.getBatchElements() != null) {
      batchingSettings.setElementCountThreshold(writeOptions.getBatchElements());
    }

    if (writeOptions.getBatchBytes() != null) {
      batchingSettings.setRequestByteThreshold(writeOptions.getBatchBytes());
    }

    if (writeOptions.getMaxRequests() != null) {
      BatchingSettings tmpSettings = batchingSettings.build();
      batchingSettings =
          batchingSettings.setFlowControlSettings(
              callSettings
                  .getBatchingSettings()
                  .getFlowControlSettings()
                  .toBuilder()
                  .setMaxOutstandingElementCount(
                      tmpSettings.getElementCountThreshold() * writeOptions.getMaxRequests())
                  .setMaxOutstandingRequestBytes(
                      tmpSettings.getRequestByteThreshold() * writeOptions.getMaxRequests())
                  .build());
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
      retrySettings.setInitialRpcTimeout(Duration.ofMillis(readOptions.getAttemptTimeout()));

      if (readOptions.getOperationTimeout() == null) {
        retrySettings.setTotalTimeout(
            Duration.ofMillis(
                Math.max(
                    retrySettings.getTotalTimeout().toMillis(), readOptions.getAttemptTimeout())));
      }
    }

    if (readOptions.getOperationTimeout() != null) {
      retrySettings.setTotalTimeout(Duration.ofMillis(readOptions.getOperationTimeout()));
    }

    if (readOptions.getRetryDelayMultiplier() != null) {
      retrySettings.setRetryDelayMultiplier(readOptions.getRetryDelayMultiplier());
    }

    if (readOptions.getRetryInitialDelay() != null) {
      retrySettings.setInitialRetryDelay(Duration.ofMillis(readOptions.getRetryInitialDelay()));
    }

    settings.stubSettings().readRowsSettings().setRetrySettings(retrySettings.build());

    return settings.build();
  }

  /** Translate BigtableOptions to BigtableConfig. */
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

    if (options.getCredentialOptions() != null) {
      try {
        CredentialOptions credOptions = options.getCredentialOptions();
        switch (credOptions.getCredentialType()) {
          case DefaultCredentials:
            // Veneer uses GoogleDefaultCredentials, so we don't need to reset it
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
              builder.setCredentialsProvider(
                  FixedCredentialsProvider.create(
                      ServiceAccountJwtAccessCredentials.newBuilder()
                          .setClientEmail(serviceAccount)
                          .setPrivateKey(privateKey)
                          .build()));
            } catch (GeneralSecurityException exception) {
              throw new RuntimeException("exception while retrieving credentials", exception);
            }
            break;
          case SuppliedCredentials:
            Credentials credentials =
                ((CredentialOptions.UserSuppliedCredentialOptions) credOptions).getCredential();
            builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            break;
          case SuppliedJson:
            CredentialOptions.JsonCredentialsOptions jsonCredentialsOptions =
                (CredentialOptions.JsonCredentialsOptions) credOptions;
            builder.setCredentialsProvider(
                FixedCredentialsProvider.create(
                    GoogleCredentials.fromStream(jsonCredentialsOptions.getInputStream())));
            break;
          case None:
            builder.setCredentialsProvider(NoCredentialsProvider.create());
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
    if (options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().isPresent()) {
      builder.setAttemptTimeout(
          options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().get());
    }
    builder.setOperationTimeout(options.getCallOptionsConfig().getReadStreamRpcTimeoutMs());
    builder.setRetryInitialDelay(options.getRetryOptions().getInitialBackoffMillis());
    builder.setRetryDelayMultiplier(options.getRetryOptions().getBackoffMultiplier());
    return builder.build();
  }

  /** Translate BigtableOptions to BigtableWriteOptions. */
  static BigtableWriteOptions translateToBigtableWriteOptions(
      BigtableWriteOptions writeOptions, BigtableOptions options) {

    BigtableWriteOptions.Builder builder = writeOptions.toBuilder();
    // configure timeouts
    if (options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().isPresent()) {
      builder.setAttemptTimeout(
          options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().get());
    }
    builder.setOperationTimeout(options.getCallOptionsConfig().getMutateRpcTimeoutMs());
    // configure retry backoffs
    builder.setRetryInitialDelay(options.getRetryOptions().getInitialBackoffMillis());
    builder.setRetryDelayMultiplier(options.getRetryOptions().getBackoffMultiplier());
    // configure batch size
    builder.setBatchElements(options.getBulkOptions().getBulkMaxRowKeyCount());
    builder.setBatchBytes(options.getBulkOptions().getBulkMaxRequestSize());
    builder.setMaxRequests(options.getBulkOptions().getMaxInflightRpcs());

    return builder.build();
  }
}

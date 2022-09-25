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
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.ServerStreamingCallSettings;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StubSettings;
import com.google.api.gax.rpc.UnaryCallSettings;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.bigtable.Version;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.stub.BigtableBatchingCallSettings;
import com.google.cloud.bigtable.data.v2.stub.BigtableBulkReadRowsCallSettings;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.ManagedChannelBuilder;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.internal.GrpcUtil;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Optional;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.threeten.bp.Duration;

/** Helper class to translate {@link BigtableOptions} to Bigtable Veneer settings. */
class BigtableHBaseVeneeringSettings {
  private static final String DEFAULT_BIGTABLE_BATCH_DATA_ENDPOINT =
      "batch-bigtable.googleapis.com:443";
  private static final Duration DEFAULT_UNARY_ATTEMPT_TIMEOUTS = Duration.ofSeconds(20);
  private static final Duration DEFAULT_BULK_MUTATE_ATTEMPT_TIMEOUTS = Duration.ofMinutes(6);

  private final BigtableDataSettings dataSettings;
  private final BigtableTableAdminSettings tableAdminSettings;
  private final BigtableInstanceAdminSettings instanceAdminSettings;

  private final BigtableIOOperationTimeouts clientTimeouts;

  static BigtableHBaseVeneeringSettings create(@Nonnull BigtableOptions options)
      throws IOException {
    return new BigtableHBaseVeneeringSettings(options);
  }

  private BigtableHBaseVeneeringSettings(@Nonnull BigtableOptions options) throws IOException {
    // Build configs for veneer
    this.clientTimeouts = buildCallSettings(options);

    this.dataSettings = buildBigtableDataSettings(clientTimeouts, options);
    this.tableAdminSettings = buildBigtableTableAdminSettings(options);
    this.instanceAdminSettings = buildBigtableInstanceAdminSettings(options);
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

  BigtableIOOperationTimeouts getOperationTimeouts() {
    return clientTimeouts;
  }

  /** Utility to convert {@link BigtableOptions} to {@link BigtableInstanceAdminSettings}. */
  BigtableInstanceAdminSettings getInstanceAdminSettings() {
    return instanceAdminSettings;
  }

  // ************** Private Helpers **************
  private BigtableDataSettings buildBigtableDataSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      BigtableIOOperationTimeouts clientTimeouts,
      BigtableOptions options)
      throws IOException {
    BigtableDataSettings.Builder dataBuilder;

    // Configure the Data connection
    dataBuilder = BigtableDataSettings.newBuilder();
    if (options.useBatch()) {
      configureConnection(
          dataBuilder.stubSettings(), DEFAULT_BIGTABLE_BATCH_DATA_ENDPOINT, options);
    } else {
      configureConnection(
          dataBuilder.stubSettings(), options.getDataHost() + ":" + options.getPort(), options);
    }
    configureCredentialProvider(dataBuilder.stubSettings(), options);
    configureHeaderProvider(dataBuilder.stubSettings(), options);

    // Configure the target
    dataBuilder.setProjectId(options.getProjectId()).setInstanceId(options.getInstanceId());
    if (options.getAppProfileId() != null) {
      dataBuilder.setAppProfileId(options.getAppProfileId());
    }

    // Configure RPCs - this happens in multiple parts:
    // - retry settings are configured here
    // - timeouts are split into multiple places:
    //   - timeouts for retries are configured here
    //   - if USE_TIMEOUTS is explicitly disabled, then an interceptor is added to force all
    // deadlines to 6 minutes
    configureConnectionCallTimeouts(dataBuilder.stubSettings(), clientTimeouts);

    // Complex RPC method settings
    configureBulkMutationSettings(
        dataBuilder.stubSettings().bulkMutateRowsSettings(),
        clientTimeouts.getBulkMutateTimeouts(),
        options);
    configureBulkReadRowsSettings(
        dataBuilder.stubSettings().bulkReadRowsSettings(),
        clientTimeouts.getBulkReadRowsTimeouts(),
        options);
    configureReadRowsSettings(
        dataBuilder.stubSettings().readRowsSettings(),
        clientTimeouts.getBulkReadRowsTimeouts(),
        options);

    // RPC methods - simple
    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().checkAndMutateRowSettings(), clientTimeouts.getUnaryTimeouts());
    configureNonRetryableCallSettings(
        dataBuilder.stubSettings().readModifyWriteRowSettings(), clientTimeouts.getUnaryTimeouts());

    configureRetryableCallSettings(
        dataBuilder.stubSettings().mutateRowSettings(), clientTimeouts.getUnaryTimeouts(), options);
    configureRetryableCallSettings(
        dataBuilder.stubSettings().readRowSettings(), clientTimeouts.getUnaryTimeouts(), options);
    configureRetryableCallSettings(
        dataBuilder.stubSettings().sampleRowKeysSettings(),
        clientTimeouts.getUnaryTimeouts(),
        options);

    return dataBuilder.build();
  }

  private BigtableTableAdminSettings buildBigtableTableAdminSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options)
      throws IOException {
    BigtableTableAdminSettings.Builder adminBuilder;

    // Configure connection
    adminBuilder = BigtableTableAdminSettings.newBuilder();
    configureConnection(
        adminBuilder.stubSettings(), options.getAdminHost() + ":" + options.getPort(), options);
    configureCredentialProvider(adminBuilder.stubSettings(), options);

    configureHeaderProvider(adminBuilder.stubSettings(), options);

    adminBuilder.setProjectId(options.getProjectId()).setInstanceId(options.getInstanceId());

    // timeout/retry settings don't apply to admin operations
    // v1 used to use RetryOptions for:
    // - createTable
    // - getTable
    // - listTables
    // - deleteTable
    // - modifyColumnFamilies
    // - dropRowRange
    // However data latencies are very different from data latencies and end users shouldn't need to
    // change the defaults
    // if it turns out that the timeout & retry behavior needs to be configurable, we will expose
    // separate settings

    return adminBuilder.build();
  }

  private BigtableInstanceAdminSettings buildBigtableInstanceAdminSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options)
      throws IOException {
    BigtableInstanceAdminSettings.Builder adminBuilder;

    // Configure connection
    adminBuilder = BigtableInstanceAdminSettings.newBuilder();
    configureConnection(
        adminBuilder.stubSettings(), options.getAdminHost() + ":" + options.getPort(), options);
    configureCredentialProvider(adminBuilder.stubSettings(), options);

    configureHeaderProvider(adminBuilder.stubSettings(), options);

    adminBuilder.setProjectId(options.getProjectId());

    return adminBuilder.build();
  }

  @SuppressWarnings("rawtypes")
  private void configureConnection(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      String endpoint,
      BigtableOptions options) {
    final InstantiatingGrpcChannelProvider.Builder channelProvider =
        ((InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider()).toBuilder();

    stubSettings.setEndpoint(endpoint);

    if (options.usePlaintextNegotiation()) {
      // Make sure to avoid clobbering the old Configurator
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

    channelProvider.setPoolSize(options.getChannelCount());

    stubSettings.setTransportChannelProvider(channelProvider.build());
  }

  private void configureHeaderProvider(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableOptions options) {

    ImmutableMap.Builder<String, String> headersBuilder = ImmutableMap.<String, String>builder();
    List<String> userAgentParts = Lists.newArrayList();
    userAgentParts.add("bigtable-" + Version.VERSION);
    userAgentParts.add("jdk-" + System.getProperty("java.specification.version"));

    String customUserAgent = options.getUserAgent();
    if (customUserAgent != null) {
      userAgentParts.add(customUserAgent);
    }

    String userAgent = Joiner.on(",").join(userAgentParts);
    headersBuilder.put(GrpcUtil.USER_AGENT_KEY.name(), userAgent);

    String tracingCookie = options.getTracingCookie();
    if (tracingCookie != null) {
      headersBuilder.put("cookie", tracingCookie);
    }

    stubSettings.setHeaderProvider(FixedHeaderProvider.create(headersBuilder.build()));
  }

  @SuppressWarnings("rawtypes")
  private void configureConnectionCallTimeouts(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableIOOperationTimeouts clientTimeouts) {
    // Only patch settings when timeouts are disabled
    if (clientTimeouts.getUseTimeouts()) {
      return;
    }
    InstantiatingGrpcChannelProvider.Builder channelProvider =
        ((InstantiatingGrpcChannelProvider) stubSettings.getTransportChannelProvider()).toBuilder();

    final ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder> prevConfigurator =
        channelProvider.getChannelConfigurator();

    channelProvider.setChannelConfigurator(
        new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
          @Override
          public ManagedChannelBuilder apply(ManagedChannelBuilder managedChannelBuilder) {
            if (prevConfigurator != null) {
              managedChannelBuilder = prevConfigurator.apply(managedChannelBuilder);
            }
            return managedChannelBuilder.intercept(new NoTimeoutsInterceptor());
          }
        });
    stubSettings.setTransportChannelProvider(channelProvider.build());
  }

  private void configureBulkMutationSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      BigtableBatchingCallSettings.Builder builder,
      OperationTimeouts operationTimeouts,
      BigtableOptions options) {
    BatchingSettings.Builder batchingSettingsBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, operationTimeouts, options);
    // End configure retries & timeouts

    // Start configure flush triggers
    long autoFlushMs = options.getBulkOptions().getAutoflushMs();
    if (autoFlushMs <= 0) {
      // setDelayThreshold(null) will cause build error. Ignore autoFlushMs
      // if it's disabled and use the default value of 1 second instead until
      // we fix the implementation in gax BatcherImpl to annotate with
      // @Nonnull or check for 0 duration.
      batchingSettingsBuilder.setDelayThreshold(Duration.ofSeconds(1));
    } else {
      batchingSettingsBuilder.setDelayThreshold(Duration.ofMillis(autoFlushMs));
    }

    batchingSettingsBuilder.setElementCountThreshold(
        Long.valueOf(options.getBulkOptions().getBulkMaxRowKeyCount()));

    batchingSettingsBuilder.setRequestByteThreshold(
        Long.valueOf(options.getBulkOptions().getBulkMaxRequestSize()));
    // End configure flush triggers

    // Start configure flow control
    FlowControlSettings.Builder flowControl =
        builder.getBatchingSettings().getFlowControlSettings().toBuilder();

    // Approximate max inflight rpcs in terms of outstanding elements
    int maxInflightRpcCount = options.getBulkOptions().getMaxInflightRpcs();

    Long bulkMaxRowKeyCount = builder.getBatchingSettings().getElementCountThreshold();
    if (bulkMaxRowKeyCount == null) {
      // Using Object.requireNotNull will cause build error "incompatible argument for
      // parameter arg0 of requireNonNull"
      throw new IllegalStateException("elementCountThreshold can't be null");
    }
    Long maxInflightElements = maxInflightRpcCount * bulkMaxRowKeyCount;
    flowControl.setMaxOutstandingElementCount(maxInflightElements);

    flowControl.setMaxOutstandingRequestBytes(options.getBulkOptions().getMaxMemory());

    batchingSettingsBuilder.setFlowControlSettings(flowControl.build());
    // End configure flow control

    builder.setBatchingSettings(batchingSettingsBuilder.build());

    if (options.getBulkOptions().isEnableBulkMutationThrottling()) {
      long latencyMs = options.getBulkOptions().getBulkMutationRpcTargetMs();
      builder.enableLatencyBasedThrottling(latencyMs);
    }
  }

  private void configureBulkReadRowsSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      BigtableBulkReadRowsCallSettings.Builder builder,
      OperationTimeouts operationTimeouts,
      BigtableOptions options) {
    BatchingSettings.Builder bulkReadBatchingBuilder = builder.getBatchingSettings().toBuilder();

    // Start configure retries & timeouts
    configureRetryableCallSettings(builder, operationTimeouts, options);
    // End configure retries & timeouts

    // Start config batch settings
    long maxRowKeyCount = options.getBulkOptions().getBulkMaxRowKeyCount();
    bulkReadBatchingBuilder.setElementCountThreshold(maxRowKeyCount);
    builder.setBatchingSettings(bulkReadBatchingBuilder.build());
    // End config batch settings

    // NOTE: autoflush, flow control settings are not currently exposed
  }

  private void configureReadRowsSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      ServerStreamingCallSettings.Builder<Query, Row> readRowsSettings,
      OperationTimeouts operationTimeouts,
      BigtableOptions options) {

    // Configure retries
    // NOTE: that similar but not the same as unary retry settings: per attempt timeouts don't
    // exist, instead we use READ_PARTIAL_ROW_TIMEOUT_MS as the intra-row timeout
    if (!options.getRetryOptions().enableRetries()) {
      // user explicitly disabled retries, treat it as a non-idempotent method
      readRowsSettings.setRetryableCodes(Collections.emptySet());
    } else {
      // apply user retry settings
      readRowsSettings.setRetryableCodes(
          extractRetryCodesFromConfig(readRowsSettings.getRetryableCodes(), options));

      // Configure backoff
      long initialElapsedBackoffMs = options.getRetryOptions().getInitialBackoffMillis();
      readRowsSettings
          .retrySettings()
          .setInitialRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));

      if (initialElapsedBackoffMs
          > readRowsSettings.retrySettings().getMaxRetryDelay().toMillis()) {
        readRowsSettings
            .retrySettings()
            .setMaxRetryDelay(Duration.ofMillis(initialElapsedBackoffMs));
      }

      readRowsSettings
          .retrySettings()
          .setMaxAttempts(options.getRetryOptions().getMaxScanTimeoutRetries());
    }

    // Per response timeouts (note: gax maps rpcTimeouts to response timeouts for streaming rpcs)
    if (operationTimeouts.getResponseTimeout().isPresent()) {
      readRowsSettings
          .retrySettings()
          .setInitialRpcTimeout(operationTimeouts.getResponseTimeout().get())
          .setMaxRpcTimeout(operationTimeouts.getResponseTimeout().get());
    }

    // attempt timeout is configured in BigtableServiceImpl

    // overall timeout
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      readRowsSettings
          .retrySettings()
          .setTotalTimeout(operationTimeouts.getOperationTimeout().get());
    }
  }

  private void configureRetryableCallSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      UnaryCallSettings.Builder<?, ?> unaryCallSettings,
      OperationTimeouts operationTimeouts,
      BigtableOptions options) {

    if (!options.getRetryOptions().enableRetries()) {
      // user explicitly disabled retries, treat it as a non-idempotent method
      configureNonRetryableCallSettings(unaryCallSettings, operationTimeouts);
      return;
    }

    // apply user user retry settings
    unaryCallSettings.setRetryableCodes(
        extractRetryCodesFromConfig(unaryCallSettings.getRetryableCodes(), options));

    // Configure backoff
    long initialBackoffMs = options.getRetryOptions().getInitialBackoffMillis();
    unaryCallSettings.retrySettings().setInitialRetryDelay(Duration.ofMillis(initialBackoffMs));

    if (initialBackoffMs > unaryCallSettings.retrySettings().getMaxRetryDelay().toMillis()) {
      unaryCallSettings.retrySettings().setMaxRetryDelay(Duration.ofMillis(initialBackoffMs));
    }

    // Configure overall timeout
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      unaryCallSettings
          .retrySettings()
          .setTotalTimeout(operationTimeouts.getOperationTimeout().get());
    }

    // Configure attempt timeout - if the user hasn't explicitly configured it, then fallback to
    // overall timeout to match previous behavior
    Optional<Duration> effectiveAttemptTimeout =
        operationTimeouts.getAttemptTimeout().or(operationTimeouts.getOperationTimeout());
    if (effectiveAttemptTimeout.isPresent()) {
      unaryCallSettings.retrySettings().setInitialRpcTimeout(effectiveAttemptTimeout.get());
      unaryCallSettings.retrySettings().setMaxRpcTimeout(effectiveAttemptTimeout.get());
    }
  }

  private void configureNonRetryableCallSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      UnaryCallSettings.Builder<?, ?> unaryCallSettings,
      OperationTimeouts operationTimeouts) {
    unaryCallSettings.setRetryableCodes(Collections.<StatusCode.Code>emptySet());

    // NOTE: attempt timeouts are not configured for non-retriable rpcs
    if (operationTimeouts.getOperationTimeout().isPresent()) {
      unaryCallSettings
          .retrySettings()
          .setLogicalTimeout(operationTimeouts.getOperationTimeout().get());
    }
  }

  private BigtableIOOperationTimeouts buildCallSettings(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options) {
    boolean useTimeouts = options.getCallOptionsConfig().isUseTimeout();

    Optional<Duration> bulkMutateOverallTimeout =
        Optional.of(Duration.ofMillis(options.getCallOptionsConfig().getMutateRpcTimeoutMs()));
    OperationTimeouts bulkMutateTimeouts =
        new OperationTimeouts(
            Optional.<Duration>absent(),
            extracBulkMutateAttemptTimeout(options),
            bulkMutateOverallTimeout);

    Optional<Duration> bulkReadPartialRowTimeout =
        Optional.of(Duration.ofMillis(options.getRetryOptions().getReadPartialRowTimeoutMillis()));
    Optional<Duration> bulkReadRowsOverallTimeout =
        Optional.of(Duration.ofMillis(options.getCallOptionsConfig().getReadStreamRpcTimeoutMs()));
    OperationTimeouts bulkReadTimeouts =
        new OperationTimeouts(
            bulkReadPartialRowTimeout,
            extractBulkReadRowsAttemptTimeout(options),
            bulkReadRowsOverallTimeout);

    OperationTimeouts unaryTimeouts =
        new OperationTimeouts(
            Optional.absent(),
            extractUnaryAttemptTimeout(options),
            Optional.of(Duration.ofMillis(options.getCallOptionsConfig().getShortRpcTimeoutMs())));

    return new BigtableIOOperationTimeouts(
        useTimeouts, unaryTimeouts, bulkReadTimeouts, bulkMutateTimeouts);
  }

  private Optional<Duration> extractUnaryAttemptTimeout(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options) {

    if (!options.getCallOptionsConfig().getShortRpcAttemptTimeoutMs().isPresent()) {
      return Optional.of(DEFAULT_UNARY_ATTEMPT_TIMEOUTS);
    }
    long attemptTimeout = options.getCallOptionsConfig().getShortRpcAttemptTimeoutMs().get();
    return Optional.of(Duration.ofMillis(attemptTimeout));
  }

  private Optional<Duration> extracBulkMutateAttemptTimeout(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options) {

    if (!options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().isPresent()) {
      return Optional.of(DEFAULT_BULK_MUTATE_ATTEMPT_TIMEOUTS);
    }
    long attemptTimeout = options.getCallOptionsConfig().getMutateRpcAttemptTimeoutMs().get();
    return Optional.of(Duration.ofMillis(attemptTimeout));
  }

  private Optional<Duration> extractBulkReadRowsAttemptTimeout(
      @UnderInitialization BigtableHBaseVeneeringSettings this, BigtableOptions options) {

    if (!options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().isPresent()) {
      return Optional.absent();
    }
    long attemptTimeout = options.getCallOptionsConfig().getReadStreamRpcAttemptTimeoutMs().get();
    return Optional.of(Duration.ofMillis(attemptTimeout));
  }

  private Set<StatusCode.Code> extractRetryCodesFromConfig(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      Set<StatusCode.Code> defaultCodes,
      BigtableOptions options) {

    Set<StatusCode.Code> codes = new HashSet<>(defaultCodes);

    for (Status.Code code : options.getRetryOptions().getRetryableStatusCodes()) {
      codes.add(StatusCode.Code.valueOf(code.name()));
    }

    if (options.getRetryOptions().retryOnDeadlineExceeded()) {
      codes.add(StatusCode.Code.DEADLINE_EXCEEDED);
    }

    return codes;
  }

  private void configureCredentialProvider(
      @UnderInitialization BigtableHBaseVeneeringSettings this,
      StubSettings.Builder<?, ?> stubSettings,
      BigtableOptions options)
      throws IOException {
    CredentialOptions credentialOptions = options.getCredentialOptions();
    switch (credentialOptions.getCredentialType()) {
      case DefaultCredentials:
        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();

        if (credentials instanceof ServiceAccountCredentials) {
          stubSettings.setCredentialsProvider(
              FixedCredentialsProvider.create(
                  getJwtToken((ServiceAccountCredentials) credentials)));
        }
        return;
      case P12:
        CredentialOptions.P12CredentialOptions p12Options =
            (CredentialOptions.P12CredentialOptions) credentialOptions;
        stubSettings.setCredentialsProvider(
            FixedCredentialsProvider.create(
                getCredentialFromPrivateKeyServiceAccount(
                    p12Options.getServiceAccount(), p12Options.getKeyFile())));
        return;
      case SuppliedCredentials:
        stubSettings.setCredentialsProvider(
            FixedCredentialsProvider.create(
                ((CredentialOptions.UserSuppliedCredentialOptions) credentialOptions)
                    .getCredential()));
        return;
      case SuppliedJson:
        CredentialOptions.JsonCredentialsOptions jsonCredentialsOptions =
            (CredentialOptions.JsonCredentialsOptions) credentialOptions;
        synchronized (jsonCredentialsOptions) {
          if (jsonCredentialsOptions.getCachedCredentials() == null) {
            jsonCredentialsOptions.setCachedCredentails(
                GoogleCredentials.fromStream(jsonCredentialsOptions.getInputStream()));
          }
          stubSettings.setCredentialsProvider(
              FixedCredentialsProvider.create(jsonCredentialsOptions.getCachedCredentials()));
        }
        return;
      case None:
        stubSettings.setCredentialsProvider(NoCredentialsProvider.create());
        return;
      default:
        throw new IllegalStateException(
            "Cannot process Credential type: " + credentialOptions.getCredentialType());
    }
  }

  private static Credentials getJwtToken(ServiceAccountCredentials serviceAccount) {
    return ServiceAccountJwtAccessCredentials.newBuilder()
        .setClientEmail(serviceAccount.getClientEmail())
        .setClientId(serviceAccount.getClientId())
        .setPrivateKey(serviceAccount.getPrivateKey())
        .setPrivateKeyId(serviceAccount.getPrivateKeyId())
        .build();
  }

  public static Credentials getCredentialFromPrivateKeyServiceAccount(
      String serviceAccountEmail, String privateKeyFile) throws IOException {

    try {
      KeyStore keyStore = KeyStore.getInstance("PKCS12");

      try (FileInputStream fin = new FileInputStream(privateKeyFile)) {
        keyStore.load(fin, "notasecret".toCharArray());
      }
      PrivateKey privateKey =
          (PrivateKey) keyStore.getKey("privatekey", "notasecret".toCharArray());

      if (privateKey == null) {
        throw new IllegalStateException("private key cannot be null");
      }
      return ServiceAccountJwtAccessCredentials.newBuilder()
          .setClientEmail(serviceAccountEmail)
          .setPrivateKey(privateKey)
          .build();
    } catch (GeneralSecurityException exception) {
      throw new RuntimeException("exception while retrieving credentials", exception);
    }
  }

  static class NoTimeoutsInterceptor implements ClientInterceptor {
    static final CallOptions.Key<Boolean> SKIP_DEFAULT_ATTEMPT_TIMEOUT =
        CallOptions.Key.createWithDefault("SKIP_DEFAULT_ATTEMPT_TIMEOUT", false);

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {

      if (!callOptions.getOption(SKIP_DEFAULT_ATTEMPT_TIMEOUT)) {
        callOptions = callOptions.withDeadline(Deadline.after(6, TimeUnit.MINUTES));
      } else {
        callOptions = callOptions.withDeadline(null);
      }

      return channel.newCall(methodDescriptor, callOptions);
    }
  }

  static class BigtableIOOperationTimeouts {
    private final boolean useTimeouts;
    private final OperationTimeouts unaryTimeouts;
    private final OperationTimeouts bulkReadRowsTimeouts;
    private final OperationTimeouts bulkMutateTimeouts;

    BigtableIOOperationTimeouts(
        boolean useTimeouts,
        OperationTimeouts unaryTimeouts,
        OperationTimeouts bulkReadRowsTimeouts,
        OperationTimeouts bulkMutateTimeouts) {
      this.useTimeouts = useTimeouts;
      this.unaryTimeouts = unaryTimeouts;
      this.bulkReadRowsTimeouts = bulkReadRowsTimeouts;
      this.bulkMutateTimeouts = bulkMutateTimeouts;
    }

    boolean getUseTimeouts() {
      return useTimeouts;
    }

    OperationTimeouts getUnaryTimeouts() {
      return unaryTimeouts;
    }

    OperationTimeouts getBulkReadRowsTimeouts() {
      return bulkReadRowsTimeouts;
    }

    OperationTimeouts getBulkMutateTimeouts() {
      return bulkMutateTimeouts;
    }
  }

  static class OperationTimeouts {
    static final OperationTimeouts EMPTY =
        new OperationTimeouts(
            Optional.<Duration>absent(), Optional.<Duration>absent(), Optional.<Duration>absent());

    // responseTimeouts are only relevant to streaming RPCs, they limit the amount of timeout a
    // stream will wait for the next response message. This is synonymous with attemptTimeouts in
    // unary RPCs since they receive a single response (so its ignored).
    private final Optional<Duration> responseTimeout;
    private final Optional<Duration> attemptTimeout;
    private final Optional<Duration> operationTimeout;

    OperationTimeouts(
        Optional<Duration> responseTimeout,
        Optional<Duration> attemptTimeout,
        Optional<Duration> operationTimeout) {
      this.responseTimeout = responseTimeout;
      this.attemptTimeout = attemptTimeout;
      this.operationTimeout = operationTimeout;
    }

    Optional<Duration> getResponseTimeout() {
      return responseTimeout;
    }

    Optional<Duration> getAttemptTimeout() {
      return attemptTimeout;
    }

    Optional<Duration> getOperationTimeout() {
      return operationTimeout;
    }
  }
}

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
package org.apache.beam.sdk.io.aws2.common;

import static java.time.Duration.ofMillis;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkAsyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.internal.http.NoneTlsKeyManagersProvider;
import software.amazon.awssdk.regions.Region;

/**
 * Factory to build and configure any {@link AwsClientBuilder} using a specific {@link
 * ClientConfiguration} or the globally provided settings in {@link AwsOptions} as fallback.
 */
public interface ClientBuilderFactory {

  /**
   * Configure a client builder {@link BuilderT} using the global defaults in {@link AwsOptions}.
   */
  default <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> BuilderT create(
      BuilderT builder, @Nullable AwsOptions defaults) {
    return create(builder, ClientConfiguration.builder().build(), defaults);
  }

  /**
   * Configure a client builder {@link BuilderT} using the provided {@link ClientConfiguration} and
   * fall back to the global defaults in {@link AwsOptions} where necessary.
   */
  <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> BuilderT create(
      BuilderT builder, ClientConfiguration config, @Nullable AwsOptions defaults);

  /**
   * Check if all necessary configuration is available to create clients.
   *
   * @throws RuntimeException if configuration is incomplete.
   */
  void checkConfiguration(ClientConfiguration config, @Nullable AwsOptions defaults);

  /** The default {@link ClientBuilderFactory} instance. */
  static ClientBuilderFactory defaultFactory() {
    return DefaultClientBuilder.INSTANCE;
  }

  /**
   * Get a {@link ClientBuilderFactory} instance according to {@link
   * AwsOptions#getClientBuilderFactory()}.
   */
  static ClientBuilderFactory getFactory(AwsOptions options) {
    if (options.getClientBuilderFactory().equals(DefaultClientBuilder.class)) {
      return defaultFactory();
    }
    return InstanceBuilder.ofType(ClientBuilderFactory.class)
        .fromClass(options.getClientBuilderFactory())
        .build();
  }

  /**
   * Utility to validate if all necessary configuration is available to create clients using the
   * {@link ClientBuilderFactory} configured in {@link AwsOptions}.
   */
  static void validate(AwsOptions options, ClientConfiguration config) {
    checkArgument(options != null, "AwsOptions cannot be null");
    checkArgument(config != null, "ClientConfiguration cannot be null");
    ClientBuilderFactory.getFactory(options).checkConfiguration(config, options);
  }

  /**
   * Utility to directly build a client of type {@link ClientT} using builder of {@link BuilderT}.
   *
   * <p>The client is created using the {@link ClientBuilderFactory} configured in {@link
   * AwsOptions} with the provided {@link ClientConfiguration} and global defaults from {@link
   * AwsOptions}.
   */
  static <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> ClientT buildClient(
      AwsOptions options, BuilderT builder, ClientConfiguration config) {
    checkArgument(options != null, "AwsOptions cannot be null");
    checkArgument(builder != null, "AwsClientBuilder cannot be null");
    checkArgument(config != null, "ClientConfiguration cannot be null");
    return ClientBuilderFactory.getFactory(options).create(builder, config, options).build();
  }

  /**
   * Default implementation of {@link ClientBuilderFactory}. This implementation can configure both,
   * synchronous clients using {@link ApacheHttpClient} as well as asynchronous clients using {@link
   * NettyNioAsyncHttpClient}.
   */
  class DefaultClientBuilder implements ClientBuilderFactory, Serializable {
    private static final ClientBuilderFactory INSTANCE = new DefaultClientBuilder();

    DefaultClientBuilder() {}

    @Override
    public void checkConfiguration(ClientConfiguration config, @Nullable AwsOptions options) {
      Region region = valueOrElse(config.region(), options, AwsOptions::getAwsRegion);
      AwsCredentialsProvider credentials =
          valueOrElse(config.credentialsProvider(), options, AwsOptions::getAwsCredentialsProvider);
      checkState(region != null, "No AWS region available");
      checkState(
          credentials != null && credentials.resolveCredentials() != null,
          "No AWS credentials provider available");
    }

    @Override
    public <BuilderT extends AwsClientBuilder<BuilderT, ClientT>, ClientT> BuilderT create(
        BuilderT builder, ClientConfiguration config, @Nullable AwsOptions options) {
      checkNotNull(config, "ClientConfiguration cannot be null!");

      Region region = valueOrElse(config.region(), options, AwsOptions::getAwsRegion);
      AwsCredentialsProvider credentials =
          valueOrElse(config.credentialsProvider(), options, AwsOptions::getAwsCredentialsProvider);
      URI endpoint = valueOrElse(config.endpoint(), options, AwsOptions::getEndpoint);

      setOptional(credentials, builder::credentialsProvider);
      setOptional(region, builder::region);
      setOptional(endpoint, builder::endpointOverride);

      if (config.retry() != null) {
        RetryPolicy retryPolicy = config.retry().toClientRetryPolicy();
        builder.overrideConfiguration(
            ClientOverrideConfiguration.builder().retryPolicy(retryPolicy).build());
      }

      // remaining configuration can only be set via AwsOptions
      if (options == null) {
        return builder;
      }

      HttpClientConfiguration httpConfig = options.getHttpClientConfiguration();
      ProxyConfiguration proxyConfig = options.getProxyConfiguration();
      boolean skipCertificateVerification = false;
      if (config.skipCertificateVerification() != null) {
        skipCertificateVerification = config.skipCertificateVerification();
      }
      if (proxyConfig != null || httpConfig != null || skipCertificateVerification) {
        if (builder instanceof SdkSyncClientBuilder) {
          ApacheHttpClient.Builder client = syncClientBuilder();

          setOptional(proxyConfig, client::proxyConfiguration);

          if (httpConfig != null) {
            setOptional(
                httpConfig.connectionAcquisitionTimeout(), client::connectionAcquisitionTimeout);
            setOptional(httpConfig.connectionMaxIdleTime(), client::connectionMaxIdleTime);
            setOptional(httpConfig.connectionTimeout(), client::connectionTimeout);
            setOptional(httpConfig.connectionTimeToLive(), client::connectionTimeToLive);
            setOptional(httpConfig.socketTimeout(), client::socketTimeout);
            setOptional(httpConfig.maxConnections(), client::maxConnections);
          }

          if (skipCertificateVerification) {
            client.tlsKeyManagersProvider(NoneTlsKeyManagersProvider.getInstance());
            throw new RuntimeException(
                "Made it this far - probably means the tlsKeyManagersProvider is not right");
          }

          // must use builder to make sure client is managed by the SDK
          ((SdkSyncClientBuilder<?, ?>) builder).httpClientBuilder(client);
        } else if (builder instanceof SdkAsyncClientBuilder) {
          NettyNioAsyncHttpClient.Builder client = asyncClientBuilder();

          if (proxyConfig != null) {
            client.proxyConfiguration(nettyProxyConfig(proxyConfig));
          }

          if (httpConfig != null) {
            setOptional(
                httpConfig.connectionAcquisitionTimeout(), client::connectionAcquisitionTimeout);
            setOptional(httpConfig.connectionMaxIdleTime(), client::connectionMaxIdleTime);
            setOptional(httpConfig.connectionTimeout(), client::connectionTimeout);
            setOptional(httpConfig.connectionTimeToLive(), client::connectionTimeToLive);
            setOptional(
                httpConfig.socketTimeout(), client::readTimeout); // fallback for readTimeout
            setOptional(httpConfig.readTimeout(), client::readTimeout);
            setOptional(
                httpConfig.socketTimeout(), client::writeTimeout); // fallback for writeTimeout
            setOptional(httpConfig.writeTimeout(), client::writeTimeout);
            setOptional(httpConfig.maxConnections(), client::maxConcurrency);
          }

          if (skipCertificateVerification) {
            client.tlsKeyManagersProvider(NoneTlsKeyManagersProvider.getInstance());
            throw new RuntimeException(
                "Made it this far - probably means the tlsKeyManagersProvider is not right");
          }

          // must use builder to make sure client is managed by the SDK
          ((SdkAsyncClientBuilder<?, ?>) builder).httpClientBuilder(client);
        }
      }
      return builder;
    }

    @VisibleForTesting
    ApacheHttpClient.Builder syncClientBuilder() {
      return ApacheHttpClient.builder();
    }

    @VisibleForTesting
    NettyNioAsyncHttpClient.Builder asyncClientBuilder() {
      return NettyNioAsyncHttpClient.builder();
    }

    private software.amazon.awssdk.http.nio.netty.ProxyConfiguration nettyProxyConfig(
        ProxyConfiguration proxyConfig) {
      return software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
          .host(proxyConfig.host())
          .port(proxyConfig.port())
          .scheme(proxyConfig.scheme())
          .username(proxyConfig.username())
          .password(proxyConfig.password())
          .nonProxyHosts(proxyConfig.nonProxyHosts())
          .build();
    }

    @Nullable
    private <T, X> T valueOrElse(@Nullable T value, @Nullable X other, Function<X, T> transform) {
      return value != null ? value : (other != null ? transform.apply(other) : null);
    }

    private <T> void setOptional(@Nullable T t, Consumer<T> setter) {
      if (t != null) {
        setter.accept(t);
      }
    }

    private void setOptional(@Nullable Integer millis, Consumer<Duration> setter) {
      if (millis != null) {
        setter.accept(ofMillis(millis));
      }
    }
  }
}

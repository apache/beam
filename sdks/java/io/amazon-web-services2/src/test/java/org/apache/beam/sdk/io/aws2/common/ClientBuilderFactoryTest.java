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
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.buildClient;
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.defaultFactory;
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.getFactory;
import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.validate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static software.amazon.awssdk.regions.Region.EU_WEST_2;
import static software.amazon.awssdk.regions.Region.US_WEST_1;

import java.net.URI;
import java.util.function.Consumer;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.DefaultClientBuilder;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkAsyncClientBuilder;
import software.amazon.awssdk.core.client.builder.SdkSyncClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

@RunWith(MockitoJUnitRunner.Silent.class)
public class ClientBuilderFactoryTest {
  @Mock AwsBuilder builder;
  @Mock AwsOptions awsOptions;

  @Before
  public void prepareOptions() {
    when(awsOptions.getClientBuilderFactory()).thenReturn((Class) TestClientBuilderFactory.class);
    when(awsOptions.getAwsRegion()).thenReturn(EU_WEST_2);
    when(awsOptions.getAwsCredentialsProvider()).thenReturn(mock(AwsCredentialsProvider.class));
  }

  @Test
  public void testGetFactoryInstance() {
    assertThat(getFactory(awsOptions)).isInstanceOf(TestClientBuilderFactory.class);
    when(awsOptions.getClientBuilderFactory()).thenReturn((Class) DefaultClientBuilder.class);
    assertThat(getFactory(awsOptions)).isSameAs(defaultFactory());
  }

  @Test
  public void testValidate() {
    AwsCredentialsProvider mock = mock(AwsCredentialsProvider.class);
    when(awsOptions.getAwsCredentialsProvider()).thenReturn(mock);
    when(mock.resolveCredentials()).thenReturn(mock(AwsCredentials.class));

    validate(awsOptions, ClientConfiguration.builder().build()); // finally success
  }

  @Test
  public void testCheckConfigurationUsingOptions() {
    ClientConfiguration config = ClientConfiguration.builder().build();
    when(awsOptions.getAwsRegion()).thenReturn(null);
    assertThatThrownBy(() -> defaultFactory().checkConfiguration(config, awsOptions))
        .hasMessage("No AWS region available");

    when(awsOptions.getAwsRegion()).thenReturn(EU_WEST_2);
    when(awsOptions.getAwsCredentialsProvider()).thenReturn(null);
    assertThatThrownBy(() -> defaultFactory().checkConfiguration(config, awsOptions))
        .hasMessage("No AWS credentials provider available");

    AwsCredentialsProvider mock = mock(AwsCredentialsProvider.class);
    when(awsOptions.getAwsCredentialsProvider()).thenReturn(mock);
    when(mock.resolveCredentials()).thenReturn(mock(AwsCredentials.class));
    defaultFactory().checkConfiguration(config, awsOptions); // finally success

    when(mock.resolveCredentials()).thenThrow(new RuntimeException("Error resolving credentials"));
    assertThatThrownBy(() -> defaultFactory().checkConfiguration(config, awsOptions))
        .hasMessage("Error resolving credentials");
  }

  @Test
  public void testCheckConfigurationUsingConfig() {
    when(awsOptions.getAwsRegion()).thenReturn(null);
    when(awsOptions.getAwsCredentialsProvider()).thenReturn(null);

    ClientConfiguration noRegion = ClientConfiguration.builder().build();
    assertThatThrownBy(() -> defaultFactory().checkConfiguration(noRegion, awsOptions))
        .hasMessage("No AWS region available");

    ClientConfiguration noCredentials = ClientConfiguration.builder().region(US_WEST_1).build();
    assertThatThrownBy(() -> defaultFactory().checkConfiguration(noCredentials, awsOptions))
        .hasMessage("No AWS credentials provider available");

    AwsBasicCredentials credentials = AwsBasicCredentials.create("key", "secret");
    ClientConfiguration valid =
        ClientConfiguration.builder()
            .region(US_WEST_1)
            .credentialsProvider(StaticCredentialsProvider.create(credentials))
            .build();

    defaultFactory().checkConfiguration(valid, awsOptions); // finally success
    defaultFactory().checkConfiguration(valid, null); // awsOptions may be null
  }

  @Test
  public void testBaseSettingsFromClientConfig() {
    ClientConfiguration config =
        ClientConfiguration.builder()
            .region(US_WEST_1)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    getFactory(awsOptions).create(builder, config, null);

    verify(builder).region(config.region());
    verify(builder).credentialsProvider(config.credentialsProvider());
    verifyNoMoreInteractions(builder);
    clearInvocations(builder);

    getFactory(awsOptions).create(builder, config, awsOptions);

    verify(builder).region(config.region());
    verify(builder).credentialsProvider(config.credentialsProvider());
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testBuildClientFromOptions() {
    ClientConfiguration config = ClientConfiguration.builder().build();

    buildClient(awsOptions, builder, config);

    verify(builder).region(awsOptions.getAwsRegion());
    verify(builder).credentialsProvider(awsOptions.getAwsCredentialsProvider());
    verify(builder).build();
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testBaseSettingsFromOptions() {
    ClientConfiguration config = ClientConfiguration.builder().build();

    getFactory(awsOptions).create(builder, config, awsOptions);

    verify(builder).region(awsOptions.getAwsRegion());
    verify(builder).credentialsProvider(awsOptions.getAwsCredentialsProvider());
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testEndpointFromClientConfig() {
    ClientConfiguration config =
        ClientConfiguration.builder().endpoint(URI.create("https://localhost")).build();

    getFactory(awsOptions).create(builder, config, awsOptions);

    verify(builder).endpointOverride(config.endpoint());
    verify(builder).region(awsOptions.getAwsRegion());
    verify(builder).credentialsProvider(awsOptions.getAwsCredentialsProvider());
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testEndpointFromOptions() {
    ClientConfiguration config = ClientConfiguration.builder().build();

    when(awsOptions.getEndpoint()).thenReturn(URI.create("https://options"));

    getFactory(awsOptions).create(builder, config, awsOptions);

    verify(builder).endpointOverride(awsOptions.getEndpoint());
    verify(builder).region(awsOptions.getAwsRegion());
    verify(builder).credentialsProvider(awsOptions.getAwsCredentialsProvider());
    verifyNoMoreInteractions(builder);
  }

  @Test
  public void testRetryConfiguration() {
    ClientConfiguration config = ClientConfiguration.builder().retry(b -> b.numRetries(3)).build();

    getFactory(awsOptions).create(builder, config, awsOptions);

    ArgumentCaptor<ClientOverrideConfiguration> overrideConfig =
        ArgumentCaptor.forClass(ClientOverrideConfiguration.class);

    verify(builder).overrideConfiguration(overrideConfig.capture());
    verify(builder).region(awsOptions.getAwsRegion());
    verify(builder).credentialsProvider(awsOptions.getAwsCredentialsProvider());
    verifyNoMoreInteractions(builder);

    assertThat(overrideConfig.getValue().retryPolicy())
        .contains(config.retry().toClientRetryPolicy());
  }

  @Test
  public void testSyncProxyConfiguration() {
    ProxyConfiguration proxy =
        ProxyConfiguration.builder()
            .endpoint(URI.create("http://localhost:7777"))
            .username("user")
            .password("secret")
            .build();

    builder = mock(AwsSyncBuilder.class);
    when(awsOptions.getProxyConfiguration()).thenReturn(proxy);

    getFactory(awsOptions).create(builder, awsOptions);

    ArgumentCaptor<ApacheHttpClient.Builder> httpClientBuilder =
        ArgumentCaptor.forClass(ApacheHttpClient.Builder.class);

    verify((AwsSyncBuilder) builder).httpClientBuilder(httpClientBuilder.capture());

    verify(httpClientBuilder.getValue()).proxyConfiguration(proxy);
    verifyNoMoreInteractions(httpClientBuilder.getValue());
  }

  @Test
  public void testAsyncProxyConfiguration() {
    ProxyConfiguration proxy =
        ProxyConfiguration.builder()
            .endpoint(URI.create("http://localhost:7777"))
            .username("user")
            .password("secret")
            .build();

    software.amazon.awssdk.http.nio.netty.ProxyConfiguration nettyProxy =
        software.amazon.awssdk.http.nio.netty.ProxyConfiguration.builder()
            .scheme(proxy.scheme())
            .host(proxy.host())
            .port(proxy.port())
            .username(proxy.username())
            .password(proxy.password())
            .nonProxyHosts(proxy.nonProxyHosts())
            .build();

    builder = mock(AwsAsyncBuilder.class);
    when(awsOptions.getProxyConfiguration()).thenReturn(proxy);

    getFactory(awsOptions).create(builder, awsOptions);

    ArgumentCaptor<NettyNioAsyncHttpClient.Builder> httpClientBuilder =
        ArgumentCaptor.forClass(NettyNioAsyncHttpClient.Builder.class);

    verify((AwsAsyncBuilder) builder).httpClientBuilder(httpClientBuilder.capture());

    verify(httpClientBuilder.getValue()).proxyConfiguration(nettyProxy);
    verifyNoMoreInteractions(httpClientBuilder.getValue());
  }

  @Test
  public void testSyncHttpConfiguration() {
    testSyncHttpConfiguration(
        HttpClientConfiguration.builder()
            .connectionAcquisitionTimeout(100)
            .connectionMaxIdleTime(200)
            .connectionTimeout(300)
            .connectionTimeToLive(400)
            .socketTimeout(500)
            .maxConnections(10)
            .build(),
        builder -> {
          verify(builder).connectionAcquisitionTimeout(ofMillis(100));
          verify(builder).connectionMaxIdleTime(ofMillis(200));
          verify(builder).connectionTimeout(ofMillis(300));
          verify(builder).connectionTimeToLive(ofMillis(400));
          verify(builder).socketTimeout(ofMillis(500));
          verify(builder).maxConnections(10);
        });
  }

  @Test
  public void testSyncEmptyHttpConfiguration() {
    testSyncHttpConfiguration(HttpClientConfiguration.builder().build(), builder -> {});
  }

  public void testSyncHttpConfiguration(
      HttpClientConfiguration conf, Consumer<ApacheHttpClient.Builder> verification) {
    builder = mock(AwsSyncBuilder.class);
    when(awsOptions.getHttpClientConfiguration()).thenReturn(conf);

    getFactory(awsOptions).create(builder, awsOptions);

    ArgumentCaptor<ApacheHttpClient.Builder> httpClientBuilder =
        ArgumentCaptor.forClass(ApacheHttpClient.Builder.class);

    verify((AwsSyncBuilder) builder).httpClientBuilder(httpClientBuilder.capture());
    verification.accept(httpClientBuilder.getValue());
    verifyNoMoreInteractions(httpClientBuilder.getValue());
  }

  @Test
  public void testAsyncHttpConfiguration() {
    testAsyncHttpConfiguration(
        HttpClientConfiguration.builder()
            .connectionAcquisitionTimeout(100)
            .connectionMaxIdleTime(200)
            .connectionTimeout(300)
            .connectionTimeToLive(400)
            .socketTimeout(400) // overwritten
            .readTimeout(500)
            .writeTimeout(600)
            .maxConnections(10)
            .build(),
        builder -> {
          verify(builder).maxConcurrency(10);
          verify(builder).connectionAcquisitionTimeout(ofMillis(100));
          verify(builder).connectionMaxIdleTime(ofMillis(200));
          verify(builder).connectionTimeout(ofMillis(300));
          verify(builder).connectionTimeToLive(ofMillis(400));
          InOrder ordered = inOrder(builder);
          ordered.verify(builder).readTimeout(ofMillis(400)); // overwritten
          ordered.verify(builder).readTimeout(ofMillis(500));
          ordered = inOrder(builder);
          ordered.verify(builder).writeTimeout(ofMillis(400)); // overwritten
          ordered.verify(builder).writeTimeout(ofMillis(600));
        });
  }

  @Test
  public void testAsyncHttpSocketConfiguration() {
    testAsyncHttpConfiguration(
        HttpClientConfiguration.builder().socketTimeout(400).build(),
        builder -> {
          verify(builder).readTimeout(ofMillis(400));
          verify(builder).writeTimeout(ofMillis(400));
        });
  }

  @Test
  public void testAsyncEmptyHttpConfiguration() {
    testAsyncHttpConfiguration(HttpClientConfiguration.builder().build(), builder -> {});
  }

  public void testAsyncHttpConfiguration(
      HttpClientConfiguration conf, Consumer<NettyNioAsyncHttpClient.Builder> verification) {
    builder = mock(AwsAsyncBuilder.class);
    when(awsOptions.getHttpClientConfiguration()).thenReturn(conf);

    getFactory(awsOptions).create(builder, awsOptions);

    ArgumentCaptor<NettyNioAsyncHttpClient.Builder> httpClientBuilder =
        ArgumentCaptor.forClass(NettyNioAsyncHttpClient.Builder.class);

    verify((AwsAsyncBuilder) builder).httpClientBuilder(httpClientBuilder.capture());
    verification.accept(httpClientBuilder.getValue());
    verifyNoMoreInteractions(httpClientBuilder.getValue());
  }

  private abstract static class AwsBuilder implements AwsClientBuilder<AwsBuilder, Void> {}

  private abstract static class AwsSyncBuilder extends AwsBuilder
      implements SdkSyncClientBuilder<AwsSyncBuilder, Void> {}

  private abstract static class AwsAsyncBuilder extends AwsBuilder
      implements SdkAsyncClientBuilder<AwsAsyncBuilder, Void> {}

  private static class TestClientBuilderFactory extends DefaultClientBuilder {
    @Override
    ApacheHttpClient.Builder syncClientBuilder() {
      return mock(ApacheHttpClient.Builder.class);
    }

    @Override
    NettyNioAsyncHttpClient.Builder asyncClientBuilder() {
      return mock(NettyNioAsyncHttpClient.Builder.class);
    }
  }
}

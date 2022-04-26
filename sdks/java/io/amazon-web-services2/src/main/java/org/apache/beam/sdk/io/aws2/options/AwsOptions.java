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
package org.apache.beam.sdk.io.aws2.options;

import java.net.URI;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.common.HttpClientConfiguration;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ContainerCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

/**
 * Options used to configure Amazon Web Services specific options such as credentials and region.
 */
@Experimental(Kind.SOURCE_SINK)
public interface AwsOptions extends PipelineOptions {

  /** Region used to configure AWS service clients. */
  @Description("Region used by AWS service clients")
  @Default.InstanceFactory(AwsRegionFactory.class)
  Region getAwsRegion();

  void setAwsRegion(Region region);

  /** Attempt to load default region. */
  class AwsRegionFactory implements DefaultValueFactory<@Nullable Region> {
    @Override
    @Nullable
    public Region create(PipelineOptions options) {
      try {
        return new DefaultAwsRegionProviderChain().getRegion();
      } catch (SdkClientException e) {
        return null;
      }
    }
  }

  /** Endpoint used to configure AWS service clients. */
  @Description("Endpoint used by AWS service clients")
  URI getEndpoint();

  void setEndpoint(URI uri);

  /**
   * {@link AwsCredentialsProvider} used to configure AWS service clients.
   *
   * <p>The class name of the provider must be set in the {@code @type} field. Note: Not all
   * available providers are supported and some configuration options might be ignored.
   *
   * <p>Most providers rely on system's environment to follow AWS conventions, there's no further
   * configuration:
   * <li>{@link DefaultCredentialsProvider}
   * <li>{@link EnvironmentVariableCredentialsProvider}
   * <li>{@link SystemPropertyCredentialsProvider}
   * <li>{@link ProfileCredentialsProvider}
   * <li>{@link ContainerCredentialsProvider}
   *
   *     <p>Example:
   *
   *     <pre>{@code --awsCredentialsProvider={"@type": "ProfileCredentialsProvider"}}</pre>
   *
   *     <p>Some other providers require additional configuration:
   * <li>{@link StaticCredentialsProvider}
   * <li>{@link StsAssumeRoleCredentialsProvider}
   *
   *     <p>Examples:
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "StaticCredentialsProvider",
   *   "awsAccessKeyId": "key_id_value",
   *   "awsSecretKey": "secret_value"
   * }
   *
   * --awsCredentialsProvider={
   *   "@type": "StaticCredentialsProvider",
   *   "awsAccessKeyId": "key_id_value",
   *   "awsSecretKey": "secret_value",
   *   "sessionToken": "token_value"
   * }
   *
   * --awsCredentialsProvider={
   *   "@type": "StsAssumeRoleCredentialsProvider",
   *   "roleArn": "role_arn_Value",
   *   "roleSessionName": "session_name_value",
   *   "policy": "policy_value",
   *   "durationSeconds": 3600
   * }}</pre>
   *
   * @see DefaultCredentialsProvider
   */
  @Description(
      "The credentials provider used to authenticate against AWS services. "
          + "The provider class must be specified in the \"@type\" field, check the Javadocs for further examples. "
          + "Example: {\"@type\": \"StaticCredentialsProvider\", \"accessKeyId\":\"<key>\", \"secretAccessKey\":\"<secret>\"}")
  @Default.InstanceFactory(AwsUserCredentialsFactory.class)
  AwsCredentialsProvider getAwsCredentialsProvider();

  void setAwsCredentialsProvider(AwsCredentialsProvider value);

  /** Return {@link DefaultCredentialsProvider} as default provider. */
  class AwsUserCredentialsFactory implements DefaultValueFactory<AwsCredentialsProvider> {
    @Override
    public AwsCredentialsProvider create(PipelineOptions options) {
      return DefaultCredentialsProvider.create();
    }
  }

  /**
   * {@link ProxyConfiguration} used to configure AWS service clients.
   *
   * <p>Note, only the options shown in the example below are supported. <code>username</code> and
   * <code>password</code> are optional.
   *
   * <p>Example:
   *
   * <pre>{@code --proxyConfiguration={
   *   "endpoint": "http://hostname:port",
   *   "username": "username",
   *   "password": "password"
   * }}</pre>
   */
  @Description(
      "The proxy configuration used to configure AWS service clients. Example: "
          + "--proxyConfiguration={\"endpoint\":\"http://hostname:port\", \"username\":\"username\", \"password\":\"password\"}")
  ProxyConfiguration getProxyConfiguration();

  void setProxyConfiguration(ProxyConfiguration value);

  /**
   * {@link HttpClientConfiguration} used to configure AWS service clients.
   *
   * <p>Example:
   *
   * <pre>{@code --httpClientConfiguration={"socketTimeout":1000, "maxConnections":10}}</pre>
   */
  @Description(
      "The HTTP client configuration used to configure AWS service clients. Example: "
          + "--httpClientConfiguration={\"socketTimeout\":1000,\"maxConnections\":10}")
  HttpClientConfiguration getHttpClientConfiguration();

  void setHttpClientConfiguration(HttpClientConfiguration value);

  @Description("Factory class to configure AWS client builders")
  @Default.Class(ClientBuilderFactory.DefaultClientBuilder.class)
  Class<? extends ClientBuilderFactory> getClientBuilderFactory();

  void setClientBuilderFactory(Class<? extends ClientBuilderFactory> clazz);
}

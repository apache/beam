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
import org.apache.beam.sdk.io.aws2.auth.SecretCredentialsProvider;
import org.apache.beam.sdk.io.aws2.auth.StsAssumeRoleForFederatedCredentialsProvider;
import org.apache.beam.sdk.io.aws2.auth.WebIdTokenProvider;
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
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;

/**
 * Options used to configure Amazon Web Services specific options such as credentials and region.
 *
 * <p>The options {@code --awsRegionName}, {@code --awsEndpointUrl}, {@code --awsAccessKeyId},
 * {@code --awsSecretAccessKey}, {@code --awsSessionToken} and {@code --awsCredentialsSecretManager}
 * have the same names as the {@code --aws_*} options of the Python SDK, so they are shared in
 * cross-language pipelines.
 */
public interface AwsOptions extends PipelineOptions {

  /**
   * Region used to configure AWS service clients.
   *
   * <p>If not set, defaults to {@link #getAwsRegionName()} or else the region from the AWS default
   * region provider chain.
   *
   * @deprecated Use {@link #getAwsRegionName()} ({@code --awsRegionName}) instead. Use {@link
   *     AwsOptionsResolver#region(AwsOptions)} to get the effective region.
   */
  @Deprecated
  @Description("Deprecated, use --awsRegionName instead. Region used by AWS service clients")
  @Default.InstanceFactory(AwsRegionFactory.class)
  Region getAwsRegion();

  /**
   * @deprecated Use {@link #setAwsRegionName(String)} instead.
   */
  @Deprecated
  void setAwsRegion(Region region);

  /**
   * Name of the region used to configure AWS service clients, e.g. {@code us-east-1}.
   *
   * <p>If not set, the region from the AWS default region provider chain is used.
   */
  @Description(
      "The name of the region used by AWS service clients, e.g. us-east-1. If not set, the AWS "
          + "default region provider chain is used.")
  @Nullable String getAwsRegionName();

  void setAwsRegionName(String regionName);

  /** Attempt to load default region. */
  class AwsRegionFactory implements DefaultValueFactory<@Nullable Region> {
    @Override
    @Nullable
    public Region create(PipelineOptions options) {
      String regionName = options.as(AwsOptions.class).getAwsRegionName();
      if (regionName != null && !regionName.trim().isEmpty()) {
        return Region.of(regionName.trim());
      }
      try {
        return new DefaultAwsRegionProviderChain().getRegion();
      } catch (SdkClientException e) {
        return null;
      }
    }
  }

  /**
   * Endpoint used to configure AWS service clients.
   *
   * <p>If not set, defaults to {@link #getAwsEndpointUrl()}.
   *
   * @deprecated Use {@link #getAwsEndpointUrl()} ({@code --awsEndpointUrl}) instead. Use {@link
   *     AwsOptionsResolver#endpoint(AwsOptions)} to get the effective endpoint.
   */
  @Deprecated
  @Description("Deprecated, use --awsEndpointUrl instead. Endpoint used by AWS service clients")
  @Default.InstanceFactory(EndpointFactory.class)
  URI getEndpoint();

  /**
   * @deprecated Use {@link #setAwsEndpointUrl(String)} instead.
   */
  @Deprecated
  void setEndpoint(URI uri);

  /** The complete URL used by AWS service clients, e.g. {@code https://localhost:4566}. */
  @Description("The complete URL used by AWS service clients, e.g. https://localhost:4566.")
  @Nullable String getAwsEndpointUrl();

  void setAwsEndpointUrl(String endpointUrl);

  /** Returns {@link #getAwsEndpointUrl()} as a {@link URI}, if set. */
  class EndpointFactory implements DefaultValueFactory<@Nullable URI> {
    @Override
    @Nullable
    public URI create(PipelineOptions options) {
      return AwsOptionsResolver.parseEndpointUrl(options.as(AwsOptions.class).getAwsEndpointUrl());
    }
  }

  /**
   * The AWS access key ID used to configure AWS service clients.
   *
   * <p>Must be set together with {@link #getAwsSecretAccessKey()}. If {@link
   * #getAwsCredentialsSecretManager()} is set, this is a secret spec parsed by that secret manager
   * (see {@link SecretCredentialsProvider}). Otherwise, it is the raw access key ID.
   *
   * <p>Cannot be combined with an explicitly set {@link #getAwsCredentialsProvider()}.
   */
  @Description(
      "The access key ID used by AWS service clients. If --awsCredentialsSecretManager is set, "
          + "this is a secret spec parsed by that secret manager. Cannot be combined with "
          + "--awsCredentialsProvider.")
  @Nullable String getAwsAccessKeyId();

  void setAwsAccessKeyId(String value);

  /**
   * The AWS secret access key used to configure AWS service clients.
   *
   * <p>See {@link #getAwsAccessKeyId()}.
   */
  @Description(
      "The secret access key used by AWS service clients. If --awsCredentialsSecretManager is set, "
          + "this is a secret spec parsed by that secret manager. Cannot be combined with "
          + "--awsCredentialsProvider.")
  @Nullable String getAwsSecretAccessKey();

  void setAwsSecretAccessKey(String value);

  /**
   * The optional AWS session token used to configure AWS service clients.
   *
   * <p>See {@link #getAwsAccessKeyId()}.
   */
  @Description(
      "The session token used by AWS service clients. If --awsCredentialsSecretManager is set, "
          + "this is a secret spec parsed by that secret manager. Cannot be combined with "
          + "--awsCredentialsProvider.")
  @Nullable String getAwsSessionToken();

  void setAwsSessionToken(String value);

  /**
   * The secret manager used to parse {@link #getAwsAccessKeyId()}, {@link #getAwsSecretAccessKey()}
   * and {@link #getAwsSessionToken()} as secret specs, e.g. {@code GoogleCloudSecretManager}.
   *
   * <p>Example:
   *
   * <pre>{@code --awsCredentialsSecretManager=GoogleCloudSecretManager
   * --awsAccessKeyId='{"name": "my-aws-key-id", "project": "my-project"}'
   * --awsSecretAccessKey='{"name": "my-aws-secret-key", "project": "my-project"}'
   * }</pre>
   *
   * <p>The credentials are fetched when they are first needed, typically on the workers, so only
   * the secret specs are part of the pipeline options. AWS Secrets Manager cannot be used here,
   * because the AWS credentials are needed to access it.
   */
  @Description(
      "Secret manager used to parse --awsAccessKeyId, --awsSecretAccessKey and --awsSessionToken "
          + "as secret specs (a JSON string or plain text), e.g. GoogleCloudSecretManager. For "
          + "example: --awsCredentialsSecretManager=GoogleCloudSecretManager "
          + "--awsSecretAccessKey='{\"name\": \"my-key\", \"project\": \"p\"}'. If not set, these "
          + "options are used as raw credentials.")
  @Nullable String getAwsCredentialsSecretManager();

  void setAwsCredentialsSecretManager(String value);

  /**
   * {@link AwsCredentialsProvider} used to configure AWS service clients.
   *
   * <p>The class name of the provider must be set in the {@code @type} field. Note: Not all
   * available providers are supported and some configuration options might be ignored.
   *
   * <p>Most providers must use the system environment following AWS conventions. Programmatic
   * configuration for these providers is NOT supported:
   * <li>{@link DefaultCredentialsProvider}
   * <li>{@link EnvironmentVariableCredentialsProvider}
   * <li>{@link SystemPropertyCredentialsProvider}
   * <li>{@link ContainerCredentialsProvider}
   *
   *     <p>Example:
   *
   *     <pre>{@code --awsCredentialsProvider={"@type": "EnvironmentVariableCredentialsProvider"}}
   *     </pre>
   *
   *     <p>Some other providers support additional configuration:
   * <li>{@link StaticCredentialsProvider}
   *
   *     <p>Examples:
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "StaticCredentialsProvider",
   *   "accessKeyId": "key_id_value",
   *   "secretAccessKey": "secret_value"
   * }
   *
   * --awsCredentialsProvider={
   *   "@type": "StaticCredentialsProvider",
   *   "accessKeyId": "key_id_value",
   *   "secretAccessKey": "secret_value",
   *   "sessionToken": "token_value"
   * }}</pre>
   *
   * <li>{@link ProfileCredentialsProvider}
   *
   *     <p>{@code profileName} is optional, if not set the environment default is used. Be careful
   *     if using this provider programmatically, it can behave unexpectedly.
   *
   *     <p>Examples:
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "ProfileCredentialsProvider"
   * }
   *
   * --awsCredentialsProvider={
   *   "@type": "ProfileCredentialsProvider",
   *   "profileName": "my_profile"
   * }}</pre>
   *
   * <li>{@link StsAssumeRoleCredentialsProvider}
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "StsAssumeRoleCredentialsProvider",
   *   "roleArn": "role_arn_Value",
   *   "roleSessionName": "session_name_value",
   *   "policy": "policy_value",
   *   "durationSeconds": 3600
   * }}</pre>
   *
   * <li>{@link StsAssumeRoleWithWebIdentityCredentialsProvider}
   *
   *     <p>Please note that this works for batch pipelines which can be completed within the
   *     expiration of the web identity token. Long batch or streaming pipelines wouldn't work with
   *     this Provider.
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "StsAssumeRoleWithWebIdentityCredentialsProvider",
   *   "roleArn": "role_arn_Value",
   *   "roleSessionName": "session_name_value",
   *   "webIdentityToken": "web_identity_token_value",
   *   "durationSeconds": 3600
   * }}</pre>
   *
   * <li>{@link StsAssumeRoleForFederatedCredentialsProvider}
   *
   *     <p>Similar to {@link StsAssumeRoleWithWebIdentityCredentialsProvider} but supporting
   *     long-running jobs. The configuration expects the FQCN for an implementation of {@link
   *     WebIdTokenProvider} which will be used to retrieve a fresh token with each STS auth
   *     request.
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "StsAssumeRoleWithDynamicWebIdentityCredentialsProvider",
   *   "roleArn": "role_arn_Value",
   *   "audience": "audience_value",
   *   "webIdTokenProviderFQCN": "fully_qualified_class_name",
   *   "durationSeconds": 3600
   * }}</pre>
   *
   * <li>{@link SecretCredentialsProvider}
   *
   *     <p>Resolves static credentials through a secret manager. Only the secret specs are
   *     serialized. Usually configured with {@link #getAwsCredentialsSecretManager()} instead.
   *
   *     <pre>{@code --awsCredentialsProvider={
   *   "@type": "SecretCredentialsProvider",
   *   "secretManager": "GoogleCloudSecretManager",
   *   "accessKeyId": {"name": "my-aws-key-id", "project": "my-project"},
   *   "secretAccessKey": {"name": "my-aws-secret-key", "project": "my-project"}
   * }}</pre>
   *
   *     <p>If not set, the provider is derived from {@link #getAwsAccessKeyId()}, {@link
   *     #getAwsSecretAccessKey()}, {@link #getAwsSessionToken()} and {@link
   *     #getAwsCredentialsSecretManager()} if they are set, and is {@link
   *     DefaultCredentialsProvider} otherwise. Setting both this option and those options is an
   *     error.
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

  /**
   * Returns the provider derived from {@link #getAwsAccessKeyId()} and related options if they are
   * set, and {@link DefaultCredentialsProvider} otherwise.
   */
  class AwsUserCredentialsFactory implements DefaultValueFactory<AwsCredentialsProvider> {
    @Override
    public AwsCredentialsProvider create(PipelineOptions options) {
      AwsCredentialsProvider provider =
          AwsOptionsResolver.credentialsProviderFromKeys(options.as(AwsOptions.class));
      return provider != null ? provider : DefaultCredentialsProvider.create();
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

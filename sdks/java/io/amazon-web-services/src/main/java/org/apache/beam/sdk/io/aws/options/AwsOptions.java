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
package org.apache.beam.sdk.io.aws.options;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/**
 * Options used to configure Amazon Web Services specific options such as credentials and region.
 */
public interface AwsOptions extends PipelineOptions {

  /** AWS region used by the AWS client. */
  @Description("AWS region used by the AWS client")
  @Validation.Required
  String getAwsRegion();

  void setAwsRegion(String value);

  /** The AWS service endpoint used by the AWS client. */
  @Description("AWS service endpoint used by the AWS client")
  String getAwsServiceEndpoint();

  void setAwsServiceEndpoint(String value);

  /**
   * The credential instance that should be used to authenticate against AWS services. The option
   * value must contain a "@type" field and an AWS Credentials Provider class as the field value.
   * Refer to {@link DefaultAWSCredentialsProviderChain} Javadoc for usage help.
   *
   * <p>For example, to specify the AWS key ID and secret, specify the following: <code>
   * {"@type" : "AWSStaticCredentialsProvider", "awsAccessKeyId" : "key_id_value",
   * "awsSecretKey" : "secret_value"}
   * </code>
   */
  @Description(
      "The credential instance that should be used to authenticate "
          + "against AWS services. The option value must contain \"@type\" field "
          + "and an AWS Credentials Provider class name as the field value. "
          + "Refer to DefaultAWSCredentialsProviderChain Javadoc for usage help. "
          + "For example, to specify the AWS key ID and secret, specify the following: "
          + "{\"@type\": \"AWSStaticCredentialsProvider\", "
          + "\"awsAccessKeyId\":\"<key_id>\", \"awsSecretKey\":\"<secret_key>\"}")
  @Default.InstanceFactory(AwsUserCredentialsFactory.class)
  AWSCredentialsProvider getAwsCredentialsProvider();

  void setAwsCredentialsProvider(AWSCredentialsProvider value);

  /** Attempts to load AWS credentials. */
  class AwsUserCredentialsFactory implements DefaultValueFactory<AWSCredentialsProvider> {

    @Override
    public AWSCredentialsProvider create(PipelineOptions options) {
      return DefaultAWSCredentialsProviderChain.getInstance();
    }
  }

  /**
   * The client configuration instance that should be used to configure AWS service clients. Please
   * note that the configuration deserialization only allows one to specify proxy settings. Please
   * use AwsHttpClientConfiguration's client configuration to set a wider range of options.
   *
   * <p>For example, to specify the proxy host, port, username and password, specify the following:
   * <code>
   * --clientConfiguration={
   *   "proxyHost":"hostname",
   *   "proxyPort":1234,
   *   "proxyUsername":"username",
   *   "proxyPassword":"password"
   * }
   * </code>
   *
   * @return
   */
  @Description(
      "The client configuration instance that should be used to configure AWS service "
          + "clients. Please note that the configuration deserialization only allows one to specify "
          + "proxy settings. For example, to specify the proxy host, port, username and password, "
          + "specify the following: --clientConfiguration={\"proxyHost\":\"hostname\",\"proxyPort\":1234,"
          + "\"proxyUsername\":\"username\",\"proxyPassword\":\"password\"}")
  @Default.InstanceFactory(ClientConfigurationFactory.class)
  ClientConfiguration getClientConfiguration();

  void setClientConfiguration(ClientConfiguration clientConfiguration);

  /**
   * The client configuration instance that should be used to configure AWS service clients. Please
   * note that the configuration deserialization allows aws http client configuration settings.
   *
   * <p>For example, to set different timeout for aws client service : Note that all the below
   * fields are optional, so only add those configurations that need to be set. <code>
   * --clientConfiguration={
   *   "clientExecutionTimeout":1000,
   *   "connectionMaxIdleTime":3000,
   *   "connectionTimeout":10000,
   *   "requestTimeout":30,
   *   "socketTimeout":600,
   *   "maxConnections":10,
   *   "socketTimeout":5000
   * }
   * </code>
   *
   * @return
   */
  @Description(
      "The client configuration instance that should be used to configure AWS http client configuration parameters."
          + "Mentioned parameters are the available parameters that can be set. All above parameters are "
          + "optional set only those that need custom changes.")
  @Default.InstanceFactory(ClientConfigurationFactory.class)
  ClientConfiguration getAwsHttpClientConfiguration();

  void setAwsHttpClientConfiguration(ClientConfiguration clientConfiguration);

  /** Default AWS client configuration. */
  class ClientConfigurationFactory implements DefaultValueFactory<ClientConfiguration> {

    @Override
    public ClientConfiguration create(PipelineOptions options) {
      return new ClientConfiguration();
    }
  }
}

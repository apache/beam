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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.utils.AttributeMap;

/**
 * Options used to configure Amazon Web Services specific options such as credentials and region.
 */
@Experimental(Kind.SOURCE_SINK)
public interface AwsOptions extends PipelineOptions {

  /** AWS region used by the AWS client. */
  @Description("AWS region used by the AWS client")
  @Validation.Required
  String getRegion();

  void setRegion(String value);

  /** The AWS service endpoint used by the AWS client. */
  @Description("AWS service endpoint used by the AWS client")
  String getEndpoint();

  void setEndpoint(String value);

  /**
   * The credential instance that should be used to authenticate against AWS services. The option
   * value must contain a "@type" field and an AWS Credentials Provider class as the field value.
   * Refer to {@link DefaultCredentialsProvider} Javadoc for usage help.
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
          + "{\"@type\": \"StaticCredentialsProvider\", "
          + "\"accessKeyId\":\"<key_id>\", \"secretAccessKey\":\"<secret_key>\"}")
  @Default.InstanceFactory(AwsUserCredentialsFactory.class)
  AwsCredentialsProvider getAwsCredentialsProvider();

  void setAwsCredentialsProvider(AwsCredentialsProvider value);

  /** Attempts to load AWS credentials. */
  class AwsUserCredentialsFactory implements DefaultValueFactory<AwsCredentialsProvider> {
    @Override
    public AwsCredentialsProvider create(PipelineOptions options) {
      return DefaultCredentialsProvider.create();
    }
  }

  /**
   * The client configuration instance that should be used to configure AWS service clients. Please
   * note that the configuration deserialization only allows one to specify proxy settings.
   *
   * <p>For example, to specify the proxy endpoint, username and password, specify the following:
   * <code>
   * --proxyConfiguration={
   *   "endpoint": "http://hostname:port",
   *   "username": "username",
   *   "password": "password"
   * }
   * </code>
   */
  @Description(
      "The proxy configuration instance that should be used to configure AWS service "
          + "clients. Please note that the configuration deserialization only allows one to specify "
          + "proxy settings. For example, to specify the proxy endpoint, username and password, "
          + "specify the following: --proxyConfiguration={\"endpoint\":\"http://hostname:port\", \"username\":\"username\", \"password\":\"password\"}")
  ProxyConfiguration getProxyConfiguration();

  void setProxyConfiguration(ProxyConfiguration value);

  /**
   * The client configuration instance that should be used to configure AWS service clients. Please
   * note that the configuration deserialization allows aws http client configuration settings.
   *
   * <p>For example, to set different timeout for aws client service : Note that all the below
   * fields are optional, so only add those configurations that need to be set. <code>
   * --attributeMap={
   *   "connectionAcquisitionTimeout":"PT1000S",
   *   "connectionMaxIdleTime":"PT3000S",
   *   "connectionTimeout":"PT10000S",
   *   "socketTimeout":"PT600S",
   *   "maxConnections":"10",
   *   "socketTimeout":"PT5000SS"
   * }
   * </code>
   *
   * @return
   */
  @Description(
      "The attribute map instance that should be used to configure AWS http client configuration parameters."
          + "Mentioned parameters are the available parameters that can be set. All above parameters are "
          + "optional set only those that need custom changes.")
  AttributeMap getAttributeMap();

  void setAttributeMap(AttributeMap attributeMap);
}

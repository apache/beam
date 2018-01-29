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

  /**
   * AWS region used by the AWS client.
   */
  @Description("AWS region used by the AWS client")
  @Validation.Required
  String getAwsRegion();
  void setAwsRegion(String value);

  /**
   * The AWS service endpoint used by the AWS client.
   */
  @Description("AWS service endpoint used by the AWS client")
  String getAwsServiceEndpoint();
  void setAwsServiceEndpoint(String value);

  /**
   * The credential instance that should be used to authenticate against AWS services. Refer to
   * {@link DefaultAWSCredentialsProviderChain} Javadoc for usage help.
   */
  @Description("The credential instance that should be used to authenticate against AWS services. "
      + "Refer to DefaultAWSCredentialsProviderChain Javadoc for usage help.")
  @Default.InstanceFactory(AwsUserCredentialsFactory.class)
  AWSCredentialsProvider getAwsCredentialsProvider();
  void setAwsCredentialsProvider(AWSCredentialsProvider value);

  /**
   * Attempts to load AWS credentials.
   */
  class AwsUserCredentialsFactory implements DefaultValueFactory<AWSCredentialsProvider> {

    @Override
    public AWSCredentialsProvider create(PipelineOptions options) {
      return DefaultAWSCredentialsProviderChain.getInstance();
    }
  }
}

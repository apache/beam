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
package org.apache.beam.sdk.io.aws.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.aws.options.AwsModule;
import org.apache.beam.sdk.io.aws.options.AwsOptions;

class SqsConfiguration implements Serializable {

  private String awsRegion;
  private String awsCredentialsProviderString;
  private String awsClientConfigurationString;

  public SqsConfiguration(AwsOptions awsOptions) {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      this.awsCredentialsProviderString =
          om.writeValueAsString(awsOptions.getAwsCredentialsProvider());
    } catch (JsonProcessingException e) {
      this.awsCredentialsProviderString = null;
    }

    try {
      this.awsClientConfigurationString =
          om.writeValueAsString(awsOptions.getClientConfiguration());
    } catch (JsonProcessingException e) {
      this.awsClientConfigurationString = null;
    }

    this.awsRegion = awsOptions.getAwsRegion();
  }

  public AWSCredentialsProvider getAwsCredentialsProvider() {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      return om.readValue(awsCredentialsProviderString, AWSCredentialsProvider.class);
    } catch (IOException e) {
      return null;
    }
  }

  public ClientConfiguration getClientConfiguration() {
    ObjectMapper om = new ObjectMapper();
    om.registerModule(new AwsModule());
    try {
      return om.readValue(awsClientConfigurationString, ClientConfiguration.class);
    } catch (IOException e) {
      return null;
    }
  }

  public String getAwsRegion() {
    return awsRegion;
  }
}

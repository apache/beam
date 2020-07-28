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
package org.apache.beam.sdk.io.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Basic implementation of {@link AWSClientsProvider} used by default in {@link KinesisIO}. */
class BasicKinesisProvider implements AWSClientsProvider {
  private final String accessKey;
  private final String secretKey;
  private final Regions region;
  private final @Nullable String serviceEndpoint;

  BasicKinesisProvider(
      String accessKey, String secretKey, Regions region, @Nullable String serviceEndpoint) {
    checkArgument(accessKey != null, "accessKey can not be null");
    checkArgument(secretKey != null, "secretKey can not be null");
    checkArgument(region != null, "region can not be null");
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
    this.serviceEndpoint = serviceEndpoint;
  }

  private AWSCredentialsProvider getCredentialsProvider() {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
  }

  @Override
  public AmazonKinesis getKinesisClient() {
    AmazonKinesisClientBuilder clientBuilder =
        AmazonKinesisClientBuilder.standard().withCredentials(getCredentialsProvider());
    if (serviceEndpoint == null) {
      clientBuilder.withRegion(region);
    } else {
      clientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region.getName()));
    }
    return clientBuilder.build();
  }

  @Override
  public AmazonCloudWatch getCloudWatchClient() {
    AmazonCloudWatchClientBuilder clientBuilder =
        AmazonCloudWatchClientBuilder.standard().withCredentials(getCredentialsProvider());
    if (serviceEndpoint == null) {
      clientBuilder.withRegion(region);
    } else {
      clientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region.getName()));
    }
    return clientBuilder.build();
  }

  @Override
  public IKinesisProducer createKinesisProducer(KinesisProducerConfiguration config) {
    config.setRegion(region.getName());
    config.setCredentialsProvider(getCredentialsProvider());
    return new KinesisProducer(config);
  }
}

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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import java.net.URI;
import java.util.Objects;
import org.apache.beam.sdk.io.kinesis.serde.AwsSerializableUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Basic implementation of {@link AWSClientsProvider} used by default in {@link KinesisIO}. */
class BasicKinesisProvider implements AWSClientsProvider {
  private final String awsCredentialsProviderSerialized;
  private final Regions region;
  private final @Nullable String serviceEndpoint;
  private final boolean verifyCertificate;

  BasicKinesisProvider(
      AWSCredentialsProvider awsCredentialsProvider,
      Regions region,
      @Nullable String serviceEndpoint,
      boolean verifyCertificate) {
    checkArgument(awsCredentialsProvider != null, "awsCredentialsProvider can not be null");
    checkArgument(region != null, "region can not be null");
    this.awsCredentialsProviderSerialized = AwsSerializableUtils.serialize(awsCredentialsProvider);
    checkNotNull(awsCredentialsProviderSerialized, "awsCredentialsProviderString can not be null");
    this.region = region;
    this.serviceEndpoint = serviceEndpoint;
    this.verifyCertificate = verifyCertificate;
  }

  private AWSCredentialsProvider getCredentialsProvider() {
    return AwsSerializableUtils.deserialize(awsCredentialsProviderSerialized);
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
    if (serviceEndpoint != null) {
      URI uri = URI.create(serviceEndpoint);
      config.setKinesisEndpoint(uri.getHost());
      config.setKinesisPort(uri.getPort());
    }
    config.setVerifyCertificate(verifyCertificate);
    return new KinesisProducer(config);
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasicKinesisProvider that = (BasicKinesisProvider) o;
    return verifyCertificate == that.verifyCertificate
        && Objects.equals(awsCredentialsProviderSerialized, that.awsCredentialsProviderSerialized)
        && Objects.equals(region, that.region)
        && Objects.equals(serviceEndpoint, that.serviceEndpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        awsCredentialsProviderSerialized, region, serviceEndpoint, verifyCertificate);
  }
}

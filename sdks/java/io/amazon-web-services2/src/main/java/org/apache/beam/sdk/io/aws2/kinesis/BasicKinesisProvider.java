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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import javax.annotation.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClientBuilder;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;

/** Basic implementation of {@link AWSClientsProvider} used by default in {@link KinesisIO}. */
class BasicKinesisProvider implements AWSClientsProvider {
  private final String accessKey;
  private final String secretKey;
  private final String region;
  @Nullable private final String serviceEndpoint;

  BasicKinesisProvider(
      String accessKey, String secretKey, Region region, @Nullable String serviceEndpoint) {
    checkArgument(accessKey != null, "accessKey can not be null");
    checkArgument(secretKey != null, "secretKey can not be null");
    checkArgument(region != null, "region can not be null");
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region.toString();
    this.serviceEndpoint = serviceEndpoint;
  }

  private AwsCredentialsProvider getCredentialsProvider() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
  }

  @Override
  public KinesisClient getKinesisClient() {
    KinesisClientBuilder clientBuilder =
        KinesisClient.builder()
            .credentialsProvider(getCredentialsProvider())
            .region(Region.of(region));
    if (serviceEndpoint != null) {
      clientBuilder.endpointOverride(URI.create(serviceEndpoint));
    }
    return clientBuilder.build();
  }

  @Override
  public CloudWatchClient getCloudWatchClient() {
    CloudWatchClientBuilder clientBuilder =
        CloudWatchClient.builder()
            .credentialsProvider(getCredentialsProvider())
            .region(Region.of(region));
    if (serviceEndpoint != null) {
      clientBuilder.endpointOverride(URI.create(serviceEndpoint));
    }
    return clientBuilder.build();
  }
}

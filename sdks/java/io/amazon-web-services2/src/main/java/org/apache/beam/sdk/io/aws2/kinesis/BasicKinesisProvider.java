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

import static org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory.defaultFactory;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.net.URI;
import org.apache.beam.sdk.io.aws2.common.ClientConfiguration;
import org.apache.beam.sdk.io.aws2.options.AwsOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;

/**
 * Basic implementation of {@link AWSClientsProvider} used by default in {@link KinesisIO}.
 *
 * @deprecated Configure a custom {@link org.apache.beam.sdk.io.aws2.common.ClientBuilderFactory}
 *     using {@link AwsOptions#getClientBuilderFactory()} instead.
 */
@Deprecated
class BasicKinesisProvider implements AWSClientsProvider {
  private final ClientConfiguration config;

  BasicKinesisProvider(
      AwsCredentialsProvider credentialsProvider, Region region, @Nullable String serviceEndpoint) {
    checkArgument(credentialsProvider != null, "awsCredentialsProvider can not be null");
    checkArgument(region != null, "region can not be null");
    URI endpoint = serviceEndpoint != null ? URI.create(serviceEndpoint) : null;
    config = ClientConfiguration.create(credentialsProvider, region, endpoint);
  }

  @Override
  public KinesisClient getKinesisClient() {
    return defaultFactory().create(KinesisClient.builder(), config, null).build();
  }

  @Override
  public CloudWatchClient getCloudWatchClient() {
    return defaultFactory().create(CloudWatchClient.builder(), config, null).build();
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
    return config.equals(that.config);
  }

  @Override
  public int hashCode() {
    return config.hashCode();
  }
}

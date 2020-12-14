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
package org.apache.beam.sdk.io.aws2.sns;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.Objects;
import org.apache.beam.sdk.io.aws2.options.AwsSerializableUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.SnsClientBuilder;

/** Basic implementation of {@link SnsClientProvider} used by default in {@link SnsIO}. */
class BasicSnsClientProvider implements SnsClientProvider {
  private final String awsCredentialsProviderSerialized;
  private final String region;
  private final @Nullable URI serviceEndpoint;

  BasicSnsClientProvider(
      AwsCredentialsProvider awsCredentialsProvider, String region, @Nullable URI serviceEndpoint) {
    checkArgument(awsCredentialsProvider != null, "awsCredentialsProvider can not be null");
    checkArgument(region != null, "region can not be null");
    this.awsCredentialsProviderSerialized =
        AwsSerializableUtils.serializeAwsCredentialsProvider(awsCredentialsProvider);
    checkNotNull(awsCredentialsProviderSerialized, "awsCredentialsProviderString can not be null");
    this.region = region;
    this.serviceEndpoint = serviceEndpoint;
  }

  @Override
  public SnsClient getSnsClient() {
    SnsClientBuilder builder =
        SnsClient.builder()
            .credentialsProvider(
                AwsSerializableUtils.deserializeAwsCredentialsProvider(
                    awsCredentialsProviderSerialized))
            .region(Region.of(region));

    if (serviceEndpoint != null) {
      builder.endpointOverride(serviceEndpoint);
    }

    return builder.build();
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BasicSnsClientProvider that = (BasicSnsClientProvider) o;
    return Objects.equals(awsCredentialsProviderSerialized, that.awsCredentialsProviderSerialized)
        && Objects.equals(region, that.region)
        && Objects.equals(serviceEndpoint, that.serviceEndpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(awsCredentialsProviderSerialized, region, serviceEndpoint);
  }
}

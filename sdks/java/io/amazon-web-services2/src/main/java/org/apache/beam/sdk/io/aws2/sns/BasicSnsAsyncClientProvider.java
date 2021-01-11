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

import java.net.URI;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.SnsAsyncClientBuilder;

/** Basic implementation of {@link SnsAsyncClientProvider} used by default in {@link SnsIO}. */
class BasicSnsAsyncClientProvider implements SnsAsyncClientProvider {
  private final AwsCredentialsProvider awsCredentialsProvider;
  private final String region;
  private final @Nullable URI serviceEndpoint;

  BasicSnsAsyncClientProvider(
      AwsCredentialsProvider awsCredentialsProvider, String region, @Nullable URI serviceEndpoint) {
    checkArgument(awsCredentialsProvider != null, "awsCredentialsProvider can not be null");
    checkArgument(region != null, "region can not be null");
    this.awsCredentialsProvider = awsCredentialsProvider;
    this.region = region;
    this.serviceEndpoint = serviceEndpoint;
  }

  @Override
  public SnsAsyncClient getSnsAsyncClient() {
    SnsAsyncClientBuilder builder =
        SnsAsyncClient.builder()
            .credentialsProvider(awsCredentialsProvider)
            .region(Region.of(region));

    if (serviceEndpoint != null) {
      builder.endpointOverride(serviceEndpoint);
    }

    return builder.build();
  }
}

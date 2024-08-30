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
package org.apache.beam.sdk.io.aws.s3;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.beam.sdk.io.aws.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/**
 * Construct AmazonS3ClientBuilder with default values of S3 client properties like path style
 * access, accelerated mode, etc.
 */
public class DefaultS3ClientBuilderFactory implements S3ClientBuilderFactory {

  @Override
  public AmazonS3ClientBuilder createBuilder(S3Options s3Options) {
    AmazonS3ClientBuilder builder =
        AmazonS3ClientBuilder.standard().withCredentials(s3Options.getAwsCredentialsProvider());

    if (s3Options.getClientConfiguration() != null) {
      builder = builder.withClientConfiguration(s3Options.getClientConfiguration());
    }

    if (!Strings.isNullOrEmpty(s3Options.getAwsServiceEndpoint())) {
      builder =
          builder.withEndpointConfiguration(
              new AwsClientBuilder.EndpointConfiguration(
                  s3Options.getAwsServiceEndpoint(), s3Options.getAwsRegion()));
    } else if (!Strings.isNullOrEmpty(s3Options.getAwsRegion())) {
      builder = builder.withRegion(s3Options.getAwsRegion());
    }
    return builder;
  }
}

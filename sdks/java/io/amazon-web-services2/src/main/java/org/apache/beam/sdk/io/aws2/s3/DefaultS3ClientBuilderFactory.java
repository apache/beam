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
package org.apache.beam.sdk.io.aws2.s3;

import java.net.URI;
import org.apache.beam.sdk.io.aws2.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Construct S3ClientBuilder with default values of S3 client properties like path style access,
 * accelerated mode, etc.
 */
public class DefaultS3ClientBuilderFactory implements S3ClientBuilderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultS3ClientBuilderFactory.class);

  @Override
  public S3ClientBuilder createBuilder(S3Options s3Options) {
    return createBuilder(S3Client.builder(), s3Options);
  }

  @VisibleForTesting
  static S3ClientBuilder createBuilder(S3ClientBuilder builder, S3Options s3Options) {
    if (s3Options.getAwsCredentialsProvider() != null) {
      builder.credentialsProvider(s3Options.getAwsCredentialsProvider());
    }

    if (s3Options.getProxyConfiguration() != null) {
      builder.httpClient(
          ApacheHttpClient.builder().proxyConfiguration(s3Options.getProxyConfiguration()).build());
    }

    if (!Strings.isNullOrEmpty(s3Options.getEndpoint())) {
      builder.endpointOverride(URI.create(s3Options.getEndpoint()));
    }

    if (!Strings.isNullOrEmpty(s3Options.getAwsRegion())) {
      builder.region(Region.of(s3Options.getAwsRegion()));
    } else {
      LOG.info(
          "The AWS S3 Beam extension was included in this build, but the awsRegion flag "
              + "was not specified. If you don't plan to use S3, then ignore this message.");
    }
    return builder;
  }
}

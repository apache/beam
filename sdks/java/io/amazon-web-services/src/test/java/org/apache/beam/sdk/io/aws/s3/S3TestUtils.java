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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.amazonaws.util.Base64;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.mockito.Mockito;

/** Utils to test S3 filesystem. */
class S3TestUtils {
  static S3Options s3Options() {
    S3Options options = PipelineOptionsFactory.as(S3Options.class);
    options.setAwsRegion("us-west-1");
    options.setS3UploadBufferSizeBytes(5_242_880);
    return options;
  }

  static S3Options s3OptionsWithCustomEndpointAndPathStyleAccessEnabled() {
    S3Options options = PipelineOptionsFactory.as(S3Options.class);
    options.setAwsServiceEndpoint("https://s3.custom.dns");
    options.setAwsRegion("no-matter");
    options.setS3UploadBufferSizeBytes(5_242_880);
    options.setS3ClientFactoryClass(PathStyleAcccessS3ClientBuilderFactory.class);
    return options;
  }

  static S3Options s3OptionsWithSSEAlgorithm() {
    S3Options options = s3Options();
    options.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
    return options;
  }

  static S3Options s3OptionsWithSSECustomerKey() {
    S3Options options = s3Options();
    options.setSSECustomerKey(new SSECustomerKey("86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA="));
    return options;
  }

  static S3Options s3OptionsWithSSEAwsKeyManagementParams() {
    S3Options options = s3Options();
    String awsKmsKeyId =
        "arn:aws:kms:eu-west-1:123456789012:key/dc123456-7890-ABCD-EF01-234567890ABC";
    SSEAwsKeyManagementParams sseAwsKeyManagementParams =
        new SSEAwsKeyManagementParams(awsKmsKeyId);
    options.setSSEAwsKeyManagementParams(sseAwsKeyManagementParams);
    return options;
  }

  static S3Options s3OptionsWithMultipleSSEOptions() {
    S3Options options = s3OptionsWithSSEAwsKeyManagementParams();
    options.setSSECustomerKey(new SSECustomerKey("86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA="));
    return options;
  }

  static S3FileSystem buildMockedS3FileSystem(S3Options options) {
    return buildMockedS3FileSystem(options, Mockito.mock(AmazonS3.class));
  }

  static S3FileSystem buildMockedS3FileSystem(S3Options options, AmazonS3 client) {
    S3FileSystem s3FileSystem = new S3FileSystem(options);
    s3FileSystem.setAmazonS3Client(client);
    return s3FileSystem;
  }

  static @Nullable String getSSECustomerKeyMd5(S3Options options) {
    SSECustomerKey sseCostumerKey = options.getSSECustomerKey();
    if (sseCostumerKey != null) {
      return Base64.encodeAsString(DigestUtils.md5(Base64.decode(sseCostumerKey.getKey())));
    }
    return null;
  }

  private static class PathStyleAcccessS3ClientBuilderFactory
      extends DefaultS3ClientBuilderFactory {
    @Override
    public AmazonS3ClientBuilder createBuilder(S3Options s3Options) {
      return super.createBuilder(s3Options).withPathStyleAccessEnabled(true);
    }
  }
}

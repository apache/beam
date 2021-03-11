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

import static org.apache.beam.sdk.io.aws2.options.S3Options.S3UploadBufferSizeBytesFactory.MINIMUM_UPLOAD_BUFFER_SIZE_BYTES;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/** Utils to test S3 filesystem. */
class S3TestUtils {

  static S3Options s3Options() {
    S3Options options = PipelineOptionsFactory.as(S3Options.class);
    options.setAwsRegion("us-west-1");
    options.setS3UploadBufferSizeBytes(MINIMUM_UPLOAD_BUFFER_SIZE_BYTES);
    return options;
  }

  static S3Options s3OptionsWithPathStyleAccessEnabled() {
    S3Options options = PipelineOptionsFactory.as(S3Options.class);
    options.setAwsRegion("us-west-1");
    options.setS3UploadBufferSizeBytes(MINIMUM_UPLOAD_BUFFER_SIZE_BYTES);
    options.setS3ClientFactoryClass(PathStyleAccessS3ClientBuilderFactory.class);
    return options;
  }

  static S3Options s3OptionsWithSSEAlgorithm() {
    S3Options options = s3Options();
    options.setSSEAlgorithm(ServerSideEncryption.AES256.name());
    return options;
  }

  static S3Options s3OptionsWithSSECustomerKey() {
    S3Options options = s3Options();
    options.setSSECustomerKey(
        SSECustomerKey.builder()
            .key("86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=")
            .algorithm("AES256")
            .build());
    return options;
  }

  static S3Options s3OptionsWithSSEKMSKeyId() {
    S3Options options = s3Options();
    String ssekmsKeyId =
        "arn:aws:kms:eu-west-1:123456789012:key/dc123456-7890-ABCD-EF01-234567890ABC";
    options.setSSEKMSKeyId(ssekmsKeyId);
    options.setSSEAlgorithm("aws:kms");
    return options;
  }

  static S3Options s3OptionsWithMultipleSSEOptions() {
    S3Options options = s3OptionsWithSSEKMSKeyId();
    options.setSSECustomerKey(
        SSECustomerKey.builder()
            .key("86glyTlCNZgccSxW8JxMa6ZdjdK3N141glAysPUZ3AA=")
            .algorithm("AES256")
            .build());
    return options;
  }

  static S3FileSystem buildMockedS3FileSystem(S3Options options) {
    return buildMockedS3FileSystem(options, Mockito.mock(S3Client.class));
  }

  static S3FileSystem buildMockedS3FileSystem(S3Options options, S3Client client) {
    S3FileSystem s3FileSystem = new S3FileSystem(options);
    s3FileSystem.setS3Client(client);
    return s3FileSystem;
  }

  static @Nullable String getSSECustomerKeyMd5(S3Options options) {
    String sseCostumerKey = options.getSSECustomerKey().getKey();
    if (sseCostumerKey != null) {
      Base64.Decoder decoder = Base64.getDecoder();
      Base64.Encoder encoder = Base64.getEncoder();
      return encoder.encodeToString(
          DigestUtils.md5(decoder.decode(sseCostumerKey.getBytes(StandardCharsets.UTF_8))));
    }
    return null;
  }

  private static class PathStyleAccessS3ClientBuilderFactory extends DefaultS3ClientBuilderFactory {

    @Override
    public S3ClientBuilder createBuilder(S3Options s3Options) {
      S3ClientBuilder s3ClientBuilder = super.createBuilder(s3Options);
      return s3ClientBuilder.serviceConfiguration(
          S3Configuration.builder().pathStyleAccessEnabled(true).build());
    }
  }
}

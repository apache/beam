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
package org.apache.beam.sdk.io.aws.options;

import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import org.apache.beam.sdk.io.aws.s3.DefaultS3ClientBuilderFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options used to configure Amazon Web Services S3. */
public interface S3Options extends AwsOptions {

  @Description("AWS S3 storage class used for creating S3 objects")
  @Default.String("STANDARD")
  String getS3StorageClass();

  void setS3StorageClass(String value);

  @Description(
      "Size of S3 upload chunks; max upload object size is this value multiplied by 10000;"
          + "default is 64MB, or 5MB in memory-constrained environments. Must be at least 5MB.")
  @Default.InstanceFactory(S3UploadBufferSizeBytesFactory.class)
  Integer getS3UploadBufferSizeBytes();

  void setS3UploadBufferSizeBytes(Integer value);

  @Description("Thread pool size, limiting max concurrent S3 operations")
  @Default.Integer(50)
  int getS3ThreadPoolSize();

  void setS3ThreadPoolSize(int value);

  @Description("Algorithm for SSE-S3 encryption, e.g. AES256.")
  @Nullable
  String getSSEAlgorithm();

  void setSSEAlgorithm(String value);

  @Description(
      "SSE key for SSE-C encryption, e.g. a base64 encoded key and the algorithm."
          + "To specify on the command-line, represent the value as a JSON object. For example:"
          + " --SSECustomerKey={\"key\": \"86glyTlCN...\", \"algorithm\": \"AES256\"}")
  @Nullable
  SSECustomerKey getSSECustomerKey();

  void setSSECustomerKey(SSECustomerKey value);

  @Description(
      "KMS key id for SSE-KMS encryption, e.g. \"arn:aws:kms:...\"."
          + "To specify on the command-line, represent the value as a JSON object. For example:"
          + " --SSEAwsKeyManagementParams={\"awsKmsKeyId\": \"arn:aws:kms:...\"}")
  @Nullable
  SSEAwsKeyManagementParams getSSEAwsKeyManagementParams();

  void setSSEAwsKeyManagementParams(SSEAwsKeyManagementParams value);

  @Description(
      "Factory class that should be created and used to create a builder of AmazonS3 client."
          + "Override the default value if you need a S3 client with custom properties, like path style access, etc.")
  @Default.Class(DefaultS3ClientBuilderFactory.class)
  Class<? extends S3ClientBuilderFactory> getS3ClientFactoryClass();

  void setS3ClientFactoryClass(Class<? extends S3ClientBuilderFactory> s3ClientFactoryClass);

  /**
   * Provide the default s3 upload buffer size in bytes: 64MB if more than 512MB in RAM are
   * available and 5MB otherwise.
   */
  class S3UploadBufferSizeBytesFactory implements DefaultValueFactory<Integer> {
    public static final int MINIMUM_UPLOAD_BUFFER_SIZE_BYTES = 5_242_880;

    @Override
    public Integer create(PipelineOptions options) {
      return Runtime.getRuntime().maxMemory() < 536_870_912
          ? MINIMUM_UPLOAD_BUFFER_SIZE_BYTES
          : 67_108_864;
    }
  }
}

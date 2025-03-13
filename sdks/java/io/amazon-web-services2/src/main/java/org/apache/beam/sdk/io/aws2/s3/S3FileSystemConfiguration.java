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

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.aws2.options.S3ClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.util.InstanceBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * Object used to configure {@link S3FileSystem}.
 *
 * @see S3Options
 * @see S3FileSystemSchemeRegistrar
 */
@AutoValue
public abstract class S3FileSystemConfiguration {
  public static final int MINIMUM_UPLOAD_BUFFER_SIZE_BYTES = 5_242_880;

  /** The uri scheme used by resources on this filesystem. */
  public abstract String getScheme();

  /** The AWS S3 storage class used for creating S3 objects. */
  public abstract String getS3StorageClass();

  /** Size of S3 upload chnks. */
  public abstract int getS3UploadBufferSizeBytes();

  /** Thread pool size, limiting the max concurrent S3 operations. */
  public abstract int getS3ThreadPoolSize();

  /** Algorithm for SSE-S3 encryption, e.g. AES256. */
  public abstract @Nullable String getSSEAlgorithm();

  /** SSE key for SSE-C encryption, e.g. a base64 encoded key and the algorithm. */
  public abstract SSECustomerKey getSSECustomerKey();

  /** KMS key id for SSE-KMS encyrption, e.g. "arn:aws:kms:..." */
  public abstract @Nullable String getSSEKMSKeyId();

  /**
   * Whether to use an S3 Bucket Key for object encryption with server-side encryption using AWS KMS
   * (SSE-KMS) or not.
   */
  public abstract boolean getBucketKeyEnabled();

  /** Builder used to create the {@code S3Client}. */
  public abstract S3ClientBuilder getS3ClientBuilder();

  /** Creates a new uninitialized {@link Builder}. */
  public static Builder builder() {
    return new AutoValue_S3FileSystemConfiguration.Builder();
  }

  /** Creates a new {@link Builder} with values initialized by this instance's properties. */
  public abstract Builder toBuilder();

  /**
   * Creates a new {@link Builder} with values initialized by the properties of {@code s3Options}.
   */
  public static Builder builderFrom(S3Options s3Options) {
    return builder()
        .setScheme("s3")
        .setS3StorageClass(s3Options.getS3StorageClass())
        .setS3UploadBufferSizeBytes(s3Options.getS3UploadBufferSizeBytes())
        .setS3ThreadPoolSize(s3Options.getS3ThreadPoolSize())
        .setSSEAlgorithm(s3Options.getSSEAlgorithm())
        .setSSECustomerKey(s3Options.getSSECustomerKey())
        .setSSEKMSKeyId(s3Options.getSSEKMSKeyId())
        .setBucketKeyEnabled(s3Options.getBucketKeyEnabled())
        .setS3ClientBuilder(getBuilder(s3Options));
  }

  public static S3FileSystemConfiguration fromS3Options(S3Options s3Options) {
    return builderFrom(s3Options).build();
  }

  /** Creates a new {@link S3ClientBuilder} as specified by {@code s3Options}. */
  private static S3ClientBuilder getBuilder(S3Options s3Options) {
    return InstanceBuilder.ofType(S3ClientBuilderFactory.class)
        .fromClass(s3Options.getS3ClientFactoryClass())
        .build()
        .createBuilder(s3Options);
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setScheme(String value);

    public abstract Builder setS3StorageClass(String value);

    public abstract Builder setS3UploadBufferSizeBytes(int value);

    public abstract Builder setS3ThreadPoolSize(int value);

    public abstract Builder setSSEAlgorithm(@Nullable String value);

    public abstract Builder setSSECustomerKey(SSECustomerKey value);

    public abstract Builder setSSEKMSKeyId(@Nullable String value);

    public abstract Builder setBucketKeyEnabled(boolean value);

    public abstract Builder setS3ClientBuilder(S3ClientBuilder value);

    public abstract S3FileSystemConfiguration build();
  }
}

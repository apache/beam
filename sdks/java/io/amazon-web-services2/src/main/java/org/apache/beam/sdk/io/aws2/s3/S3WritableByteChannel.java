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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Objects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/** A writable S3 object, as a {@link WritableByteChannel}. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
class S3WritableByteChannel implements WritableByteChannel {

  private final S3Client s3Client;
  private final S3FileSystemConfiguration config;
  private final S3ResourceId path;

  private final String uploadId;
  private final ByteBuffer uploadBuffer;

  // AWS S3 parts are 1-indexed, not zero-indexed.
  private int partNumber = 1;
  private boolean open = true;
  private final MessageDigest md5 = md5();
  private final ArrayList<CompletedPart> completedParts;

  S3WritableByteChannel(
      S3Client s3, S3ResourceId path, String contentType, S3FileSystemConfiguration config)
      throws IOException {
    this.s3Client = checkNotNull(s3, "s3Client");
    this.config = checkNotNull(config);
    this.path = checkNotNull(path, "path");

    String awsKms = ServerSideEncryption.AWS_KMS.toString();
    checkArgument(
        atMostOne(
            config.getSSECustomerKey().getKey() != null,
            (Objects.equals(config.getSSEAlgorithm(), awsKms) || config.getSSEKMSKeyId() != null),
            (config.getSSEAlgorithm() != null && !config.getSSEAlgorithm().equals(awsKms))),
        "Either SSECustomerKey (SSE-C) or SSEAlgorithm (SSE-S3)"
            + " or SSEAwsKeyManagementParams (SSE-KMS) must not be set at the same time.");
    // Amazon S3 API docs: Each part must be at least 5 MB in size, except the last part.
    checkArgument(
        config.getS3UploadBufferSizeBytes()
            >= S3FileSystemConfiguration.MINIMUM_UPLOAD_BUFFER_SIZE_BYTES,
        "S3UploadBufferSizeBytes must be at least %s bytes",
        S3FileSystemConfiguration.MINIMUM_UPLOAD_BUFFER_SIZE_BYTES);
    this.uploadBuffer = ByteBuffer.allocate(config.getS3UploadBufferSizeBytes());

    completedParts = new ArrayList<>();
    CreateMultipartUploadRequest createMultipartUploadRequest =
        CreateMultipartUploadRequest.builder()
            .bucket(path.getBucket())
            .key(path.getKey())
            .storageClass(config.getS3StorageClass())
            .contentType(contentType)
            .serverSideEncryption(config.getSSEAlgorithm())
            .sseCustomerKey(config.getSSECustomerKey().getKey())
            .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .ssekmsKeyId(config.getSSEKMSKeyId())
            .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
            .bucketKeyEnabled(config.getBucketKeyEnabled())
            .build();
    CreateMultipartUploadResponse response;
    try {
      response = this.s3Client.createMultipartUpload(createMultipartUploadRequest);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
    uploadId = response.uploadId();
  }

  private static MessageDigest md5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public int write(ByteBuffer sourceBuffer) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }

    int totalBytesWritten = 0;
    while (sourceBuffer.hasRemaining()) {
      int position = sourceBuffer.position();
      int bytesWritten = Math.min(sourceBuffer.remaining(), uploadBuffer.remaining());
      totalBytesWritten += bytesWritten;

      if (sourceBuffer.hasArray()) {
        // If the underlying array is accessible, direct access is the most efficient approach.
        int start = sourceBuffer.arrayOffset() + position;
        uploadBuffer.put(sourceBuffer.array(), start, bytesWritten);
        md5.update(sourceBuffer.array(), start, bytesWritten);
      } else {
        // Otherwise, use a readonly copy with an appropriate mark to read the current range of the
        // buffer twice.
        ByteBuffer copyBuffer = sourceBuffer.asReadOnlyBuffer();
        copyBuffer.mark().limit(position + bytesWritten);
        uploadBuffer.put(copyBuffer);
        copyBuffer.reset();
        md5.update(copyBuffer);
      }
      sourceBuffer.position(position + bytesWritten); // move position forward by the bytes written

      if (!uploadBuffer.hasRemaining() || sourceBuffer.hasRemaining()) {
        flush();
      }
    }

    return totalBytesWritten;
  }

  private void flush() throws IOException {
    uploadBuffer.flip();
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream(uploadBuffer.array(), 0, uploadBuffer.limit());

    UploadPartRequest request =
        UploadPartRequest.builder()
            .bucket(path.getBucket())
            .key(path.getKey())
            .uploadId(uploadId)
            .partNumber(partNumber++)
            .contentLength((long) uploadBuffer.limit())
            .sseCustomerKey(config.getSSECustomerKey().getKey())
            .sseCustomerAlgorithm(config.getSSECustomerKey().getAlgorithm())
            .sseCustomerKeyMD5(config.getSSECustomerKey().getMD5())
            .contentMD5(Base64.getEncoder().encodeToString(md5.digest()))
            .build();

    UploadPartResponse response;
    try {
      response =
          s3Client.uploadPart(
              request, RequestBody.fromInputStream(inputStream, request.contentLength()));
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
    CompletedPart part =
        CompletedPart.builder().partNumber(request.partNumber()).eTag(response.eTag()).build();
    uploadBuffer.clear();
    md5.reset();
    completedParts.add(part);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() throws IOException {
    open = false;
    if (uploadBuffer.remaining() > 0) {
      flush();
    }

    CompletedMultipartUpload completedMultipartUpload =
        CompletedMultipartUpload.builder().parts(completedParts).build();

    CompleteMultipartUploadRequest request =
        CompleteMultipartUploadRequest.builder()
            .bucket(path.getBucket())
            .key(path.getKey())
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();
    try {
      s3Client.completeMultipartUpload(request);
    } catch (SdkClientException e) {
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  static boolean atMostOne(boolean... values) {
    boolean one = false;
    for (boolean value : values) {
      if (!one && value) {
        one = true;
      } else if (value) {
        return false;
      }
    }
    return true;
  }
}

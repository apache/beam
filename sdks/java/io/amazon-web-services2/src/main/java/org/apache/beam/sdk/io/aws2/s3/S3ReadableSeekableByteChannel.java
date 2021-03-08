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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;

/** A readable S3 object, as a {@link SeekableByteChannel}. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
class S3ReadableSeekableByteChannel implements SeekableByteChannel {

  private final S3Client s3Client;
  private final S3ResourceId path;
  private final S3Options options;
  private final long contentLength;
  private long position = 0;
  private boolean open = true;
  private @Nullable ResponseInputStream<GetObjectResponse> s3ResponseInputStream;
  private @Nullable ReadableByteChannel s3ObjectContentChannel;

  S3ReadableSeekableByteChannel(S3Client s3Client, S3ResourceId path, S3Options options)
      throws IOException {
    this.s3Client = checkNotNull(s3Client, "s3Client");
    checkNotNull(path, "path");
    this.options = checkNotNull(options, "options");

    if (path.getSize().isPresent()) {
      contentLength = path.getSize().get();
      this.path = path;

    } else {
      HeadObjectRequest request =
          HeadObjectRequest.builder().bucket(path.getBucket()).key(path.getKey()).build();
      try {
        contentLength = s3Client.headObject(request).contentLength();
      } catch (SdkClientException e) {
        throw new IOException(e);
      }
      this.path = path.withSize(contentLength);
    }
  }

  @Override
  public int read(ByteBuffer destinationBuffer) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    if (!destinationBuffer.hasRemaining()) {
      return 0;
    }
    if (position == contentLength) {
      return -1;
    }

    if (s3ResponseInputStream == null) {

      GetObjectRequest.Builder builder =
          GetObjectRequest.builder()
              .bucket(path.getBucket())
              .key(path.getKey())
              .sseCustomerKey(options.getSSECustomerKey().getKey())
              .sseCustomerAlgorithm(options.getSSECustomerKey().getAlgorithm());
      if (position > 0) {
        builder.range(String.format("bytes=%s-%s", position, contentLength));
      }
      GetObjectRequest request = builder.build();
      try {
        s3ResponseInputStream = s3Client.getObject(request);
      } catch (SdkClientException e) {
        throw new IOException(e);
      }
      s3ObjectContentChannel =
          Channels.newChannel(new BufferedInputStream(s3ResponseInputStream, 1024 * 1024));
    }

    int totalBytesRead = 0;
    int bytesRead = 0;

    do {
      totalBytesRead += bytesRead;
      try {
        bytesRead = s3ObjectContentChannel.read(destinationBuffer);
      } catch (SdkServiceException e) {
        throw new IOException(e);
      }
    } while (bytesRead > 0);

    position += totalBytesRead;
    return totalBytesRead;
  }

  @Override
  public long position() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) throws IOException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    checkArgument(newPosition >= 0, "newPosition too low");
    checkArgument(newPosition < contentLength, "new position too high");

    if (newPosition == position) {
      return this;
    }

    // The position has changed, so close and destroy the object to induce a re-creation on the next
    // call to read()
    if (s3ResponseInputStream != null) {
      s3ResponseInputStream.close();
      s3ResponseInputStream = null;
    }
    position = newPosition;
    return this;
  }

  @Override
  public long size() throws ClosedChannelException {
    if (!isOpen()) {
      throw new ClosedChannelException();
    }
    return contentLength;
  }

  @Override
  public void close() throws IOException {
    if (s3ResponseInputStream != null) {
      s3ResponseInputStream.close();
    }
    open = false;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new NonWritableChannelException();
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new NonWritableChannelException();
  }
}

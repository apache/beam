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

import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3Config;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3ConfigWithMultipleSSEOptions;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3ConfigWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3ConfigWithSSEAwsKeyManagementParams;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3ConfigWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3Options;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithMultipleSSEOptions;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSEAwsKeyManagementParams;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.toMd5;
import static org.apache.beam.sdk.io.aws.s3.S3WritableByteChannel.atMostOne;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.io.aws.options.S3Options;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link S3WritableByteChannel}. */
@RunWith(JUnit4.class)
public class S3WritableByteChannelTest {
  @Rule public ExpectedException expected = ExpectedException.none();

  @Test
  public void write() throws IOException {
    writeFromConfig(s3Config("s3"), false);
    writeFromConfig(s3Config("s3"), true);
    writeFromConfig(s3ConfigWithSSEAlgorithm("s3"), false);
    writeFromConfig(s3ConfigWithSSECustomerKey("s3"), false);
    writeFromConfig(s3ConfigWithSSEAwsKeyManagementParams("s3"), false);
    expected.expect(IllegalArgumentException.class);
    writeFromConfig(s3ConfigWithMultipleSSEOptions("s3"), false);
  }

  @Test
  public void writeWithS3Options() throws IOException {
    writeFromOptions(s3Options(), false);
    writeFromOptions(s3Options(), true);
    writeFromOptions(s3OptionsWithSSEAlgorithm(), false);
    writeFromOptions(s3OptionsWithSSECustomerKey(), false);
    writeFromOptions(s3OptionsWithSSEAwsKeyManagementParams(), false);
    expected.expect(IllegalArgumentException.class);
    writeFromOptions(s3OptionsWithMultipleSSEOptions(), false);
  }

  @FunctionalInterface
  public interface Supplier {
    S3WritableByteChannel get() throws IOException;
  }

  private void writeFromOptions(S3Options options, boolean writeReadOnlyBuffer) throws IOException {
    AmazonS3 mockAmazonS3 = mock(AmazonS3.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    Supplier channel =
        () ->
            new S3WritableByteChannel(
                mockAmazonS3,
                path,
                "text/plain",
                S3FileSystemConfiguration.fromS3Options(options).build());
    write(
        mockAmazonS3,
        channel,
        path,
        options.getSSEAlgorithm(),
        toMd5(options.getSSECustomerKey()),
        options.getSSEAwsKeyManagementParams(),
        options.getS3UploadBufferSizeBytes(),
        options.getBucketKeyEnabled(),
        writeReadOnlyBuffer);
  }

  private void writeFromConfig(S3FileSystemConfiguration config, boolean writeReadOnlyBuffer)
      throws IOException {
    AmazonS3 mockAmazonS3 = mock(AmazonS3.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    Supplier channel = () -> new S3WritableByteChannel(mockAmazonS3, path, "text/plain", config);
    write(
        mockAmazonS3,
        channel,
        path,
        config.getSSEAlgorithm(),
        toMd5(config.getSSECustomerKey()),
        config.getSSEAwsKeyManagementParams(),
        config.getS3UploadBufferSizeBytes(),
        config.getBucketKeyEnabled(),
        writeReadOnlyBuffer);
  }

  private void write(
      AmazonS3 mockAmazonS3,
      Supplier channelSupplier,
      S3ResourceId path,
      String sseAlgorithm,
      String sseCustomerKeyMd5,
      SSEAwsKeyManagementParams sseAwsKeyManagementParams,
      long s3UploadBufferSizeBytes,
      boolean bucketKeyEnabled,
      boolean writeReadOnlyBuffer)
      throws IOException {
    InitiateMultipartUploadResult initiateMultipartUploadResult =
        new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("upload-id");
    if (sseAlgorithm != null) {
      initiateMultipartUploadResult.setSSEAlgorithm(sseAlgorithm);
    }
    if (sseCustomerKeyMd5 != null) {
      initiateMultipartUploadResult.setSSECustomerKeyMd5(sseCustomerKeyMd5);
    }
    if (sseAwsKeyManagementParams != null) {
      sseAlgorithm = "aws:kms";
      initiateMultipartUploadResult.setSSEAlgorithm(sseAlgorithm);
    }
    initiateMultipartUploadResult.setBucketKeyEnabled(bucketKeyEnabled);
    doReturn(initiateMultipartUploadResult)
        .when(mockAmazonS3)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

    InitiateMultipartUploadResult mockInitiateMultipartUploadResult =
        mockAmazonS3.initiateMultipartUpload(
            new InitiateMultipartUploadRequest(path.getBucket(), path.getKey()));
    assertEquals(sseAlgorithm, mockInitiateMultipartUploadResult.getSSEAlgorithm());
    assertEquals(bucketKeyEnabled, mockInitiateMultipartUploadResult.getBucketKeyEnabled());
    assertEquals(sseCustomerKeyMd5, mockInitiateMultipartUploadResult.getSSECustomerKeyMd5());

    UploadPartResult result = new UploadPartResult();
    result.setETag("etag");
    if (sseCustomerKeyMd5 != null) {
      result.setSSECustomerKeyMd5(sseCustomerKeyMd5);
    }
    doReturn(result).when(mockAmazonS3).uploadPart(any(UploadPartRequest.class));

    UploadPartResult mockUploadPartResult = mockAmazonS3.uploadPart(new UploadPartRequest());
    assertEquals(sseCustomerKeyMd5, mockUploadPartResult.getSSECustomerKeyMd5());

    int contentSize = 34_078_720;
    ByteBuffer uploadContent = ByteBuffer.allocate((int) (contentSize * 2.5));
    for (int i = 0; i < contentSize; i++) {
      uploadContent.put((byte) 0xff);
    }
    uploadContent.flip();

    S3WritableByteChannel channel = channelSupplier.get();
    int uploadedSize =
        channel.write(writeReadOnlyBuffer ? uploadContent.asReadOnlyBuffer() : uploadContent);
    assertEquals(contentSize, uploadedSize);

    CompleteMultipartUploadResult completeMultipartUploadResult =
        new CompleteMultipartUploadResult();
    doReturn(completeMultipartUploadResult)
        .when(mockAmazonS3)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

    channel.close();

    verify(mockAmazonS3, times(2))
        .initiateMultipartUpload(notNull(InitiateMultipartUploadRequest.class));
    int partQuantity = (int) Math.ceil((double) contentSize / s3UploadBufferSizeBytes) + 1;
    verify(mockAmazonS3, times(partQuantity)).uploadPart(notNull(UploadPartRequest.class));
    verify(mockAmazonS3, times(1))
        .completeMultipartUpload(notNull(CompleteMultipartUploadRequest.class));
    verifyNoMoreInteractions(mockAmazonS3);
  }

  @Test
  public void testAtMostOne() {
    assertTrue(atMostOne(true));
    assertTrue(atMostOne(false));
    assertFalse(atMostOne(true, true));
    assertTrue(atMostOne(true, false));
    assertTrue(atMostOne(false, true));
    assertTrue(atMostOne(false, false));
    assertFalse(atMostOne(true, true, true));
    assertFalse(atMostOne(true, true, false));
    assertFalse(atMostOne(true, false, true));
    assertTrue(atMostOne(true, false, false));
    assertFalse(atMostOne(false, true, true));
    assertTrue(atMostOne(false, true, false));
    assertTrue(atMostOne(false, false, true));
    assertTrue(atMostOne(false, false, false));
  }
}

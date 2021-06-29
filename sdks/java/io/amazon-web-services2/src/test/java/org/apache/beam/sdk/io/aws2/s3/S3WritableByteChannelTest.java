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

import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.getSSECustomerKeyMd5;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3Options;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithMultipleSSEOptions;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSEKMSKeyId;
import static org.apache.beam.sdk.io.aws2.s3.S3WritableByteChannel.atMostOne;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

/** Tests {@link S3WritableByteChannel}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class S3WritableByteChannelTest {

  @Test
  public void write() throws IOException {
    writeFromOptions(s3Options());
    writeFromOptions(s3OptionsWithSSEAlgorithm());
    writeFromOptions(s3OptionsWithSSECustomerKey());
    writeFromOptions(s3OptionsWithSSEKMSKeyId());
    assertThrows(
        IllegalArgumentException.class, () -> writeFromOptions(s3OptionsWithMultipleSSEOptions()));
  }

  private void writeFromOptions(S3Options options) throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");

    CreateMultipartUploadResponse.Builder builder =
        CreateMultipartUploadResponse.builder().uploadId("upload-id");

    ServerSideEncryption sseAlgorithm = ServerSideEncryption.fromValue(options.getSSEAlgorithm());
    if (options.getSSEAlgorithm() != null) {
      builder.serverSideEncryption(sseAlgorithm);
    }
    if (getSSECustomerKeyMd5(options) != null) {
      builder.sseCustomerKeyMD5(getSSECustomerKeyMd5(options));
    }
    if (options.getSSEKMSKeyId() != null) {
      sseAlgorithm = ServerSideEncryption.AWS_KMS;
      builder.serverSideEncryption(sseAlgorithm);
    }
    CreateMultipartUploadResponse createMultipartUploadResponse = builder.build();
    doReturn(createMultipartUploadResponse)
        .when(mockS3Client)
        .createMultipartUpload(any(CreateMultipartUploadRequest.class));

    CreateMultipartUploadRequest createMultipartUploadRequest =
        CreateMultipartUploadRequest.builder().bucket(path.getBucket()).key(path.getKey()).build();
    CreateMultipartUploadResponse mockCreateMultipartUploadResponse1 =
        mockS3Client.createMultipartUpload(createMultipartUploadRequest);
    assertEquals(sseAlgorithm, mockCreateMultipartUploadResponse1.serverSideEncryption());
    assertEquals(
        getSSECustomerKeyMd5(options), mockCreateMultipartUploadResponse1.sseCustomerKeyMD5());

    UploadPartResponse.Builder uploadPartResponseBuilder =
        UploadPartResponse.builder().eTag("etag");
    if (getSSECustomerKeyMd5(options) != null) {
      uploadPartResponseBuilder.sseCustomerKeyMD5(getSSECustomerKeyMd5(options));
    }
    UploadPartResponse response = uploadPartResponseBuilder.build();
    doReturn(response)
        .when(mockS3Client)
        .uploadPart(any(UploadPartRequest.class), any(RequestBody.class));

    UploadPartResponse mockUploadPartResult =
        mockS3Client.uploadPart(UploadPartRequest.builder().build(), RequestBody.empty());
    assertEquals(getSSECustomerKeyMd5(options), mockUploadPartResult.sseCustomerKeyMD5());

    S3WritableByteChannel channel =
        new S3WritableByteChannel(mockS3Client, path, "text/plain", options);
    int contentSize = 34_078_720;
    ByteBuffer uploadContent = ByteBuffer.allocate((int) (contentSize * 2.5));
    for (int i = 0; i < contentSize; i++) {
      uploadContent.put((byte) 0xff);
    }
    uploadContent.flip();

    int uploadedSize = channel.write(uploadContent);
    assertEquals(contentSize, uploadedSize);

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        CompleteMultipartUploadResponse.builder().build();
    doReturn(completeMultipartUploadResponse)
        .when(mockS3Client)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

    channel.close();

    int partQuantity =
        (int) Math.ceil((double) contentSize / options.getS3UploadBufferSizeBytes()) + 1;

    verify(mockS3Client, times(2))
        .createMultipartUpload((CreateMultipartUploadRequest) isNotNull());
    verify(mockS3Client, times(partQuantity))
        .uploadPart((UploadPartRequest) isNotNull(), any(RequestBody.class));
    verify(mockS3Client, times(1))
        .completeMultipartUpload((CompleteMultipartUploadRequest) notNull());
    verifyNoMoreInteractions(mockS3Client);
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

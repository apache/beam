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

import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.getSSECustomerKeyMd5;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3Options;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithMultipleSSEOptions;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSEAwsKeyManagementParams;
import static org.apache.beam.sdk.io.aws.s3.S3TestUtils.s3OptionsWithSSECustomerKey;
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
    writeFromOptions(s3Options());
    writeFromOptions(s3OptionsWithSSEAlgorithm());
    writeFromOptions(s3OptionsWithSSECustomerKey());
    writeFromOptions(s3OptionsWithSSEAwsKeyManagementParams());
    expected.expect(IllegalArgumentException.class);
    writeFromOptions(s3OptionsWithMultipleSSEOptions());
  }

  private void writeFromOptions(S3Options options) throws IOException {
    AmazonS3 mockAmazonS3 = mock(AmazonS3.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");

    InitiateMultipartUploadResult initiateMultipartUploadResult =
        new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId("upload-id");
    String sseAlgorithm = options.getSSEAlgorithm();
    if (options.getSSEAlgorithm() != null) {
      initiateMultipartUploadResult.setSSEAlgorithm(sseAlgorithm);
    }
    if (getSSECustomerKeyMd5(options) != null) {
      initiateMultipartUploadResult.setSSECustomerKeyMd5(getSSECustomerKeyMd5(options));
    }
    if (options.getSSEAwsKeyManagementParams() != null) {
      sseAlgorithm = "aws:kms";
      initiateMultipartUploadResult.setSSEAlgorithm(sseAlgorithm);
    }
    doReturn(initiateMultipartUploadResult)
        .when(mockAmazonS3)
        .initiateMultipartUpload(any(InitiateMultipartUploadRequest.class));

    InitiateMultipartUploadResult mockInitiateMultipartUploadResult =
        mockAmazonS3.initiateMultipartUpload(
            new InitiateMultipartUploadRequest(path.getBucket(), path.getKey()));
    assertEquals(sseAlgorithm, mockInitiateMultipartUploadResult.getSSEAlgorithm());
    assertEquals(
        getSSECustomerKeyMd5(options), mockInitiateMultipartUploadResult.getSSECustomerKeyMd5());

    UploadPartResult result = new UploadPartResult();
    result.setETag("etag");
    if (getSSECustomerKeyMd5(options) != null) {
      result.setSSECustomerKeyMd5(getSSECustomerKeyMd5(options));
    }
    doReturn(result).when(mockAmazonS3).uploadPart(any(UploadPartRequest.class));

    UploadPartResult mockUploadPartResult = mockAmazonS3.uploadPart(new UploadPartRequest());
    assertEquals(getSSECustomerKeyMd5(options), mockUploadPartResult.getSSECustomerKeyMd5());

    S3WritableByteChannel channel =
        new S3WritableByteChannel(mockAmazonS3, path, "text/plain", options);
    int contentSize = 34_078_720;
    ByteBuffer uploadContent = ByteBuffer.allocate((int) (contentSize * 2.5));
    for (int i = 0; i < contentSize; i++) {
      uploadContent.put((byte) 0xff);
    }
    uploadContent.flip();

    int uploadedSize = channel.write(uploadContent);
    assertEquals(contentSize, uploadedSize);

    CompleteMultipartUploadResult completeMultipartUploadResult =
        new CompleteMultipartUploadResult();
    doReturn(completeMultipartUploadResult)
        .when(mockAmazonS3)
        .completeMultipartUpload(any(CompleteMultipartUploadRequest.class));

    channel.close();

    verify(mockAmazonS3, times(2))
        .initiateMultipartUpload(notNull(InitiateMultipartUploadRequest.class));
    int partQuantity =
        (int) Math.ceil((double) contentSize / options.getS3UploadBufferSizeBytes()) + 1;
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

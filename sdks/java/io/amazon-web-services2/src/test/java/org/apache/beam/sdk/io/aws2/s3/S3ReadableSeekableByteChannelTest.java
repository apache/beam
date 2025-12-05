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

import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3Config;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3ConfigWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3ConfigWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3ConfigWithSSEKMSKeyId;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3Options;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSEAlgorithm;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSEKMSKeyId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.withSettings;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/** Tests {@link S3ReadableSeekableByteChannel}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class S3ReadableSeekableByteChannelTest {

  @FunctionalInterface
  public interface ChannelSupplier {
    S3ReadableSeekableByteChannel get() throws IOException;
  }

  private void readFromOptions(S3Options options) throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    ChannelSupplier channelSupplier =
        () ->
            new S3ReadableSeekableByteChannel(
                mockS3Client, path, S3FileSystemConfiguration.fromS3Options(options));
    read(
        mockS3Client,
        channelSupplier,
        path,
        options.getSSEAlgorithm(),
        options.getSSECustomerKey());
  }

  private void readFromConfig(S3FileSystemConfiguration config) throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    ChannelSupplier channelSupplier =
        () -> new S3ReadableSeekableByteChannel(mockS3Client, path, config);
    read(mockS3Client, channelSupplier, path, config.getSSEAlgorithm(), config.getSSECustomerKey());
  }

  @SuppressWarnings("unused")
  private void read(
      S3Client mockS3Client,
      ChannelSupplier channelSupplier,
      S3ResourceId path,
      String sseAlgorithm,
      SSECustomerKey sseCustomerKey)
      throws IOException {
    // Mock HeadObject response
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));

    // Mock GetObject response with a readable stream
    String sampleData = "Hello, Beam!";
    byte[] sampleDataBytes = sampleData.getBytes(StandardCharsets.UTF_8);
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(12L).build();

    doAnswer(
            invocation -> {
              GetObjectRequest request = invocation.getArgument(0);
              String range = request.range();
              byte[] dataToReturn;
              long contentLengthForResponse;

              if (range != null && range.startsWith("bytes=")) {
                // Use Guava Splitter for safe splitting
                List<String> rangeParts = Splitter.on('-').splitToList(range.substring(6));
                int start = Integer.parseInt(rangeParts.get(0));
                int end = Integer.parseInt(rangeParts.get(1));
                // Ensure end does not exceed array bounds
                end = Math.min(end, sampleDataBytes.length - 1);
                contentLengthForResponse = (long) end - start + 1;
                if (contentLengthForResponse <= 0) {
                  contentLengthForResponse = 0; // Handle invalid range
                  dataToReturn = new byte[0];
                } else {
                  dataToReturn = new byte[(int) contentLengthForResponse];
                  System.arraycopy(
                      sampleDataBytes, start, dataToReturn, 0, (int) contentLengthForResponse);
                }
              } else {
                contentLengthForResponse = sampleDataBytes.length;
                dataToReturn = sampleDataBytes;
              }

              GetObjectResponse responseWithAdjustedLength =
                  GetObjectResponse.builder().contentLength(contentLengthForResponse).build();
              return new ResponseInputStream<>(
                  responseWithAdjustedLength, new ByteArrayInputStream(dataToReturn));
            })
        .when(mockS3Client)
        .getObject(any(GetObjectRequest.class));

    S3ReadableSeekableByteChannel channel = channelSupplier.get();

    // Test initial read
    ByteBuffer buffer = ByteBuffer.allocate(sampleData.length());
    int bytesRead = channel.read(buffer);
    assertEquals(sampleData.length(), bytesRead);
    buffer.flip();
    assertEquals(sampleData, new String(buffer.array(), StandardCharsets.UTF_8));
    assertEquals(12L, channel.position());

    // Test seek and read
    channel.position(7); // Seek to "Beam"
    buffer = ByteBuffer.allocate(4);
    bytesRead = channel.read(buffer);
    assertEquals(4, bytesRead);
    buffer.flip();
    assertEquals("Beam", new String(buffer.array(), StandardCharsets.UTF_8));
    assertEquals(11L, channel.position());

    // Test reading the last byte and EOF
    channel.position(11L); // Seek to the last valid position ('!')
    buffer = ByteBuffer.allocate(1);
    bytesRead = channel.read(buffer);
    assertEquals(1, bytesRead); // Should read the last byte
    buffer.flip();
    assertEquals("!", new String(buffer.array(), StandardCharsets.UTF_8));
    assertEquals(12L, channel.position());

    // Now test EOF
    buffer = ByteBuffer.allocate(10);
    bytesRead = channel.read(buffer);
    assertEquals(-1, bytesRead); // EOF
    assertEquals(12L, channel.position());

    // Verify S3 interactions (adjusted to 2 calls)
    verify(mockS3Client, times(1)).headObject(any(HeadObjectRequest.class));
    verify(mockS3Client, times(2)).getObject(any(GetObjectRequest.class));
    verifyNoMoreInteractions(mockS3Client);

    // Test channel state
    assertTrue(channel.isOpen());
    channel.close();
    assertFalse(channel.isOpen());
  }

  @Test
  public void readWithS3Options() throws IOException {
    readFromOptions(s3Options());
    readFromOptions(s3OptionsWithSSEAlgorithm());
    readFromOptions(s3OptionsWithSSECustomerKey());
    readFromOptions(s3OptionsWithSSEKMSKeyId());
  }

  @Test
  public void readWithConfig() throws IOException {
    readFromConfig(s3Config("s3"));
    readFromConfig(s3ConfigWithSSEAlgorithm("s3"));
    readFromConfig(s3ConfigWithSSECustomerKey("s3"));
    readFromConfig(s3ConfigWithSSEKMSKeyId("s3"));
  }

  @Test
  public void testPosition() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertEquals(0, channel.position());
    channel.position(5);
    assertEquals(5, channel.position());
  }

  @Test
  public void testPositionClosedChannel() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    channel.close();
    assertThrows(ClosedChannelException.class, () -> channel.position());
  }

  @Test
  public void testPositionNegative() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertThrows(IllegalArgumentException.class, () -> channel.position(-1));
  }

  @Test
  public void testPositionBeyondSize() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertThrows(IllegalArgumentException.class, () -> channel.position(13));
  }

  @Test
  public void testSize() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertEquals(12L, channel.size());
  }

  @Test
  public void testSizeClosedChannel() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    channel.close();
    assertThrows(ClosedChannelException.class, () -> channel.size());
  }

  @Test
  public void testReadClosedChannel() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    channel.close();
    assertThrows(ClosedChannelException.class, () -> channel.read(ByteBuffer.allocate(10)));
  }

  @Test
  public void testWrite() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertThrows(NonWritableChannelException.class, () -> channel.write(ByteBuffer.allocate(10)));
  }

  @Test
  public void testTruncate() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));
    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    assertThrows(NonWritableChannelException.class, () -> channel.truncate(5));
  }

  @Test
  public void testIOExceptionOnS3ClientError() throws IOException {
    S3Client mockS3Client = mock(S3Client.class, withSettings().defaultAnswer(RETURNS_SMART_NULLS));
    S3ResourceId path = S3ResourceId.fromUri("s3://bucket/dir/file");
    HeadObjectResponse headResponse = HeadObjectResponse.builder().contentLength(12L).build();
    doReturn(headResponse).when(mockS3Client).headObject(any(HeadObjectRequest.class));

    // Mock the SdkClientException instead of instantiating it
    SdkClientException mockException = mock(SdkClientException.class);
    doReturn("S3 error").when(mockException).getMessage();
    doThrow(mockException).when(mockS3Client).getObject(any(GetObjectRequest.class));

    S3ReadableSeekableByteChannel channel =
        new S3ReadableSeekableByteChannel(mockS3Client, path, s3Config("s3"));

    ByteBuffer buffer = ByteBuffer.allocate(10);
    assertThrows(IOException.class, () -> channel.read(buffer));
  }
}

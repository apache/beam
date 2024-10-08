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

import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.buildMockedS3FileSystem;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3Config;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3ConfigWithPathStyleAccessEnabled;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3ConfigWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3Options;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithPathStyleAccessEnabled;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.s3OptionsWithSSECustomerKey;
import static org.apache.beam.sdk.io.aws2.s3.S3TestUtils.toMd5;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import akka.http.scaladsl.Http;
import io.findify.s3mock.S3Mock;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.aws2.options.S3Options;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectResponse;
import software.amazon.awssdk.services.s3.model.CopyPartResult;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.UploadPartCopyRequest;
import software.amazon.awssdk.services.s3.model.UploadPartCopyResponse;

/** Test case for {@link S3FileSystem}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class S3FileSystemTest {

  private static S3Mock api;
  private static S3Client client;

  @BeforeClass
  public static void beforeClass() {
    api = new S3Mock.Builder().withInMemoryBackend().withPort(8002).build();
    Http.ServerBinding binding = api.start();

    URI endpoint = URI.create("http://localhost:" + binding.localAddress().getPort());
    S3Configuration s3Configuration =
        S3Configuration.builder().pathStyleAccessEnabled(true).build();
    client =
        S3Client.builder()
            .region(Region.US_WEST_1)
            .serviceConfiguration(s3Configuration)
            .endpointOverride(endpoint)
            .credentialsProvider(AnonymousCredentialsProvider.create())
            .build();
  }

  @AfterClass
  public static void afterClass() {
    api.stop();
  }

  @Test
  public void testGetScheme() {
    S3FileSystem s3FileSystem = new S3FileSystem(s3Config("s3"));
    assertEquals("s3", s3FileSystem.getScheme());

    s3FileSystem = new S3FileSystem(s3Config("other"));
    assertEquals("other", s3FileSystem.getScheme());
  }

  @Test
  public void testGetSchemeWithS3Options() {
    S3FileSystem s3FileSystem = new S3FileSystem(s3Options());
    assertEquals("s3", s3FileSystem.getScheme());
  }

  @Test
  public void testGetPathStyleAccessEnabled() throws URISyntaxException {
    S3FileSystem s3FileSystem = new S3FileSystem(s3ConfigWithPathStyleAccessEnabled("s3"));
    URL s3Url =
        s3FileSystem
            .getS3Client()
            .utilities()
            .getUrl(GetUrlRequest.builder().bucket("bucket").key("file").build());
    assertEquals("https://s3.us-west-1.amazonaws.com/bucket/file", s3Url.toURI().toString());
  }

  @Test
  public void testGetPathStyleAccessEnabledWithS3Options() throws URISyntaxException {
    S3FileSystem s3FileSystem = new S3FileSystem(s3OptionsWithPathStyleAccessEnabled());
    URL s3Url =
        s3FileSystem
            .getS3Client()
            .utilities()
            .getUrl(GetUrlRequest.builder().bucket("bucket").key("file").build());
    assertEquals("https://s3.us-west-1.amazonaws.com/bucket/file", s3Url.toURI().toString());
  }

  @Test
  public void testCopy() throws IOException {
    testCopy(s3Config("s3"));
    testCopy(s3Config("other"));
    testCopy(s3ConfigWithSSECustomerKey("s3"));
    testCopy(s3ConfigWithSSECustomerKey("other"));
  }

  @Test
  public void testCopyWithS3Options() throws IOException {
    testCopy(s3Options());
    testCopy(s3OptionsWithSSECustomerKey());
  }

  private HeadObjectRequest createObjectHeadRequest(
      S3ResourceId path, SSECustomerKey sseCustomerKey) {
    return HeadObjectRequest.builder()
        .bucket(path.getBucket())
        .key(path.getKey())
        .sseCustomerKey(sseCustomerKey.getKey())
        .sseCustomerAlgorithm(sseCustomerKey.getAlgorithm())
        .build();
  }

  private void assertGetObjectHead(
      S3FileSystem s3FileSystem,
      HeadObjectRequest request,
      String sseCustomerKeyMd5,
      HeadObjectResponse objectMetadata) {
    when(s3FileSystem.getS3Client().headObject(argThat(new GetHeadObjectRequestMatcher(request))))
        .thenReturn(objectMetadata);
    assertEquals(
        sseCustomerKeyMd5, s3FileSystem.getS3Client().headObject(request).sseCustomerKeyMD5());
  }

  private void testCopy(S3FileSystemConfiguration config) throws IOException {
    testCopy(buildMockedS3FileSystem(config), config.getSSECustomerKey());
  }

  private void testCopy(S3Options options) throws IOException {
    testCopy(buildMockedS3FileSystem(options), options.getSSECustomerKey());
  }

  private void testCopy(S3FileSystem s3FileSystem, SSECustomerKey sseCustomerKey)
      throws IOException {
    S3ResourceId sourcePath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/from");
    S3ResourceId destinationPath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/to");

    HeadObjectResponse.Builder builder = HeadObjectResponse.builder().contentLength(0L);

    String sseCustomerKeyMd5 = toMd5(sseCustomerKey);
    if (sseCustomerKeyMd5 != null) {
      builder.sseCustomerKeyMD5(sseCustomerKeyMd5);
    }
    HeadObjectResponse headObjectResponse = builder.build();
    assertGetObjectHead(
        s3FileSystem,
        createObjectHeadRequest(sourcePath, sseCustomerKey),
        sseCustomerKeyMd5,
        headObjectResponse);

    s3FileSystem.copy(sourcePath, destinationPath);

    verify(s3FileSystem.getS3Client(), times(1)).copyObject(any(CopyObjectRequest.class));

    // we simulate a big object >= 5GB so it takes the multiPart path
    HeadObjectResponse bigHeadObjectResponse =
        headObjectResponse.toBuilder().contentLength(5_368_709_120L).build();
    assertGetObjectHead(
        s3FileSystem,
        createObjectHeadRequest(sourcePath, sseCustomerKey),
        sseCustomerKeyMd5,
        bigHeadObjectResponse);

    try {
      s3FileSystem.copy(sourcePath, destinationPath);
    } catch (NullPointerException e) {
      // ignore failing unmocked path, this is covered by testMultipartCopy test
    }

    verify(s3FileSystem.getS3Client(), never()).copyObject((CopyObjectRequest) null);
  }

  @Test
  public void testAtomicCopy() throws IOException {
    testAtomicCopy(s3Config("s3"));
    testAtomicCopy(s3Config("other"));
    testAtomicCopy(s3ConfigWithSSECustomerKey("s3"));
    testAtomicCopy(s3ConfigWithSSECustomerKey("other"));
  }

  @Test
  public void testAtomicCopyWithS3Options() throws IOException {
    testAtomicCopy(s3Options());
    testAtomicCopy(s3OptionsWithSSECustomerKey());
  }

  private void testAtomicCopy(S3FileSystemConfiguration config) throws IOException {
    testAtomicCopy(buildMockedS3FileSystem(config), config.getSSECustomerKey());
  }

  private void testAtomicCopy(S3Options options) throws IOException {
    testAtomicCopy(buildMockedS3FileSystem(options), options.getSSECustomerKey());
  }

  private void testAtomicCopy(S3FileSystem s3FileSystem, SSECustomerKey sseCustomerKey)
      throws IOException {
    S3ResourceId sourcePath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/from");
    S3ResourceId destinationPath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/to");

    CopyObjectResponse.Builder builder = CopyObjectResponse.builder();
    String sseCustomerKeyMd5 = toMd5(sseCustomerKey);
    if (sseCustomerKeyMd5 != null) {
      builder.sseCustomerKeyMD5(sseCustomerKeyMd5);
    }
    CopyObjectResponse copyObjectResponse = builder.build();
    CopyObjectRequest copyObjectRequest =
        CopyObjectRequest.builder()
            .sourceBucket(sourcePath.getBucket())
            .sourceKey(sourcePath.getKey())
            .destinationBucket(destinationPath.getBucket())
            .destinationBucket(destinationPath.getKey())
            .sseCustomerKey(sseCustomerKey.getKey())
            .copySourceSSECustomerAlgorithm(sseCustomerKey.getAlgorithm())
            .build();
    when(s3FileSystem.getS3Client().copyObject(any(CopyObjectRequest.class)))
        .thenReturn(copyObjectResponse);
    assertEquals(
        sseCustomerKeyMd5,
        s3FileSystem.getS3Client().copyObject(copyObjectRequest).sseCustomerKeyMD5());

    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder().build();
    s3FileSystem.atomicCopy(sourcePath, destinationPath, headObjectResponse);

    verify(s3FileSystem.getS3Client(), times(2)).copyObject(any(CopyObjectRequest.class));
  }

  @Test
  public void testMultipartCopy() throws IOException {
    testMultipartCopy(s3Config("s3"));
    testMultipartCopy(s3Config("other"));
    testMultipartCopy(s3ConfigWithSSECustomerKey("s3"));
    testMultipartCopy(s3ConfigWithSSECustomerKey("other"));
  }

  @Test
  public void testMultipartCopyWithS3Options() throws IOException {
    testMultipartCopy(s3Options());
    testMultipartCopy(s3OptionsWithSSECustomerKey());
  }

  private void testMultipartCopy(S3FileSystemConfiguration config) throws IOException {
    testMultipartCopy(buildMockedS3FileSystem(config), config.getSSECustomerKey());
  }

  private void testMultipartCopy(S3Options options) throws IOException {
    testMultipartCopy(buildMockedS3FileSystem(options), options.getSSECustomerKey());
  }

  private void testMultipartCopy(S3FileSystem s3FileSystem, SSECustomerKey sseCustomerKey)
      throws IOException {
    S3ResourceId sourcePath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/from");
    S3ResourceId destinationPath = S3ResourceId.fromUri(s3FileSystem.getScheme() + "://bucket/to");

    CreateMultipartUploadResponse.Builder builder =
        CreateMultipartUploadResponse.builder().uploadId("upload-id");
    String sseCustomerKeyMd5 = toMd5(sseCustomerKey);
    if (sseCustomerKeyMd5 != null) {
      builder.sseCustomerKeyMD5(sseCustomerKeyMd5);
    }
    CreateMultipartUploadResponse createMultipartUploadResponse = builder.build();
    when(s3FileSystem.getS3Client().createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(createMultipartUploadResponse);
    assertEquals(
        sseCustomerKeyMd5,
        s3FileSystem
            .getS3Client()
            .createMultipartUpload(
                CreateMultipartUploadRequest.builder()
                    .bucket(destinationPath.getBucket())
                    .key(destinationPath.getKey())
                    .build())
            .sseCustomerKeyMD5());

    HeadObjectResponse.Builder headObjectResponseBuilder =
        HeadObjectResponse.builder()
            .contentLength((long) (S3FileSystem.MAX_COPY_OBJECT_SIZE_BYTES * 1.5))
            .contentEncoding("read-seek-efficient");
    if (sseCustomerKeyMd5 != null) {
      headObjectResponseBuilder.sseCustomerKeyMD5(sseCustomerKeyMd5);
    }
    HeadObjectResponse headObjectResponse = headObjectResponseBuilder.build();
    assertGetObjectHead(
        s3FileSystem,
        createObjectHeadRequest(sourcePath, sseCustomerKey),
        sseCustomerKeyMd5,
        headObjectResponse);

    UploadPartCopyRequest uploadPartCopyRequest1 =
        UploadPartCopyRequest.builder()
            .destinationBucket(destinationPath.getBucket())
            .destinationKey(destinationPath.getKey())
            .sourceBucket(sourcePath.getBucket())
            .sourceKey(sourcePath.getKey())
            .uploadId(createMultipartUploadResponse.uploadId())
            .partNumber(1)
            .copySourceRange(
                String.format("bytes=0-%s", S3FileSystem.MAX_COPY_OBJECT_SIZE_BYTES - 1))
            .sseCustomerKey(sseCustomerKey == null ? null : sseCustomerKey.getKey())
            .sseCustomerAlgorithm(sseCustomerKey == null ? null : sseCustomerKey.getAlgorithm())
            .sseCustomerKeyMD5(sseCustomerKeyMd5)
            .copySourceSSECustomerKey(sseCustomerKey == null ? null : sseCustomerKey.getKey())
            .copySourceSSECustomerAlgorithm(
                sseCustomerKey == null ? null : sseCustomerKey.getAlgorithm())
            .copySourceSSECustomerKeyMD5(sseCustomerKeyMd5)
            .build();

    UploadPartCopyRequest uploadPartCopyRequest2 =
        uploadPartCopyRequest1
            .toBuilder()
            .partNumber(2)
            .copySourceRange(
                String.format(
                    "bytes=%s-%s",
                    S3FileSystem.MAX_COPY_OBJECT_SIZE_BYTES,
                    headObjectResponse.contentLength() - 1))
            .build();

    CopyPartResult copyPartResult1 = CopyPartResult.builder().eTag("etag-1").build();
    CopyPartResult copyPartResult2 = CopyPartResult.builder().eTag("etag-2").build();
    UploadPartCopyResponse.Builder uploadPartCopyResponseBuilder1 =
        UploadPartCopyResponse.builder().copyPartResult(copyPartResult1);
    UploadPartCopyResponse.Builder uploadPartCopyResponseBuilder2 =
        UploadPartCopyResponse.builder().copyPartResult(copyPartResult2);
    if (sseCustomerKeyMd5 != null) {
      uploadPartCopyResponseBuilder1.sseCustomerKeyMD5(sseCustomerKeyMd5);
      uploadPartCopyResponseBuilder2.sseCustomerKeyMD5(sseCustomerKeyMd5);
    }
    UploadPartCopyResponse uploadPartCopyResponse1 = uploadPartCopyResponseBuilder1.build();
    UploadPartCopyResponse uploadPartCopyResponse2 = uploadPartCopyResponseBuilder2.build();
    when(s3FileSystem.getS3Client().uploadPartCopy(eq(uploadPartCopyRequest1)))
        .thenReturn(uploadPartCopyResponse1);
    when(s3FileSystem.getS3Client().uploadPartCopy(eq(uploadPartCopyRequest2)))
        .thenReturn(uploadPartCopyResponse2);

    s3FileSystem.multipartCopy(sourcePath, destinationPath, headObjectResponse);

    verify(s3FileSystem.getS3Client(), times(1))
        .completeMultipartUpload(
            eq(
                CompleteMultipartUploadRequest.builder()
                    .bucket(destinationPath.getBucket())
                    .key(destinationPath.getKey())
                    .uploadId(createMultipartUploadResponse.uploadId())
                    .multipartUpload(
                        CompletedMultipartUpload.builder()
                            .parts(
                                CompletedPart.builder()
                                    .partNumber(1)
                                    .eTag(copyPartResult1.eTag())
                                    .build(),
                                CompletedPart.builder()
                                    .partNumber(2)
                                    .eTag(copyPartResult2.eTag())
                                    .build())
                            .build())
                    .build()));
  }

  @Test
  public void deleteThousandsOfObjectsInMultipleBuckets() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("s3"));

    List<String> buckets = ImmutableList.of("bucket1", "bucket2");
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 2500; i++) {
      keys.add(String.format("key-%d", i));
    }
    List<S3ResourceId> paths = new ArrayList<>();
    for (String bucket : buckets) {
      for (String key : keys) {
        paths.add(S3ResourceId.fromComponents("s3", bucket, key));
      }
    }

    s3FileSystem.delete(paths);

    // Should require 6 calls to delete 2500 objects in each of 2 buckets.
    verify(s3FileSystem.getS3Client(), times(6)).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  public void deleteThousandsOfObjectsInMultipleBucketsWithS3Options() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    List<String> buckets = ImmutableList.of("bucket1", "bucket2");
    List<String> keys = new ArrayList<>();
    for (int i = 0; i < 2500; i++) {
      keys.add(String.format("key-%d", i));
    }
    List<S3ResourceId> paths = new ArrayList<>();
    for (String bucket : buckets) {
      for (String key : keys) {
        paths.add(S3ResourceId.fromComponents("s3", bucket, key));
      }
    }

    s3FileSystem.delete(paths);

    // Should require 6 calls to delete 2500 objects in each of 2 buckets.
    verify(s3FileSystem.getS3Client(), times(6)).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  public void matchNonGlob() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"));

    S3ResourceId path = S3ResourceId.fromUri("mys3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .contentEncoding("read-seek-efficient")
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(true)
                    .build())));
  }

  @Test
  public void matchNonGlobWithS3Options() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .contentEncoding("read-seek-efficient")
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(true)
                    .build())));
  }

  @Test
  public void matchNonGlobNotReadSeekEfficient() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("s3"));

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .contentEncoding("gzip")
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(false)
                    .build())));
  }

  @Test
  public void matchNonGlobNotReadSeekEfficientWithS3Options() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .contentEncoding("gzip")
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(false)
                    .build())));
  }

  @Test
  public void matchNonGlobNullContentEncoding() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("s3"));

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .contentEncoding(null)
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(true)
                    .build())));
  }

  @Test
  public void matchNonGlobNullContentEncodingWithOptions() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/filethatexists");
    long lastModifiedMillis = 1540000000000L;
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .lastModified(Instant.ofEpochMilli(lastModifiedMillis))
            .contentEncoding(null)
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenReturn(headObjectResponse);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setSizeBytes(100)
                    .setLastModifiedMillis(lastModifiedMillis)
                    .setResourceId(path)
                    .setIsReadSeekEfficient(true)
                    .build())));
  }

  @Test
  public void matchNonGlobNotFound() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("s3"));

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/nonexistentfile");
    SdkServiceException exception =
        S3Exception.builder().message("mock exception").statusCode(404).build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenThrow(exception);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException()));
  }

  @Test
  public void matchNonGlobNotFoundWithS3Options() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/nonexistentfile");
    SdkServiceException exception =
        S3Exception.builder().message("mock exception").statusCode(404).build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenThrow(exception);

    MatchResult result = s3FileSystem.matchNonGlobPath(path);
    assertThat(
        result,
        MatchResultMatcher.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException()));
  }

  @Test
  public void matchNonGlobForbidden() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("s3"));

    SdkServiceException exception =
        S3Exception.builder().message("mock exception").statusCode(403).build();
    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/keyname");
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenThrow(exception);

    assertThat(
        s3FileSystem.matchNonGlobPath(path),
        MatchResultMatcher.create(MatchResult.Status.ERROR, new IOException(exception)));
  }

  @Test
  public void matchNonGlobForbiddenWithS3Options() {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Options());

    SdkServiceException exception =
        S3Exception.builder().message("mock exception").statusCode(403).build();
    S3ResourceId path = S3ResourceId.fromUri("s3://testbucket/testdirectory/keyname");
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(path.getBucket())
                            .key(path.getKey())
                            .build()))))
        .thenThrow(exception);

    assertThat(
        s3FileSystem.matchNonGlobPath(path),
        MatchResultMatcher.create(MatchResult.Status.ERROR, new IOException(exception)));
  }

  static class ListObjectsV2RequestArgumentMatches
      implements ArgumentMatcher<ListObjectsV2Request> {

    private final ListObjectsV2Request expected;

    ListObjectsV2RequestArgumentMatches(ListObjectsV2Request expected) {
      this.expected = checkNotNull(expected);
    }

    @Override
    public boolean matches(ListObjectsV2Request argument) {
      if (argument != null) {
        return expected.bucket().equals(argument.bucket())
            && expected.prefix().equals(argument.prefix())
            && (expected.continuationToken() == null
                ? argument.continuationToken() == null
                : expected.continuationToken().equals(argument.continuationToken()));
      }
      return false;
    }
  }

  @Test
  public void matchGlob() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"));

    S3ResourceId path = S3ResourceId.fromUri("mys3://testbucket/foo/bar*baz");

    ListObjectsV2Request firstRequest =
        ListObjectsV2Request.builder()
            .bucket(path.getBucket())
            .prefix(path.getKeyNonWildcardPrefix())
            .continuationToken(null)
            .build();

    // Expected to be returned; prefix and wildcard/regex match
    S3Object firstMatch =
        S3Object.builder()
            .key("foo/bar0baz")
            .size(100L)
            .lastModified(Instant.ofEpochMilli(1540000000001L))
            .build();

    // Expected to not be returned; prefix matches, but substring after wildcard does not
    S3Object secondMatch =
        S3Object.builder()
            .key("foo/bar1qux")
            .size(200L)
            .lastModified(Instant.ofEpochMilli(1540000000002L))
            .build();

    // Expected first request returns continuation token
    ListObjectsV2Response firstResponse =
        ListObjectsV2Response.builder()
            .nextContinuationToken("token")
            .contents(firstMatch, secondMatch)
            .build();
    when(s3FileSystem
            .getS3Client()
            .listObjectsV2(argThat(new ListObjectsV2RequestArgumentMatches(firstRequest))))
        .thenReturn(firstResponse);

    // Expect second request with continuation token
    ListObjectsV2Request secondRequest =
        ListObjectsV2Request.builder()
            .bucket(path.getBucket())
            .prefix(path.getKeyNonWildcardPrefix())
            .continuationToken("token")
            .build();

    // Expected to be returned; prefix and wildcard/regex match
    S3Object thirdMatch =
        S3Object.builder()
            .key("foo/bar2baz")
            .size(300L)
            .lastModified(Instant.ofEpochMilli(1540000000003L))
            .build();

    // Expected second request returns third prefix match and no continuation token
    ListObjectsV2Response secondResponse =
        ListObjectsV2Response.builder().nextContinuationToken(null).contents(thirdMatch).build();
    when(s3FileSystem
            .getS3Client()
            .listObjectsV2(argThat(new ListObjectsV2RequestArgumentMatches(secondRequest))))
        .thenReturn(secondResponse);

    // Expect object metadata queries for content encoding
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder().contentEncoding("").build();
    when(s3FileSystem.getS3Client().headObject(any(HeadObjectRequest.class)))
        .thenReturn(headObjectResponse);

    assertThat(
        s3FileSystem.matchGlobPaths(ImmutableList.of(path)).get(0),
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setIsReadSeekEfficient(true)
                    .setResourceId(
                        S3ResourceId.fromComponents("mys3", path.getBucket(), firstMatch.key()))
                    .setSizeBytes(firstMatch.size())
                    .setLastModifiedMillis(firstMatch.lastModified().toEpochMilli())
                    .build(),
                MatchResult.Metadata.builder()
                    .setIsReadSeekEfficient(true)
                    .setResourceId(
                        S3ResourceId.fromComponents("mys3", path.getBucket(), thirdMatch.key()))
                    .setSizeBytes(thirdMatch.size())
                    .setLastModifiedMillis(thirdMatch.lastModified().toEpochMilli())
                    .build())));
  }

  @Test
  public void matchGlobWithSlashes() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"));

    S3ResourceId path = S3ResourceId.fromUri("mys3://testbucket/foo/bar\\baz*");

    ListObjectsV2Request request =
        ListObjectsV2Request.builder()
            .bucket(path.getBucket())
            .prefix(path.getKeyNonWildcardPrefix())
            .continuationToken(null)
            .build();

    // Expected to be returned; prefix and wildcard/regex match
    S3Object firstMatch =
        S3Object.builder()
            .key("foo/bar\\baz0")
            .size(100L)
            .lastModified(Instant.ofEpochMilli(1540000000001L))
            .build();

    // Expected to not be returned; prefix matches, but substring after wildcard does not
    S3Object secondMatch =
        S3Object.builder()
            .key("foo/bar/baz1")
            .size(200L)
            .lastModified(Instant.ofEpochMilli(1540000000002L))
            .build();

    // Expected first request returns continuation token
    ListObjectsV2Response response =
        ListObjectsV2Response.builder().contents(firstMatch, secondMatch).build();
    when(s3FileSystem
            .getS3Client()
            .listObjectsV2(argThat(new ListObjectsV2RequestArgumentMatches(request))))
        .thenReturn(response);

    // Expect object metadata queries for content encoding
    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder().contentEncoding("").build();
    when(s3FileSystem.getS3Client().headObject(any(HeadObjectRequest.class)))
        .thenReturn(headObjectResponse);

    assertThat(
        s3FileSystem.matchGlobPaths(ImmutableList.of(path)).get(0),
        MatchResultMatcher.create(
            ImmutableList.of(
                MatchResult.Metadata.builder()
                    .setIsReadSeekEfficient(true)
                    .setResourceId(
                        S3ResourceId.fromComponents("mys3", path.getBucket(), firstMatch.key()))
                    .setSizeBytes(firstMatch.size())
                    .setLastModifiedMillis(firstMatch.lastModified().toEpochMilli())
                    .build())));
  }

  @Test
  public void matchVariousInvokeThreadPool() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"));
    SdkServiceException notFoundException =
        S3Exception.builder().message("mock exception").statusCode(404).build();
    S3ResourceId pathNotExist =
        S3ResourceId.fromUri("mys3://testbucket/testdirectory/nonexistentfile");
    HeadObjectRequest headObjectRequestNotExist =
        HeadObjectRequest.builder()
            .bucket(pathNotExist.getBucket())
            .key(pathNotExist.getKey())
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(argThat(new GetHeadObjectRequestMatcher(headObjectRequestNotExist))))
        .thenThrow(notFoundException);

    SdkServiceException forbiddenException =
        SdkServiceException.builder().message("mock exception").statusCode(403).build();
    S3ResourceId pathForbidden =
        S3ResourceId.fromUri("mys3://testbucket/testdirectory/forbiddenfile");
    HeadObjectRequest headObjectRequestForbidden =
        HeadObjectRequest.builder()
            .bucket(pathForbidden.getBucket())
            .key(pathForbidden.getKey())
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(argThat(new GetHeadObjectRequestMatcher(headObjectRequestForbidden))))
        .thenThrow(forbiddenException);

    S3ResourceId pathExist = S3ResourceId.fromUri("mys3://testbucket/testdirectory/filethatexists");
    HeadObjectRequest headObjectRequestExist =
        HeadObjectRequest.builder().bucket(pathExist.getBucket()).key(pathExist.getKey()).build();
    HeadObjectResponse s3ObjectMetadata =
        HeadObjectResponse.builder()
            .contentLength(100L)
            .contentEncoding("not-gzip")
            .lastModified(Instant.ofEpochMilli(1540000000000L))
            .build();
    when(s3FileSystem
            .getS3Client()
            .headObject(argThat(new GetHeadObjectRequestMatcher(headObjectRequestExist))))
        .thenReturn(s3ObjectMetadata);

    S3ResourceId pathGlob = S3ResourceId.fromUri("mys3://testbucket/path/part*");

    S3Object foundListObject =
        S3Object.builder()
            .key("path/part-0")
            .size(200L)
            .lastModified(Instant.ofEpochMilli(1541000000000L))
            .build();

    ListObjectsV2Response listObjectsResponse =
        ListObjectsV2Response.builder().continuationToken(null).contents(foundListObject).build();
    when(s3FileSystem.getS3Client().listObjectsV2((ListObjectsV2Request) notNull()))
        .thenReturn(listObjectsResponse);

    HeadObjectResponse headObjectResponse =
        HeadObjectResponse.builder().contentEncoding("").build();
    when(s3FileSystem
            .getS3Client()
            .headObject(
                argThat(
                    new GetHeadObjectRequestMatcher(
                        HeadObjectRequest.builder()
                            .bucket(pathGlob.getBucket())
                            .key("path/part-0")
                            .build()))))
        .thenReturn(headObjectResponse);

    assertThat(
        s3FileSystem.match(
            ImmutableList.of(
                pathNotExist.toString(),
                pathForbidden.toString(),
                pathExist.toString(),
                pathGlob.toString())),
        contains(
            MatchResultMatcher.create(MatchResult.Status.NOT_FOUND, new FileNotFoundException()),
            MatchResultMatcher.create(
                MatchResult.Status.ERROR, new IOException(forbiddenException)),
            MatchResultMatcher.create(100, 1540000000000L, pathExist, true),
            MatchResultMatcher.create(
                200,
                1541000000000L,
                S3ResourceId.fromComponents("mys3", pathGlob.getBucket(), foundListObject.key()),
                true)));
  }

  @Test
  public void testWriteAndRead() throws IOException {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"), client);

    client.createBucket(CreateBucketRequest.builder().bucket("testbucket").build());

    byte[] writtenArray = new byte[] {0};
    ByteBuffer bb = ByteBuffer.allocate(writtenArray.length);
    bb.put(writtenArray);

    // First create an object and write data to it
    S3ResourceId path = S3ResourceId.fromUri("mys3://testbucket/foo/bar.txt");
    WritableByteChannel writableByteChannel =
        s3FileSystem.create(
            path,
            CreateOptions.StandardCreateOptions.builder().setMimeType("application/text").build());
    writableByteChannel.write(bb);
    writableByteChannel.close();

    // Now read the same object
    ByteBuffer bb2 = ByteBuffer.allocate(writtenArray.length);
    ReadableByteChannel open = s3FileSystem.open(path);
    open.read(bb2);

    // And compare the content with the one that was written
    byte[] readArray = bb2.array();
    assertArrayEquals(readArray, writtenArray);
    open.close();
  }

  @Test
  public void testReportLineageOnBucket() {
    verifyLineage("s3://testbucket", ImmutableList.of("testbucket"));
    verifyLineage("s3://testbucket/", ImmutableList.of("testbucket"));
    verifyLineage("s3://testbucket/foo/bar.txt", ImmutableList.of("testbucket", "foo/bar.txt"));
  }

  private void verifyLineage(String uri, List<String> expected) {
    S3FileSystem s3FileSystem = buildMockedS3FileSystem(s3Config("mys3"), client);
    S3ResourceId path = S3ResourceId.fromUri(uri);
    Lineage mockLineage = mock(Lineage.class);
    s3FileSystem.reportLineage(path, mockLineage);
    verify(mockLineage, times(1)).add("s3", expected);
  }

  /** A mockito argument matcher to implement equality on GetHeadObjectRequest. */
  private static class GetHeadObjectRequestMatcher implements ArgumentMatcher<HeadObjectRequest> {

    private final HeadObjectRequest expected;

    GetHeadObjectRequestMatcher(HeadObjectRequest expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(HeadObjectRequest obj) {
      if (obj == null) {
        return false;
      }
      return obj.bucket().equals(expected.bucket()) && obj.key().equals(expected.key());
    }
  }
}

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
package org.apache.beam.sdk.extensions.gcp.util;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.RewriteOp;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Test case for {@link GcsUtil}. */
@RunWith(JUnit4.class)
public class GcsUtilTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static GcsOptions gcsOptionsWithTestCredential() {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGcpCredential(new TestCredential());
    return pipelineOptions;
  }

  @Test
  public void testCreationWithDefaultOptions() {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    assertNotNull(pipelineOptions.getGcpCredential());
  }

  @Test
  public void testUploadBufferSizeDefault() {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil util = pipelineOptions.getGcsUtil();
    assertNull(util.getUploadBufferSizeBytes());
  }

  @Test
  public void testUploadBufferSizeUserSpecified() {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    pipelineOptions.setGcsUploadBufferSizeBytes(12345);
    GcsUtil util = pipelineOptions.getGcsUtil();
    assertEquals((Integer) 12345, util.getUploadBufferSizeBytes());
  }

  @Test
  public void testCreationWithExecutorServiceProvided() {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    pipelineOptions.setExecutorService(Executors.newCachedThreadPool());
    assertSame(pipelineOptions.getExecutorService(), pipelineOptions.getGcsUtil().executorService);
  }

  @Test
  public void testCreationWithGcsUtilProvided() {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    GcsUtil gcsUtil = Mockito.mock(GcsUtil.class);
    pipelineOptions.setGcsUtil(gcsUtil);
    assertSame(gcsUtil, pipelineOptions.getGcsUtil());
  }

  @Test
  public void testMultipleThreadsCanCompleteOutOfOrderWithDefaultThreadPool() throws Exception {
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    ExecutorService executorService = pipelineOptions.getExecutorService();

    int numThreads = 100;
    final CountDownLatch[] countDownLatches = new CountDownLatch[numThreads];
    for (int i = 0; i < numThreads; i++) {
      final int currentLatch = i;
      countDownLatches[i] = new CountDownLatch(1);
      executorService.execute(
          () -> {
            // Wait for latch N and then release latch N - 1
            try {
              countDownLatches[currentLatch].await();
              if (currentLatch > 0) {
                countDownLatches[currentLatch - 1].countDown();
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          });
    }

    // Release the last latch starting the chain reaction.
    countDownLatches[countDownLatches.length - 1].countDown();
    executorService.shutdown();
    assertTrue(
        "Expected tasks to complete", executorService.awaitTermination(10, TimeUnit.SECONDS));
  }

  @Test
  public void testGlobExpansion() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);
    Storage.Objects.List mockStorageList = Mockito.mock(Storage.Objects.List.class);

    Objects modelObjects = new Objects();
    List<StorageObject> items = new ArrayList<>();
    // A directory
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/"));

    // Files within the directory
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/file1name"));
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/file2name"));
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/file3name"));
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/otherfile"));
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/anotherfile"));

    modelObjects.setItems(items);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "testdirectory/otherfile"))
        .thenReturn(mockStorageGet);
    when(mockStorageObjects.list("testbucket")).thenReturn(mockStorageList);
    when(mockStorageGet.execute())
        .thenReturn(new StorageObject().setBucket("testbucket").setName("testdirectory/otherfile"));
    when(mockStorageList.execute()).thenReturn(modelObjects);

    // Test a single file.
    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/otherfile");
      List<GcsPath> expectedFiles =
          ImmutableList.of(GcsPath.fromUri("gs://testbucket/testdirectory/otherfile"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    // Test patterns.
    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file*");
      List<GcsPath> expectedFiles =
          ImmutableList.of(
              GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file[1-3]*");
      List<GcsPath> expectedFiles =
          ImmutableList.of(
              GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file?name");
      List<GcsPath> expectedFiles =
          ImmutableList.of(
              GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/test*ectory/fi*name");
      List<GcsPath> expectedFiles =
          ImmutableList.of(
              GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
              GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }
  }

  @Test
  public void testRecursiveGlobExpansion() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);
    Storage.Objects.List mockStorageList = Mockito.mock(Storage.Objects.List.class);

    Objects modelObjects = new Objects();
    List<StorageObject> items = new ArrayList<>();
    // A directory
    items.add(new StorageObject().setBucket("testbucket").setName("testdirectory/"));

    // Files within the directory
    items.add(new StorageObject().setBucket("testbucket").setName("test/directory/file1.txt"));
    items.add(new StorageObject().setBucket("testbucket").setName("test/directory/file2.txt"));
    items.add(new StorageObject().setBucket("testbucket").setName("test/directory/file3.txt"));
    items.add(new StorageObject().setBucket("testbucket").setName("test/directory/otherfile"));
    items.add(new StorageObject().setBucket("testbucket").setName("test/directory/anotherfile"));
    items.add(new StorageObject().setBucket("testbucket").setName("test/file4.txt"));

    modelObjects.setItems(items);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "test/directory/otherfile"))
        .thenReturn(mockStorageGet);
    when(mockStorageObjects.list("testbucket")).thenReturn(mockStorageList);
    when(mockStorageGet.execute())
        .thenReturn(
            new StorageObject().setBucket("testbucket").setName("test/directory/otherfile"));
    when(mockStorageList.execute()).thenReturn(modelObjects);

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/test/**/*.txt");
      List<GcsPath> expectedFiles =
          ImmutableList.of(
              GcsPath.fromUri("gs://testbucket/test/directory/file1.txt"),
              GcsPath.fromUri("gs://testbucket/test/directory/file2.txt"),
              GcsPath.fromUri("gs://testbucket/test/directory/file3.txt"),
              GcsPath.fromUri("gs://testbucket/test/file4.txt"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }
  }

  // GCSUtil.expand() should fail when matching a single object when that object does not exist.
  // We should return the empty result since GCS get object is strongly consistent.
  @Test
  public void testNonExistentObjectReturnsEmptyResult() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/nonexistentfile");
    GoogleJsonResponseException expectedException =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_NOT_FOUND, "It don't exist", "Nothing here to see");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(pattern.getBucket(), pattern.getObject()))
        .thenReturn(mockStorageGet);
    when(mockStorageGet.execute()).thenThrow(expectedException);

    assertEquals(Collections.emptyList(), gcsUtil.expand(pattern));
  }

  // GCSUtil.expand() should fail for other errors such as access denied.
  @Test
  public void testAccessDeniedObjectThrowsIOException() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/accessdeniedfile");
    GoogleJsonResponseException expectedException =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_FORBIDDEN,
            "Waves hand mysteriously",
            "These aren't the buckets you're looking for");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(pattern.getBucket(), pattern.getObject()))
        .thenReturn(mockStorageGet);
    when(mockStorageGet.execute()).thenThrow(expectedException);

    thrown.expect(IOException.class);
    thrown.expectMessage("Unable to get the file object for path");
    gcsUtil.expand(pattern);
  }

  @Test
  public void testFileSizeNonBatch() throws Exception {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "testobject")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenReturn(new StorageObject().setSize(BigInteger.valueOf(1000)));

    assertEquals(1000, gcsUtil.fileSize(GcsPath.fromComponents("testbucket", "testobject")));
  }

  @Test
  public void testFileSizeWhenFileNotFoundNonBatch() throws Exception {
    MockLowLevelHttpResponse notFoundResponse = new MockLowLevelHttpResponse();
    notFoundResponse.setContent("");
    notFoundResponse.setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);

    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder().setLowLevelHttpResponse(notFoundResponse).build();

    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    gcsUtil.setStorageClient(new Storage(mockTransport, Transport.getJsonFactory(), null));

    thrown.expect(FileNotFoundException.class);
    gcsUtil.fileSize(GcsPath.fromComponents("testbucket", "testobject"));
  }

  @Test
  public void testRetryFileSizeNonBatch() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    BackOff mockBackOff =
        BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.withMaxRetries(2).backoff());

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "testobject")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new StorageObject().setSize(BigInteger.valueOf(1000)));

    assertEquals(
        1000,
        gcsUtil
            .getObject(
                GcsPath.fromComponents("testbucket", "testobject"),
                mockBackOff,
                new FastNanoClockAndSleeper())
            .getSize()
            .longValue());
    assertEquals(BackOff.STOP, mockBackOff.nextBackOffMillis());
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundBatch() throws Exception {
    JsonFactory jsonFactory = new JacksonFactory();

    String contentBoundary = "batch_foobarbaz";
    String contentBoundaryLine = "--" + contentBoundary;
    String endOfContentBoundaryLine = "--" + contentBoundary + "--";

    GenericJson error = new GenericJson().set("error", new GenericJson().set("code", 404));
    error.setFactory(jsonFactory);

    String content =
        contentBoundaryLine
            + "\n"
            + "Content-Type: application/http\n"
            + "\n"
            + "HTTP/1.1 404 Not Found\n"
            + "Content-Length: -1\n"
            + "\n"
            + error.toString()
            + "\n"
            + "\n"
            + endOfContentBoundaryLine
            + "\n";
    thrown.expect(FileNotFoundException.class);
    MockLowLevelHttpResponse notFoundResponse =
        new MockLowLevelHttpResponse()
            .setContentType("multipart/mixed; boundary=" + contentBoundary)
            .setContent(content)
            .setStatusCode(HttpStatusCodes.STATUS_CODE_OK);

    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder().setLowLevelHttpResponse(notFoundResponse).build();

    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    gcsUtil.setStorageClient(new Storage(mockTransport, Transport.getJsonFactory(), null));
    gcsUtil.fileSizes(ImmutableList.of(GcsPath.fromComponents("testbucket", "testobject")));
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundBatchRetry() throws Exception {
    JsonFactory jsonFactory = new JacksonFactory();

    String contentBoundary = "batch_foobarbaz";
    String contentBoundaryLine = "--" + contentBoundary;
    String endOfContentBoundaryLine = "--" + contentBoundary + "--";

    GenericJson error = new GenericJson().set("error", new GenericJson().set("code", 404));
    error.setFactory(jsonFactory);

    String content =
        contentBoundaryLine
            + "\n"
            + "Content-Type: application/http\n"
            + "\n"
            + "HTTP/1.1 404 Not Found\n"
            + "Content-Length: -1\n"
            + "\n"
            + error.toString()
            + "\n"
            + "\n"
            + endOfContentBoundaryLine
            + "\n";
    thrown.expect(FileNotFoundException.class);

    final LowLevelHttpResponse mockResponse = Mockito.mock(LowLevelHttpResponse.class);
    when(mockResponse.getContentType()).thenReturn("multipart/mixed; boundary=" + contentBoundary);

    // 429: Too many requests, then 200: OK.
    when(mockResponse.getStatusCode()).thenReturn(429, 200);
    when(mockResponse.getContent()).thenReturn(toStream("error"), toStream(content));

    // A mock transport that lets us mock the API responses.
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpRequest(
                new MockLowLevelHttpRequest() {
                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    return mockResponse;
                  }
                })
            .build();

    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    gcsUtil.setStorageClient(
        new Storage(mockTransport, Transport.getJsonFactory(), new RetryHttpRequestInitializer()));
    gcsUtil.fileSizes(ImmutableList.of(GcsPath.fromComponents("testbucket", "testobject")));
  }

  @Test
  public void testRemoveWhenFileNotFound() throws Exception {
    JsonFactory jsonFactory = new JacksonFactory();

    String contentBoundary = "batch_foobarbaz";
    String contentBoundaryLine = "--" + contentBoundary;
    String endOfContentBoundaryLine = "--" + contentBoundary + "--";

    GenericJson error = new GenericJson().set("error", new GenericJson().set("code", 404));
    error.setFactory(jsonFactory);

    String content =
        contentBoundaryLine
            + "\n"
            + "Content-Type: application/http\n"
            + "\n"
            + "HTTP/1.1 404 Not Found\n"
            + "Content-Length: -1\n"
            + "\n"
            + error.toString()
            + "\n"
            + "\n"
            + endOfContentBoundaryLine
            + "\n";

    final LowLevelHttpResponse mockResponse = Mockito.mock(LowLevelHttpResponse.class);
    when(mockResponse.getContentType()).thenReturn("multipart/mixed; boundary=" + contentBoundary);
    when(mockResponse.getStatusCode()).thenReturn(200);
    when(mockResponse.getContent()).thenReturn(toStream(content));

    // A mock transport that lets us mock the API responses.
    MockLowLevelHttpRequest request =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            return mockResponse;
          }
        };
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();

    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();
    gcsUtil.setStorageClient(
        new Storage(mockTransport, Transport.getJsonFactory(), new RetryHttpRequestInitializer()));
    gcsUtil.remove(Arrays.asList("gs://some-bucket/already-deleted"));
  }

  @Test
  public void testCreateBucket() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets.Insert mockStorageInsert = Mockito.mock(Storage.Buckets.Insert.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(any(String.class), any(Bucket.class)))
        .thenReturn(mockStorageInsert);
    when(mockStorageInsert.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new Bucket());

    gcsUtil.createBucket("a", new Bucket(), mockBackOff, new FastNanoClockAndSleeper());
  }

  @Test
  public void testCreateBucketAccessErrors() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Insert mockStorageInsert = Mockito.mock(Storage.Buckets.Insert.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());
    GoogleJsonResponseException expectedException =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_FORBIDDEN,
            "Waves hand mysteriously",
            "These aren't the buckets you're looking for");

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.insert(any(String.class), any(Bucket.class)))
        .thenReturn(mockStorageInsert);
    when(mockStorageInsert.execute()).thenThrow(expectedException);

    thrown.expect(AccessDeniedException.class);

    gcsUtil.createBucket("a", new Bucket(), mockBackOff, new FastNanoClockAndSleeper());
  }

  @Test
  public void testBucketAccessible() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new Bucket());

    assertTrue(
        gcsUtil.bucketAccessible(
            GcsPath.fromComponents("testbucket", "testobject"),
            mockBackOff,
            new FastNanoClockAndSleeper()));
  }

  @Test
  public void testBucketDoesNotExistBecauseOfAccessError() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());
    GoogleJsonResponseException expectedException =
        googleJsonResponseException(
            HttpStatusCodes.STATUS_CODE_FORBIDDEN,
            "Waves hand mysteriously",
            "These aren't the buckets you're looking for");

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute()).thenThrow(expectedException);

    assertFalse(
        gcsUtil.bucketAccessible(
            GcsPath.fromComponents("testbucket", "testobject"),
            mockBackOff,
            new FastNanoClockAndSleeper()));
  }

  @Test
  public void testBucketDoesNotExist() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(
            googleJsonResponseException(
                HttpStatusCodes.STATUS_CODE_NOT_FOUND, "It don't exist", "Nothing here to see"));

    assertFalse(
        gcsUtil.bucketAccessible(
            GcsPath.fromComponents("testbucket", "testobject"),
            mockBackOff,
            new FastNanoClockAndSleeper()));
  }

  @Test
  public void testGetBucket() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new Bucket());

    assertNotNull(
        gcsUtil.getBucket(
            GcsPath.fromComponents("testbucket", "testobject"),
            mockBackOff,
            new FastNanoClockAndSleeper()));
  }

  @Test
  public void testGetBucketNotExists() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = BackOffAdapter.toGcpBackOff(FluentBackoff.DEFAULT.backoff());

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(
            googleJsonResponseException(
                HttpStatusCodes.STATUS_CODE_NOT_FOUND, "It don't exist", "Nothing here to see"));

    thrown.expect(FileNotFoundException.class);
    thrown.expectMessage("It don't exist");
    gcsUtil.getBucket(
        GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff,
        new FastNanoClockAndSleeper());
  }

  @Test
  public void testGCSChannelCloseIdempotent() throws IOException {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();
    SeekableByteChannel channel =
        new GoogleCloudStorageReadChannel(
            null, "dummybucket", "dummyobject", null, new ClientRequestHelper<>(), readOptions);
    channel.close();
    channel.close();
  }

  /** Builds a fake GoogleJsonResponseException for testing API error handling. */
  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final String reason, final String message) throws IOException {
    final JsonFactory jsonFactory = new JacksonFactory();
    HttpTransport transport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
            ErrorInfo errorInfo = new ErrorInfo();
            errorInfo.setReason(reason);
            errorInfo.setMessage(message);
            errorInfo.setFactory(jsonFactory);
            GenericJson error = new GenericJson();
            error.set("code", status);
            error.set("errors", Arrays.asList(errorInfo));
            error.setFactory(jsonFactory);
            GenericJson errorResponse = new GenericJson();
            errorResponse.set("error", error);
            errorResponse.setFactory(jsonFactory);
            return new MockLowLevelHttpRequest()
                .setResponse(
                    new MockLowLevelHttpResponse()
                        .setContent(errorResponse.toPrettyString())
                        .setContentType(Json.MEDIA_TYPE)
                        .setStatusCode(status));
          }
        };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }

  private static List<String> makeStrings(String s, int n) {
    ImmutableList.Builder<String> ret = ImmutableList.builder();
    for (int i = 0; i < n; ++i) {
      ret.add(String.format("gs://bucket/%s%d", s, i));
    }
    return ret.build();
  }

  private static List<GcsPath> makeGcsPaths(String s, int n) {
    ImmutableList.Builder<GcsPath> ret = ImmutableList.builder();
    for (int i = 0; i < n; ++i) {
      ret.add(GcsPath.fromUri(String.format("gs://bucket/%s%d", s, i)));
    }
    return ret.build();
  }

  private static int sumBatchSizes(List<BatchRequest> batches) {
    int ret = 0;
    for (BatchRequest b : batches) {
      ret += b.size();
      assertThat(b.size(), greaterThan(0));
    }
    return ret;
  }

  @Test
  public void testMakeRewriteOps() throws IOException {
    GcsOptions gcsOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();

    LinkedList<RewriteOp> rewrites =
        gcsUtil.makeRewriteOps(makeStrings("s", 1), makeStrings("d", 1));
    assertEquals(1, rewrites.size());

    RewriteOp rewrite = rewrites.pop();
    assertTrue(rewrite.getReadyToEnqueue());
    Storage.Objects.Rewrite request = rewrite.rewriteRequest;
    assertNull(request.getMaxBytesRewrittenPerCall());
    assertEquals("bucket", request.getSourceBucket());
    assertEquals("s0", request.getSourceObject());
    assertEquals("bucket", request.getDestinationBucket());
    assertEquals("d0", request.getDestinationObject());
  }

  @Test
  public void testMakeRewriteOpsWithOptions() throws IOException {
    GcsOptions gcsOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = gcsOptions.getGcsUtil();
    gcsUtil.maxBytesRewrittenPerCall = 1337L;

    LinkedList<RewriteOp> rewrites =
        gcsUtil.makeRewriteOps(makeStrings("s", 1), makeStrings("d", 1));
    assertEquals(1, rewrites.size());

    RewriteOp rewrite = rewrites.pop();
    assertTrue(rewrite.getReadyToEnqueue());
    Storage.Objects.Rewrite request = rewrite.rewriteRequest;
    assertEquals(Long.valueOf(1337L), request.getMaxBytesRewrittenPerCall());
  }

  @Test
  public void testMakeCopyBatches() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    // Small number of files fits in 1 batch
    List<BatchRequest> batches =
        gcsUtil.makeCopyBatches(gcsUtil.makeRewriteOps(makeStrings("s", 3), makeStrings("d", 3)));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(3));

    // 1 batch of files fits in 1 batch
    batches =
        gcsUtil.makeCopyBatches(
            gcsUtil.makeRewriteOps(makeStrings("s", 100), makeStrings("d", 100)));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(100));

    // A little more than 5 batches of files fits in 6 batches
    batches =
        gcsUtil.makeCopyBatches(
            gcsUtil.makeRewriteOps(makeStrings("s", 501), makeStrings("d", 501)));
    assertThat(batches.size(), equalTo(6));
    assertThat(sumBatchSizes(batches), equalTo(501));
  }

  @Test
  public void testMakeRewriteOpsInvalid() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Number of source files 3");

    gcsUtil.makeRewriteOps(makeStrings("s", 3), makeStrings("d", 1));
  }

  @Test
  public void testMakeRemoveBatches() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    // Small number of files fits in 1 batch
    List<BatchRequest> batches = gcsUtil.makeRemoveBatches(makeStrings("s", 3));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(3));

    // 1 batch of files fits in 1 batch
    batches = gcsUtil.makeRemoveBatches(makeStrings("s", 100));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(100));

    // A little more than 5 batches of files fits in 6 batches
    batches = gcsUtil.makeRemoveBatches(makeStrings("s", 501));
    assertThat(batches.size(), equalTo(6));
    assertThat(sumBatchSizes(batches), equalTo(501));
  }

  @Test
  public void testMakeGetBatches() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    // Small number of files fits in 1 batch
    List<StorageObjectOrIOException[]> results = Lists.newArrayList();
    List<BatchRequest> batches = gcsUtil.makeGetBatches(makeGcsPaths("s", 3), results);
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(3));
    assertEquals(3, results.size());

    // 1 batch of files fits in 1 batch
    results = Lists.newArrayList();
    batches = gcsUtil.makeGetBatches(makeGcsPaths("s", 100), results);
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(100));
    assertEquals(100, results.size());

    // A little more than 5 batches of files fits in 6 batches
    results = Lists.newArrayList();
    batches = gcsUtil.makeGetBatches(makeGcsPaths("s", 501), results);
    assertThat(batches.size(), equalTo(6));
    assertThat(sumBatchSizes(batches), equalTo(501));
    assertEquals(501, results.size());
  }

  /** A helper to wrap a {@link GenericJson} object in a content stream. */
  private static InputStream toStream(String content) throws IOException {
    return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
  }
}

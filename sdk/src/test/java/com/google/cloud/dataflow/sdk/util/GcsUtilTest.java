/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
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
import com.google.cloud.dataflow.sdk.options.GcsOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadChannel;
import com.google.cloud.hadoop.util.ClientRequestHelper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/** Test case for {@link GcsUtil}. */
@RunWith(JUnit4.class)
public class GcsUtilTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testGlobTranslation() {
    assertEquals("foo", GcsUtil.globToRegexp("foo"));
    assertEquals("fo[^/]*o", GcsUtil.globToRegexp("fo*o"));
    assertEquals("f[^/]*o\\.[^/]", GcsUtil.globToRegexp("f*o.?"));
    assertEquals("foo-[0-9][^/]*", GcsUtil.globToRegexp("foo-[0-9]*"));
  }

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
      executorService.execute(new Runnable() {
        @Override
        public void run() {
          // Wait for latch N and then release latch N - 1
          try {
            countDownLatches[currentLatch].await();
            if (currentLatch > 0) {
              countDownLatches[currentLatch - 1].countDown();
            }
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        }
      });
    }

    // Release the last latch starting the chain reaction.
    countDownLatches[countDownLatches.length - 1].countDown();
    executorService.shutdown();
    assertTrue("Expected tasks to complete",
        executorService.awaitTermination(10, TimeUnit.SECONDS));
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
    when(mockStorageObjects.get("testbucket", "testdirectory/otherfile")).thenReturn(
        mockStorageGet);
    when(mockStorageObjects.list("testbucket")).thenReturn(mockStorageList);
    when(mockStorageGet.execute()).thenReturn(
        new StorageObject().setBucket("testbucket").setName("testdirectory/otherfile"));
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
      List<GcsPath> expectedFiles = ImmutableList.of(
          GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file[1-3]*");
      List<GcsPath> expectedFiles = ImmutableList.of(
          GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/testdirectory/file?name");
      List<GcsPath> expectedFiles = ImmutableList.of(
          GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }

    {
      GcsPath pattern = GcsPath.fromUri("gs://testbucket/test*ectory/fi*name");
      List<GcsPath> expectedFiles = ImmutableList.of(
          GcsPath.fromUri("gs://testbucket/testdirectory/file1name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file2name"),
          GcsPath.fromUri("gs://testbucket/testdirectory/file3name"));

      assertThat(expectedFiles, contains(gcsUtil.expand(pattern).toArray()));
    }
  }

  // Patterns that contain recursive wildcards ('**') are not supported.
  @Test
  public void testRecursiveGlobExpansionFails() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    GcsPath pattern = GcsPath.fromUri("gs://testbucket/test**");

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Unsupported wildcard usage");
    gcsUtil.expand(pattern);
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
        googleJsonResponseException(HttpStatusCodes.STATUS_CODE_NOT_FOUND,
            "It don't exist", "Nothing here to see");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(pattern.getBucket(), pattern.getObject())).thenReturn(
        mockStorageGet);
    when(mockStorageGet.execute()).thenThrow(expectedException);

    assertEquals(Collections.EMPTY_LIST, gcsUtil.expand(pattern));
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
        googleJsonResponseException(HttpStatusCodes.STATUS_CODE_FORBIDDEN,
            "Waves hand mysteriously", "These aren't the buckets your looking for");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(pattern.getBucket(), pattern.getObject())).thenReturn(
        mockStorageGet);
    when(mockStorageGet.execute()).thenThrow(expectedException);

    thrown.expect(IOException.class);
    thrown.expectMessage("Unable to match files for pattern");
    gcsUtil.expand(pattern);
  }

  @Test
  public void testGetSizeBytes() throws Exception {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "testobject")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute()).thenReturn(
        new StorageObject().setSize(BigInteger.valueOf(1000)));

    assertEquals(1000, gcsUtil.fileSize(GcsPath.fromComponents("testbucket", "testobject")));
  }

  @Test
  public void testGetSizeBytesWhenFileNotFound() throws Exception {
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
  public void testRetryFileSize() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockStorageGet = Mockito.mock(Storage.Objects.Get.class);

    BackOff mockBackOff = new AttemptBoundedExponentialBackOff(3, 200);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket", "testobject")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new StorageObject().setSize(BigInteger.valueOf(1000)));

    assertEquals(1000, gcsUtil.fileSize(GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff, new FastNanoClockAndSleeper()));
    assertEquals(mockBackOff.nextBackOffMillis(), BackOff.STOP);
  }

  @Test
  public void testBucketExists() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = new AttemptBoundedExponentialBackOff(3, 200);

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(new SocketTimeoutException("SocketException"))
        .thenReturn(new Bucket());

    assertTrue(gcsUtil.bucketExists(GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff, new FastNanoClockAndSleeper()));
  }

  @Test
  public void testBucketDoesNotExistBecauseOfAccessError() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = new AttemptBoundedExponentialBackOff(3, 200);
    GoogleJsonResponseException expectedException =
        googleJsonResponseException(HttpStatusCodes.STATUS_CODE_FORBIDDEN,
            "Waves hand mysteriously", "These aren't the buckets your looking for");

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(expectedException);

    assertFalse(gcsUtil.bucketExists(GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff, new FastNanoClockAndSleeper()));
  }

  @Test
  public void testBucketDoesNotExist() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    Storage.Buckets mockStorageObjects = Mockito.mock(Storage.Buckets.class);
    Storage.Buckets.Get mockStorageGet = Mockito.mock(Storage.Buckets.Get.class);

    BackOff mockBackOff = new AttemptBoundedExponentialBackOff(3, 200);

    when(mockStorage.buckets()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get("testbucket")).thenReturn(mockStorageGet);
    when(mockStorageGet.execute())
        .thenThrow(googleJsonResponseException(HttpStatusCodes.STATUS_CODE_NOT_FOUND,
            "It don't exist", "Nothing here to see"));

    assertFalse(gcsUtil.bucketExists(GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff, new FastNanoClockAndSleeper()));
  }

  @Test
  public void testGCSChannelCloseIdempotent() throws IOException {
    SeekableByteChannel channel =
        new GoogleCloudStorageReadChannel(null, "dummybucket", "dummyobject", null,
        new ClientRequestHelper<StorageObject>());
    channel.close();
    channel.close();
  }

  /**
   * Builds a fake GoogleJsonResponseException for testing API error handling.
   */
  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final String reason, final String message) throws IOException {
    final JsonFactory jsonFactory = new JacksonFactory();
    HttpTransport transport = new MockHttpTransport() {
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
        return new MockLowLevelHttpRequest().setResponse(
            new MockLowLevelHttpResponse().setContent(errorResponse.toPrettyString())
            .setContentType(Json.MEDIA_TYPE).setStatusCode(status));
        }
    };
    HttpRequest request =
        transport.createRequestFactory().buildGetRequest(HttpTesting.SIMPLE_GENERIC_URL);
    request.setThrowExceptionOnExecuteError(false);
    HttpResponse response = request.execute();
    return GoogleJsonResponseException.from(jsonFactory, response);
  }
}

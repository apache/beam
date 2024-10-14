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

import static org.apache.beam.sdk.options.ExperimentalOptions.hasExperiment;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.BackOff;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.RewriteResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageReadOptions;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidObjectException;
import java.math.BigInteger;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.BatchInterface;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.CreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.RewriteOp;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.FastNanoClockAndSleeper;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
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
  MetricsContainerImpl testMetricsContainer;

  @Before
  public void setUp() {
    // Setup the ProcessWideContainer for testing metrics are set.
    testMetricsContainer = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(testMetricsContainer);
    MetricsEnvironment.setCurrentContainer(testMetricsContainer);
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
  public void testCreationWithDefaultGoogleCloudStorageReadOptions() throws Exception {
    Configuration.addDefaultResource("test-hadoop-conf.xml");
    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);

    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    GoogleCloudStorage googleCloudStorageMock = Mockito.spy(GoogleCloudStorage.class);
    Mockito.when(googleCloudStorageMock.open(Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(SeekableByteChannel.class));
    gcsUtil.setCloudStorageImpl(googleCloudStorageMock);

    GoogleCloudStorageReadOptions expectedOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(GoogleCloudStorageReadOptions.Fadvise.AUTO)
            .setSupportGzipEncoding(true)
            .setFastFailOnNotFound(false)
            .build();

    assertEquals(expectedOptions, pipelineOptions.getGoogleCloudStorageReadOptions());

    // Assert read options are passed to GCS calls
    pipelineOptions.getGcsUtil().open(GcsPath.fromUri("gs://bucket/path"));
    Mockito.verify(googleCloudStorageMock, Mockito.times(1))
        .open(StorageResourceId.fromStringPath("gs://bucket/path"), expectedOptions);
  }

  @Test
  public void testCreationWithExplicitGoogleCloudStorageReadOptions() throws Exception {
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder()
            .setFadvise(GoogleCloudStorageReadOptions.Fadvise.AUTO)
            .setSupportGzipEncoding(true)
            .setFastFailOnNotFound(false)
            .build();

    GcsOptions pipelineOptions = PipelineOptionsFactory.as(GcsOptions.class);
    pipelineOptions.setGoogleCloudStorageReadOptions(readOptions);

    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    GoogleCloudStorage googleCloudStorageMock = Mockito.spy(GoogleCloudStorage.class);
    Mockito.when(googleCloudStorageMock.open(Mockito.any(), Mockito.any()))
        .thenReturn(Mockito.mock(SeekableByteChannel.class));
    gcsUtil.setCloudStorageImpl(googleCloudStorageMock);

    assertEquals(readOptions, pipelineOptions.getGoogleCloudStorageReadOptions());

    // Assert read options are passed to GCS calls
    pipelineOptions.getGcsUtil().open(GcsPath.fromUri("gs://bucket/path"));
    Mockito.verify(googleCloudStorageMock, Mockito.times(1))
        .open(StorageResourceId.fromStringPath("gs://bucket/path"), readOptions);
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
                new FastNanoClockAndSleeper()::sleep)
            .getSize()
            .longValue());
    assertEquals(BackOff.STOP, mockBackOff.nextBackOffMillis());
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundBatch() throws Exception {
    JsonFactory jsonFactory = new GsonFactory();

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
            + contentBoundaryLine
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
    gcsUtil.fileSizes(
        ImmutableList.of(
            GcsPath.fromComponents("testbucket", "testobject"),
            GcsPath.fromComponents("testbucket", "testobject2")));
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundNoBatch() throws Exception {
    thrown.expect(FileNotFoundException.class);
    MockLowLevelHttpResponse notFoundResponse =
        new MockLowLevelHttpResponse()
            .setContentType("text/plain")
            .setContent("error")
            .setStatusCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);

    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder().setLowLevelHttpResponse(notFoundResponse).build();

    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    gcsUtil.setStorageClient(new Storage(mockTransport, Transport.getJsonFactory(), null));
    gcsUtil.fileSizes(ImmutableList.of(GcsPath.fromComponents("testbucket", "testobject")));
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundBatchRetry() throws Exception {
    JsonFactory jsonFactory = new GsonFactory();

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
            + contentBoundaryLine
            + "\n"
            + "Content-Type: application/http\n"
            + "\n"
            + "HTTP/1.1 404 Not Found\n"
            + "Content-Length: -1\n"
            + "\n"
            + error.toString()
            + "\n"
            + "\n"
            + endOfContentBoundaryLine;

    thrown.expect(FileNotFoundException.class);

    final LowLevelHttpResponse[] mockResponses =
        new LowLevelHttpResponse[] {
          Mockito.mock(LowLevelHttpResponse.class), Mockito.mock(LowLevelHttpResponse.class),
        };
    when(mockResponses[0].getContentType()).thenReturn("text/plain");
    when(mockResponses[1].getContentType())
        .thenReturn("multipart/mixed; boundary=" + contentBoundary);

    // 429: Too many requests, then 200: OK.
    when(mockResponses[0].getStatusCode()).thenReturn(429);
    when(mockResponses[1].getStatusCode()).thenReturn(200);
    when(mockResponses[0].getContent()).thenReturn(toStream("error"));
    when(mockResponses[1].getContent()).thenReturn(toStream(content));

    // A mock transport that lets us mock the API responses.
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpRequest(
                new MockLowLevelHttpRequest() {
                  int index = 0;

                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    return mockResponses[index++];
                  }
                })
            .build();

    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    gcsUtil.setStorageClient(
        new Storage(mockTransport, Transport.getJsonFactory(), new RetryHttpRequestInitializer()));
    gcsUtil.fileSizes(
        ImmutableList.of(
            GcsPath.fromComponents("testbucket", "testobject"),
            GcsPath.fromComponents("testbucket", "testobject2")));
  }

  @Test
  public void testGetSizeBytesWhenFileNotFoundNoBatchRetry() throws Exception {
    thrown.expect(FileNotFoundException.class);

    final LowLevelHttpResponse[] mockResponses =
        new LowLevelHttpResponse[] {
          Mockito.mock(LowLevelHttpResponse.class), Mockito.mock(LowLevelHttpResponse.class),
        };
    when(mockResponses[0].getContentType()).thenReturn("text/plain");
    when(mockResponses[1].getContentType()).thenReturn("text/plain");

    // 429: Too many requests, then 200: OK.
    when(mockResponses[0].getStatusCode()).thenReturn(429);
    when(mockResponses[1].getStatusCode()).thenReturn(404);
    when(mockResponses[0].getContent()).thenReturn(toStream("error"));
    when(mockResponses[1].getContent()).thenReturn(toStream("error"));

    // A mock transport that lets us mock the API responses.
    MockHttpTransport mockTransport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpRequest(
                new MockLowLevelHttpRequest() {
                  int index = 0;

                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    return mockResponses[index++];
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
    JsonFactory jsonFactory = new GsonFactory();

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

    gcsUtil.createBucket("a", new Bucket(), mockBackOff, new FastNanoClockAndSleeper()::sleep);
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

    gcsUtil.createBucket("a", new Bucket(), mockBackOff, new FastNanoClockAndSleeper()::sleep);
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
            new FastNanoClockAndSleeper()::sleep));
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
            new FastNanoClockAndSleeper()::sleep));
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
            new FastNanoClockAndSleeper()::sleep));
  }

  @Test
  public void testVerifyBucketAccessible() throws IOException {
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

    gcsUtil.verifyBucketAccessible(
        GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff,
        new FastNanoClockAndSleeper()::sleep);
  }

  @Test(expected = AccessDeniedException.class)
  public void testVerifyBucketAccessibleAccessError() throws IOException {
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

    gcsUtil.verifyBucketAccessible(
        GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff,
        new FastNanoClockAndSleeper()::sleep);
  }

  @Test(expected = FileNotFoundException.class)
  public void testVerifyBucketAccessibleDoesNotExist() throws IOException {
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

    gcsUtil.verifyBucketAccessible(
        GcsPath.fromComponents("testbucket", "testobject"),
        mockBackOff,
        new FastNanoClockAndSleeper()::sleep);
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
            new FastNanoClockAndSleeper()::sleep));
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
        new FastNanoClockAndSleeper()::sleep);
  }

  @Test
  public void testGCSChannelCloseIdempotent() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(false).build();
    SeekableByteChannel channel =
        gcsUtil.open(GcsPath.fromComponents("testbucket", "testobject"), readOptions);
    channel.close();
    channel.close();
  }

  @Test
  public void testGCSReadMetricsIsSet() {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    gcsUtil.setCloudStorageImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("Beam")
            .setGrpcEnabled(true)
            .setProjectId("my_project")
            .build());
    GoogleCloudStorageReadOptions readOptions =
        GoogleCloudStorageReadOptions.builder().setFastFailOnNotFound(true).build();
    assertThrows(
        IOException.class,
        () -> gcsUtil.open(GcsPath.fromComponents("testbucket", "testbucket"), readOptions));
    verifyMetricWasSet("my_project", "testbucket", "GcsGet", "permission_denied", 1);
  }

  @Test
  public void testGCSWriteMetricsIsSet() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();
    GoogleCloudStorage mockStorage = Mockito.mock(GoogleCloudStorage.class);
    gcsUtil.setCloudStorageImpl(
        GoogleCloudStorageOptions.builder()
            .setAppName("Beam")
            .setGrpcEnabled(true)
            .setProjectId("my_project")
            .build());
    when(mockStorage.create(
            new StorageResourceId("testbucket", "testobject"),
            CreateObjectOptions.builder()
                .setOverwriteExisting(true)
                .setContentType("type")
                .build()))
        .thenThrow(IOException.class);
    GcsPath gcsPath = GcsPath.fromComponents("testbucket", "testobject");
    assertThrows(IOException.class, () -> gcsUtil.create(gcsPath, ""));
    verifyMetricWasSet("my_project", "testbucket", "GcsInsert", "permission_denied", 1);
  }

  private void verifyMetricWasSet(
      String projectId, String bucketId, String method, String status, long count) {
    // Verify the metric as reported.
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "Storage");
    labels.put(MonitoringInfoConstants.Labels.METHOD, method);
    labels.put(MonitoringInfoConstants.Labels.GCS_PROJECT_ID, projectId);
    labels.put(MonitoringInfoConstants.Labels.GCS_BUCKET, bucketId);
    labels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.cloudStorageBucket(bucketId));
    labels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getProcessWideContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
  }

  /** Builds a fake GoogleJsonResponseException for testing API error handling. */
  private static GoogleJsonResponseException googleJsonResponseException(
      final int status, final String reason, final String message) throws IOException {
    final JsonFactory jsonFactory = new GsonFactory();
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
    return makeStrings("bucket", s, n);
  }

  private static List<String> makeStrings(String bucket, String s, int n) {
    ImmutableList.Builder<String> ret = ImmutableList.builder();
    for (int i = 0; i < n; ++i) {
      ret.add(String.format("gs://%s/%s%d", bucket, s, i));
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

  private static int sumBatchSizes(List<BatchInterface> batches) {
    int ret = 0;
    for (BatchInterface b : batches) {
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
        gcsUtil.makeRewriteOps(makeStrings("s", 1), makeStrings("d", 1), false, false, false);
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
        gcsUtil.makeRewriteOps(makeStrings("s", 1), makeStrings("d", 1), false, false, false);
    assertEquals(1, rewrites.size());

    RewriteOp rewrite = rewrites.pop();
    assertTrue(rewrite.getReadyToEnqueue());
    Storage.Objects.Rewrite request = rewrite.rewriteRequest;
    assertEquals(Long.valueOf(1337L), request.getMaxBytesRewrittenPerCall());
  }

  @Test
  public void testMakeRewriteBatches() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    // Small number of files fits in 1 batch
    List<BatchInterface> batches =
        gcsUtil.makeRewriteBatches(
            gcsUtil.makeRewriteOps(makeStrings("s", 3), makeStrings("d", 3), false, false, false));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(3));

    // 1 batch of files fits in 1 batch
    batches =
        gcsUtil.makeRewriteBatches(
            gcsUtil.makeRewriteOps(
                makeStrings("s", 100), makeStrings("d", 100), false, false, false));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(100));

    // A little more than 5 batches of files fits in 6 batches
    batches =
        gcsUtil.makeRewriteBatches(
            gcsUtil.makeRewriteOps(
                makeStrings("s", 501), makeStrings("d", 501), false, false, false));
    assertThat(batches.size(), equalTo(6));
    assertThat(sumBatchSizes(batches), equalTo(501));
  }

  @Test
  public void testMakeRewriteBatchesWithLowerDataOpLimit() throws IOException {
    GcsOptions options = gcsOptionsWithTestCredential();
    options.setGcsRewriteDataOpBatchLimit(2);
    GcsUtil gcsUtil = options.getGcsUtil();

    // Small number of files in same bucket fits in 1 batch
    List<BatchInterface> batches =
        gcsUtil.makeRewriteBatches(
            gcsUtil.makeRewriteOps(makeStrings("s", 5), makeStrings("d", 5), false, false, false));
    assertThat(batches.size(), equalTo(1));
    assertThat(sumBatchSizes(batches), equalTo(5));

    // Files copying between buckets use smaller batch size
    batches =
        gcsUtil.makeRewriteBatches(
            gcsUtil.makeRewriteOps(
                makeStrings("bucket1", "s", 5),
                makeStrings("bucket2", "d", 5),
                false,
                false,
                false));
    assertThat(batches.size(), equalTo(3));
    assertThat(sumBatchSizes(batches), equalTo(5));

    // A mix of same bucket and different buckets uses large batches when possible.
    List<String> fromFiles = new ArrayList<>(makeStrings("bucket1", "s", 3));
    List<String> toFiles = new ArrayList<>(makeStrings("bucket2", "d", 3));
    fromFiles.addAll(makeStrings("t", 90));
    toFiles.addAll(makeStrings("e", 90));
    fromFiles.addAll(makeStrings("bucket3", "u", 3));
    toFiles.addAll(makeStrings("bucket4", "f", 3));
    fromFiles.addAll(makeStrings("bucket5", "v", 1));
    toFiles.addAll(makeStrings("bucket5", "g", 1));

    batches =
        gcsUtil.makeRewriteBatches(gcsUtil.makeRewriteOps(fromFiles, toFiles, false, false, false));
    assertThat(batches.size(), equalTo(4));
    assertThat(batches.get(0).size(), equalTo(91));
    assertThat(sumBatchSizes(batches), equalTo(97));
  }

  @Test
  public void testMakeRewriteOpsInvalid() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Number of source files 3");

    gcsUtil.makeRewriteOps(makeStrings("s", 3), makeStrings("d", 1), false, false, false);
  }

  private class FakeBatcher implements BatchInterface {
    ArrayList<Supplier<Void>> requests = new ArrayList<>();

    @Override
    public <T> void queue(AbstractGoogleJsonClientRequest<T> request, JsonBatchCallback<T> cb) {
      assertNotNull(request);
      assertNotNull(cb);
      requests.add(
          () -> {
            try {
              try {
                T result = request.execute();
                cb.onSuccess(result, null);
              } catch (FileNotFoundException e) {
                GoogleJsonError error = new GoogleJsonError();
                error.setCode(HttpStatusCodes.STATUS_CODE_NOT_FOUND);
                cb.onFailure(error, null);
              } catch (GoogleJsonResponseException e) {
                cb.onFailure(e.getDetails(), null);
              } catch (SocketTimeoutException e) {
                System.out.println("Propagating socket exception as batch processing error");
                throw e;
              } catch (Exception e) {
                System.out.println("Propagating exception as server error " + e);
                e.printStackTrace();
                GoogleJsonError error = new GoogleJsonError();
                error.setCode(HttpStatusCodes.STATUS_CODE_SERVER_ERROR);
                cb.onFailure(error, null);
              }
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            return null;
          });
    }

    @Override
    public void execute() throws IOException {
      RuntimeException lastException = null;
      for (Supplier<Void> request : requests) {
        try {
          request.get();
        } catch (RuntimeException e) {
          lastException = e;
        }
      }
      if (lastException != null) {
        throw lastException;
      }
    }

    @Override
    public int size() {
      return requests.size();
    }
  }

  @Test
  public void testRename() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Rewrite mockStorageRewrite = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Delete mockStorageDelete1 = Mockito.mock(Storage.Objects.Delete.class);
    Storage.Objects.Delete mockStorageDelete2 = Mockito.mock(Storage.Objects.Delete.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite);
    when(mockStorageRewrite.execute())
        .thenThrow(new InvalidObjectException("Test exception"))
        .thenReturn(new RewriteResponse().setDone(true));
    when(mockStorageObjects.delete("bucket", "s0"))
        .thenReturn(mockStorageDelete1)
        .thenReturn(mockStorageDelete2);

    when(mockStorageDelete1.execute()).thenThrow(new InvalidObjectException("Test exception"));

    gcsUtil.rename(makeStrings("s", 1), makeStrings("d", 1));
    verify(mockStorageRewrite, times(2)).execute();
    verify(mockStorageDelete1, times(1)).execute();
    verify(mockStorageDelete2, times(1)).execute();
  }

  @Test
  public void testRenameIgnoringMissing() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Rewrite mockStorageRewrite1 = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Rewrite mockStorageRewrite2 = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Delete mockStorageDelete = Mockito.mock(Storage.Objects.Delete.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite1);
    when(mockStorageRewrite1.execute()).thenThrow(new FileNotFoundException());
    when(mockStorageObjects.rewrite("bucket", "s1", "bucket", "d1", null))
        .thenReturn(mockStorageRewrite2);
    when(mockStorageRewrite2.execute()).thenReturn(new RewriteResponse().setDone(true));
    when(mockStorageObjects.delete("bucket", "s1")).thenReturn(mockStorageDelete);

    gcsUtil.rename(
        makeStrings("s", 2), makeStrings("d", 2), StandardMoveOptions.IGNORE_MISSING_FILES);
    verify(mockStorageRewrite1, times(1)).execute();
    verify(mockStorageRewrite2, times(1)).execute();
    verify(mockStorageDelete, times(1)).execute();
  }

  @Test
  public void testRenamePropagateMissingException() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Rewrite mockStorageRewrite = Mockito.mock(Storage.Objects.Rewrite.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite);
    when(mockStorageRewrite.execute()).thenThrow(new FileNotFoundException());

    assertThrows(IOException.class, () -> gcsUtil.rename(makeStrings("s", 1), makeStrings("d", 1)));
    verify(mockStorageRewrite, times(1)).execute();
  }

  @Test
  public void testRenameSkipDestinationExistsSameBucket() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Rewrite mockStorageRewrite = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Delete mockStorageDelete = Mockito.mock(Storage.Objects.Delete.class);

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite);
    when(mockStorageRewrite.execute()).thenReturn(new RewriteResponse().setDone(true));
    when(mockStorageObjects.delete("bucket", "s0")).thenReturn(mockStorageDelete);

    gcsUtil.rename(
        makeStrings("s", 1), makeStrings("d", 1), StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);
    verify(mockStorageRewrite, times(1)).execute();
    verify(mockStorageDelete, times(1)).execute();
  }

  @Test
  public void testRenameSkipDestinationExistsDifferentBucket() throws IOException {
    GcsOptions pipelineOptions = gcsOptionsWithTestCredential();
    GcsUtil gcsUtil = pipelineOptions.getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            gcsUtil.rename(
                Collections.singletonList("gs://bucket/source"),
                Collections.singletonList("gs://different_bucket/dest"),
                StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS));
  }

  @Test
  public void testThrowRetentionPolicyNotMetErrorWhenUnequalChecksum() throws IOException {
    // ./gradlew sdks:java:extensions:google-cloud-platform-core:test --tests
    // org.apache.beam.sdk.extensions.gcp.util.GcsUtilTest.testHanRetentionPolicyNotMetError
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockGetRequest1 = Mockito.mock(Storage.Objects.Get.class);
    Storage.Objects.Get mockGetRequest2 = Mockito.mock(Storage.Objects.Get.class);
    Storage.Objects.Rewrite mockStorageRewrite = Mockito.mock(Storage.Objects.Rewrite.class);

    // Gcs object to be used when checking the hash of the files during rewrite fail.
    StorageObject srcObject = new StorageObject().setMd5Hash("a");
    StorageObject destObject = new StorageObject().setMd5Hash("b");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite);
    when(mockStorageRewrite.execute())
        .thenThrow(googleJsonResponseException(403, "retentionPolicyNotMet", "Too soon"));
    when(mockStorageObjects.get("bucket", "s0")).thenReturn(mockGetRequest1);
    when(mockGetRequest1.execute()).thenReturn(srcObject);
    when(mockStorageObjects.get("bucket", "d0")).thenReturn(mockGetRequest2);
    when(mockGetRequest2.execute()).thenReturn(destObject);

    assertThrows(IOException.class, () -> gcsUtil.rename(makeStrings("s", 1), makeStrings("d", 1)));

    verify(mockStorageRewrite, times(1)).execute();
  }

  @Test
  public void testIgnoreRetentionPolicyNotMetErrorWhenEqualChecksum() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockGetRequest = Mockito.mock(Storage.Objects.Get.class);
    Storage.Objects.Rewrite mockStorageRewrite1 = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Rewrite mockStorageRewrite2 = Mockito.mock(Storage.Objects.Rewrite.class);
    Storage.Objects.Delete mockStorageDelete = Mockito.mock(Storage.Objects.Delete.class);

    // Gcs object to be used when checking the hash of the files during rewrite fail.
    StorageObject gcsObject = new StorageObject().setMd5Hash("a");

    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    // First rewrite with retentionPolicyNotMet error.
    when(mockStorageObjects.rewrite("bucket", "s0", "bucket", "d0", null))
        .thenReturn(mockStorageRewrite1);
    when(mockStorageRewrite1.execute())
        .thenThrow(googleJsonResponseException(403, "retentionPolicyNotMet", "Too soon"));
    when(mockStorageObjects.get(any(), any())) // to access object hash during error handling
        .thenReturn(mockGetRequest);
    when(mockGetRequest.execute())
        .thenReturn(gcsObject); // both source and destination will get the same hash
    when(mockStorageObjects.delete("bucket", "s0")).thenReturn(mockStorageDelete);

    // Second rewrite should not be affected.
    when(mockStorageObjects.rewrite("bucket", "s1", "bucket", "d1", null))
        .thenReturn(mockStorageRewrite2);
    when(mockStorageRewrite2.execute()).thenReturn(new RewriteResponse().setDone(true));
    when(mockStorageObjects.delete("bucket", "s1")).thenReturn(mockStorageDelete);

    gcsUtil.rename(makeStrings("s", 2), makeStrings("d", 2));

    verify(mockStorageRewrite1, times(1)).execute();
    verify(mockStorageRewrite2, times(1)).execute();
    verify(mockStorageDelete, times(2)).execute();
  }

  @Test
  public void testMakeRemoveBatches() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    // Small number of files fits in 1 batch
    List<BatchInterface> batches = gcsUtil.makeRemoveBatches(makeStrings("s", 3));
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
    List<BatchInterface> batches = gcsUtil.makeGetBatches(makeGcsPaths("s", 3), results);
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

  @Test
  public void testGetObjects() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockGetRequest = Mockito.mock(Storage.Objects.Get.class);
    StorageObject object = new StorageObject();
    when(mockGetRequest.execute()).thenReturn(object);
    when(mockStorageObjects.get(any(), any())).thenReturn(mockGetRequest);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);

    List<StorageObjectOrIOException> results = gcsUtil.getObjects(makeGcsPaths("s", 1));

    assertEquals(object, results.get(0).storageObject());
  }

  @Test
  public void testGetObjectsWithException() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    Storage.Objects.Get mockGetRequest = Mockito.mock(Storage.Objects.Get.class);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    when(mockStorageObjects.get(any(), any())).thenReturn(mockGetRequest);
    when(mockGetRequest.execute()).thenThrow(new RuntimeException("fakeException"));

    thrown.expect(IOException.class);
    thrown.expectMessage("Error trying to get gs://bucket/s0");

    List<StorageObjectOrIOException> results = gcsUtil.getObjects(makeGcsPaths("s", 1));

    for (StorageObjectOrIOException result : results) {
      if (null != result.ioException()) {
        throw result.ioException();
      }
    }
  }

  @Test
  public void testListObjectsException() throws IOException {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();

    Storage mockStorage = Mockito.mock(Storage.class);
    gcsUtil.setStorageClient(mockStorage);
    gcsUtil.setBatchRequestSupplier(FakeBatcher::new);

    Storage.Objects mockStorageObjects = Mockito.mock(Storage.Objects.class);
    when(mockStorage.objects()).thenReturn(mockStorageObjects);
    Storage.Objects.List mockStorageList = Mockito.mock(Storage.Objects.List.class);
    when(mockStorageObjects.list(any())).thenReturn(mockStorageList);
    when(mockStorageList.execute()).thenThrow(new RuntimeException("FakeException"));

    thrown.expect(IOException.class);
    thrown.expectMessage("Unable to match files in bucket testBucket");

    gcsUtil.listObjects("testBucket", "prefix", null);
  }

  public static class GcsUtilMock extends GcsUtil {

    public GoogleCloudStorage googleCloudStorage;

    public static GcsUtilMock createMockWithMockStorage(PipelineOptions options, byte[] readPayload)
        throws IOException {
      GcsUtilMock gcsUtilMock = createMock(options);
      GoogleCloudStorage googleCloudStorageMock = Mockito.mock(GoogleCloudStorage.class);
      gcsUtilMock.googleCloudStorage = googleCloudStorageMock;
      // set the mock in the super object as well
      gcsUtilMock.setCloudStorageImpl(gcsUtilMock.googleCloudStorage);

      if (readPayload == null) {
        Mockito.when(googleCloudStorageMock.create(Mockito.any(), Mockito.any()))
            .thenReturn(Channels.newChannel(new ByteArrayOutputStream()));
      } else {
        SeekableByteChannel seekableByteChannel = new SeekableInMemoryByteChannel(readPayload);
        Mockito.when(googleCloudStorageMock.open(Mockito.any())).thenReturn(seekableByteChannel);
        Mockito.when(googleCloudStorageMock.open(Mockito.any(), Mockito.any()))
            .thenReturn(seekableByteChannel);
      }
      return gcsUtilMock;
    }

    public static GcsUtilMock createMock(PipelineOptions options) {
      GcsOptions gcsOptions = options.as(GcsOptions.class);
      Storage.Builder storageBuilder = Transport.newStorageClient(gcsOptions);
      return new GcsUtilMock(
          storageBuilder.build(),
          storageBuilder.getHttpRequestInitializer(),
          gcsOptions.getExecutorService(),
          hasExperiment(options, "use_grpc_for_gcs"),
          gcsOptions.getGcpCredential(),
          gcsOptions.getGcsUploadBufferSizeBytes(),
          gcsOptions.getGcsRewriteDataOpBatchLimit(),
          GcsCountersOptions.create(
              gcsOptions.getEnableBucketReadMetricCounter()
                  ? gcsOptions.getGcsReadCounterPrefix()
                  : null,
              gcsOptions.getEnableBucketWriteMetricCounter()
                  ? gcsOptions.getGcsWriteCounterPrefix()
                  : null),
          gcsOptions.getGoogleCloudStorageReadOptions());
    }

    private GcsUtilMock(
        Storage storageClient,
        HttpRequestInitializer httpRequestInitializer,
        ExecutorService executorService,
        Boolean shouldUseGrpc,
        Credentials credentials,
        @Nullable Integer uploadBufferSizeBytes,
        @Nullable Integer rewriteDataOpBatchLimit,
        GcsCountersOptions gcsCountersOptions,
        GoogleCloudStorageReadOptions gcsReadOptions) {
      super(
          storageClient,
          httpRequestInitializer,
          executorService,
          shouldUseGrpc,
          credentials,
          uploadBufferSizeBytes,
          rewriteDataOpBatchLimit,
          gcsCountersOptions,
          gcsReadOptions);
    }

    @Override
    GoogleCloudStorage createGoogleCloudStorage(
        GoogleCloudStorageOptions options, Storage storage, Credentials credentials) {
      return googleCloudStorage;
    }
  }

  @Test
  public void testCreate() throws IOException {
    GcsOptions gcsOptions = gcsOptionsWithTestCredential();

    GcsUtilMock gcsUtil = GcsUtilMock.createMock(gcsOptions);

    GoogleCloudStorage mockStorage = Mockito.mock(GoogleCloudStorage.class);
    WritableByteChannel mockChannel = Mockito.mock(WritableByteChannel.class);

    gcsUtil.googleCloudStorage = mockStorage;

    when(mockStorage.create(any(), any())).thenReturn(mockChannel);

    GcsPath path = GcsPath.fromUri("gs://testbucket/testdirectory/otherfile");
    CreateOptions createOptions = CreateOptions.builder().build();

    assertEquals(mockChannel, gcsUtil.create(path, createOptions));
  }

  @Test
  public void testCreateWithException() throws IOException {
    GcsOptions gcsOptions = gcsOptionsWithTestCredential();

    GcsUtilMock gcsUtil = GcsUtilMock.createMock(gcsOptions);

    GoogleCloudStorage mockStorage = Mockito.mock(GoogleCloudStorage.class);

    gcsUtil.googleCloudStorage = mockStorage;

    when(mockStorage.create(any(), any())).thenThrow(new RuntimeException("testException"));

    GcsPath path = GcsPath.fromUri("gs://testbucket/testdirectory/otherfile");
    CreateOptions createOptions = CreateOptions.builder().build();

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("testException");

    gcsUtil.create(path, createOptions);
  }

  private void testWriteMetrics(boolean enabled) throws IOException {
    // arrange
    GcsOptions gcsOptions = PipelineOptionsFactory.create().as(GcsOptions.class);
    gcsOptions.setEnableBucketWriteMetricCounter(enabled);
    gcsOptions.setGcsWriteCounterPrefix("test_counter");
    GcsUtilMock gcsUtil = GcsUtilMock.createMockWithMockStorage(gcsOptions, null);
    byte[] payload = "some_bytes".getBytes(StandardCharsets.UTF_8);
    String bucketName = "some_bucket";

    // act
    try (WritableByteChannel byteChannel =
        gcsUtil.create(new GcsPath(null, bucketName, "o1"), CreateOptions.builder().build())) {
      int bytesWrittenReportedByChannel = byteChannel.write(ByteBuffer.wrap(payload));
      long bytesWrittenReportedByMetric =
          testMetricsContainer
              .getCounter(
                  MetricName.named(
                      GcsUtil.class,
                      String.format("%s_%s", gcsOptions.getGcsWriteCounterPrefix(), bucketName)))
              .getCumulative();

      // assert
      assertEquals(payload.length, bytesWrittenReportedByChannel);
      assertEquals(enabled ? payload.length : 0, bytesWrittenReportedByMetric);
    }
  }

  private void testReadMetrics(boolean enabled, GoogleCloudStorageReadOptions readOptions)
      throws IOException {
    // arrange
    GcsOptions gcsOptions = PipelineOptionsFactory.create().as(GcsOptions.class);
    gcsOptions.setEnableBucketReadMetricCounter(enabled);
    gcsOptions.setGcsReadCounterPrefix("test_counter");
    byte[] payload = "some_bytes".getBytes(StandardCharsets.UTF_8);
    GcsUtilMock gcsUtil = GcsUtilMock.createMockWithMockStorage(gcsOptions, payload);
    String bucketName = "some_bucket";
    GcsPath gcsPath = new GcsPath(null, bucketName, "o1");
    // act
    try (SeekableByteChannel byteChannel =
        readOptions != null ? gcsUtil.open(gcsPath, readOptions) : gcsUtil.open(gcsPath)) {
      int bytesReadReportedByChannel = byteChannel.read(ByteBuffer.allocate(payload.length));
      long bytesReadReportedByMetric =
          testMetricsContainer
              .getCounter(
                  MetricName.named(
                      GcsUtil.class,
                      String.format("%s_%s", gcsOptions.getGcsReadCounterPrefix(), bucketName)))
              .getCumulative();

      // assert
      assertEquals(payload.length, bytesReadReportedByChannel);
      assertEquals(enabled ? payload.length : 0, bytesReadReportedByMetric);
    }
  }

  @Test
  public void testWriteMetricsAreCorrectlyReportedWhenEnabled() throws Exception {
    testWriteMetrics(true);
  }

  @Test
  public void testWriteMetricsAreNotCollectedWhenNotEnabled() throws Exception {
    testWriteMetrics(false);
  }

  @Test
  public void testReadMetricsAreCorrectlyReportedWhenEnabled() throws Exception {
    testReadMetrics(true, null);
  }

  @Test
  public void testReadMetricsAreNotCollectedWhenNotEnabled() throws Exception {
    testReadMetrics(false, null);
  }

  @Test
  public void testReadMetricsAreCorrectlyReportedWhenEnabledOpenWithOptions() throws Exception {
    testReadMetrics(true, GoogleCloudStorageReadOptions.DEFAULT);
  }

  @Test
  public void testReadMetricsAreNotCollectedWhenNotEnabledOpenWithOptions() throws Exception {
    testReadMetrics(false, GoogleCloudStorageReadOptions.DEFAULT);
  }

  /** A helper to wrap a {@link GenericJson} object in a content stream. */
  private static InputStream toStream(String content) throws IOException {
    return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
  }
}

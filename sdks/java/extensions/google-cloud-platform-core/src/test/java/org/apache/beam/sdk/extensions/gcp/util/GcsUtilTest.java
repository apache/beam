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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.paging.Page;
import com.google.api.services.storage.model.Bucket;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.BucketTargetOption;
import com.google.cloud.storage.Storage.PredefinedAcl;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.CreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.fs.MoveOptions.StandardMoveOptions;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
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

  @After
  public void tearDown() {
    // Don't leak the containers installed by setUp into later tests in the same JVM.
    MetricsEnvironment.setProcessWideContainer(null);
    MetricsEnvironment.setCurrentContainer(null);
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

  // The tests below cover the routing that the GcsUtil facade performs for the legacy typed
  // methods once the use_gcsutil_v2 experiment installs a GcsUtilV2 delegate. Both delegates are
  // mocked, so they assert both that V2 is used and that V1 is left alone.

  private GcsUtilV1 mockDelegate;
  private GcsUtilV2 mockDelegateV2;

  private GcsUtil gcsUtilRoutingToV2() {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();
    mockDelegate = Mockito.mock(GcsUtilV1.class);
    mockDelegateV2 = Mockito.mock(GcsUtilV2.class);
    gcsUtil.delegate = mockDelegate;
    gcsUtil.delegateV2 = mockDelegateV2;
    return gcsUtil;
  }

  private static com.google.cloud.storage.Blob mockBlob(String bucket, String object) {
    com.google.cloud.storage.Blob blob = Mockito.mock(com.google.cloud.storage.Blob.class);
    when(blob.getBucket()).thenReturn(bucket);
    when(blob.getName()).thenReturn(object);
    return blob;
  }

  @Test
  public void testCopyIsRoutedToV2AsAnUnconditionalOverwrite() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();

    gcsUtil.copy(ImmutableList.of("gs://bucket/from"), ImmutableList.of("gs://bucket/to"));

    verify(mockDelegateV2)
        .copy(
            ImmutableList.of(GcsPath.fromUri("gs://bucket/from")),
            ImmutableList.of(GcsPath.fromUri("gs://bucket/to")),
            GcsUtilV2.OverwriteStrategy.ALWAYS_OVERWRITE);
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testRenameWithoutOptionsIsRoutedToV2AsAnUnconditionalOverwrite() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();

    gcsUtil.rename(ImmutableList.of("gs://bucket/from"), ImmutableList.of("gs://bucket/to"));

    verify(mockDelegateV2)
        .move(
            ImmutableList.of(GcsPath.fromUri("gs://bucket/from")),
            ImmutableList.of(GcsPath.fromUri("gs://bucket/to")),
            GcsUtilV2.MissingStrategy.FAIL_IF_MISSING,
            GcsUtilV2.OverwriteStrategy.ALWAYS_OVERWRITE);
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testRenameMoveOptionsAreTranslatedForV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();

    gcsUtil.rename(
        ImmutableList.of("gs://bucket/from"),
        ImmutableList.of("gs://bucket/to"),
        StandardMoveOptions.IGNORE_MISSING_FILES,
        StandardMoveOptions.SKIP_IF_DESTINATION_EXISTS);

    verify(mockDelegateV2)
        .move(
            ImmutableList.of(GcsPath.fromUri("gs://bucket/from")),
            ImmutableList.of(GcsPath.fromUri("gs://bucket/to")),
            GcsUtilV2.MissingStrategy.SKIP_IF_MISSING,
            GcsUtilV2.OverwriteStrategy.SKIP_IF_EXISTS);
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testRemoveIsRoutedToV2AndToleratesMissingFiles() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();

    gcsUtil.remove(ImmutableList.of("gs://bucket/one", "gs://bucket/two"));

    verify(mockDelegateV2)
        .remove(
            ImmutableList.of(
                GcsPath.fromUri("gs://bucket/one"), GcsPath.fromUri("gs://bucket/two")),
            GcsUtilV2.MissingStrategy.SKIP_IF_MISSING);
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  /**
   * Blobs and errors are passed through in order. The field-by-field conversion is covered by
   * {@link #testGetObjectIsRoutedToV2AndKeepsAllFields}.
   */
  @Test
  public void testGetObjectsIsRoutedToV2AndConvertsBlobs() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    com.google.cloud.storage.Blob blob = mockBlob("bucket", "found");
    FileNotFoundException notFound = new FileNotFoundException("gs://bucket/missing");
    List<GcsPath> paths =
        ImmutableList.of(GcsPath.fromUri("gs://bucket/found"), GcsPath.fromUri("gs://bucket/miss"));
    when(mockDelegateV2.getBlobs(paths))
        .thenReturn(
            ImmutableList.of(
                GcsUtilV2.BlobResult.create(blob), GcsUtilV2.BlobResult.create(notFound)));

    List<StorageObjectOrIOException> results = gcsUtil.getObjects(paths);

    assertEquals(2, results.size());
    StorageObject converted = results.get(0).storageObject();
    assertNotNull(converted);
    assertEquals("found", converted.getName());
    assertNull(results.get(0).ioException());
    assertSame(notFound, results.get(1).ioException());
    assertNull(results.get(1).storageObject());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testListObjectsIsRoutedToV2AndConvertsAPage() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    com.google.cloud.storage.Blob object = mockBlob("bucket", "prefix/object");
    com.google.cloud.storage.Blob directory = mockBlob("bucket", "prefix/dir/");
    when(directory.isDirectory()).thenReturn(true);
    @SuppressWarnings("unchecked")
    Page<com.google.cloud.storage.Blob> page = Mockito.mock(Page.class);
    when(page.getValues()).thenReturn(ImmutableList.of(object, directory));
    when(page.hasNextPage()).thenReturn(true);
    when(page.getNextPageToken()).thenReturn("next");
    when(mockDelegateV2.listBlobs("bucket", "prefix/", null)).thenReturn(page);

    Objects objects = gcsUtil.listObjects("bucket", "prefix/", null);

    assertEquals(1, objects.getItems().size());
    assertEquals("prefix/object", objects.getItems().get(0).getName());
    assertEquals(ImmutableList.of("prefix/dir/"), objects.getPrefixes());
    assertEquals("next", objects.getNextPageToken());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testListObjectsReportsTheLastPageWithANullToken() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    @SuppressWarnings("unchecked")
    Page<com.google.cloud.storage.Blob> page = Mockito.mock(Page.class);
    when(page.getValues()).thenReturn(ImmutableList.of());
    // A gax page reports an empty token rather than a null one once it is exhausted. Callers of
    // listObjects loop until the token is null, so it has to be normalized.
    when(page.hasNextPage()).thenReturn(false);
    when(page.getNextPageToken()).thenReturn("");
    when(mockDelegateV2.listBlobs("bucket", "prefix/", null)).thenReturn(page);

    Objects objects = gcsUtil.listObjects("bucket", "prefix/", null);

    assertNull(objects.getItems());
    assertNull(objects.getPrefixes());
    assertNull(objects.getNextPageToken());
  }

  /**
   * The storage class is checked here because the emulator ignores it, so only a unit test can see
   * it carried over.
   */
  @Test
  public void testCreateBucketIsRoutedToV2WithProjectPrivateAcls() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    // This is the bucket that GcpOptions.tryCreateDefaultBucketWithPrefix builds, plus a storage
    // class.
    Bucket bucket =
        new Bucket()
            .setName("bucket")
            .setLocation("us-central1")
            .setStorageClass("NEARLINE")
            .setSoftDeletePolicy(new Bucket.SoftDeletePolicy().setRetentionDurationSeconds(0L));

    gcsUtil.createBucket("a-project", bucket);

    verify(mockDelegateV2)
        .createBucket(
            "a-project",
            BucketInfo.newBuilder("bucket")
                .setLocation("us-central1")
                .setStorageClass(StorageClass.NEARLINE)
                .setSoftDeletePolicy(
                    BucketInfo.SoftDeletePolicy.newBuilder()
                        .setRetentionDuration(java.time.Duration.ZERO)
                        .build())
                .build(),
            BucketTargetOption.predefinedAcl(PredefinedAcl.PROJECT_PRIVATE),
            BucketTargetOption.predefinedDefaultObjectAcl(PredefinedAcl.PROJECT_PRIVATE));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testRemoveBucketIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();

    gcsUtil.removeBucket(new Bucket().setName("bucket"));

    verify(mockDelegateV2).removeBucket(BucketInfo.of("bucket"));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testBucketOwnerIsRoutedToV2BucketProject() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    when(mockDelegateV2.bucketProject(path)).thenReturn(123L);

    assertEquals(123L, gcsUtil.bucketOwner(path));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testGetObjectIsRoutedToV2AndKeepsAllFields() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    com.google.cloud.storage.Blob blob = mockBlob("bucket", "object");
    when(blob.getSize()).thenReturn(42L);
    when(blob.getGeneration()).thenReturn(7L);
    when(blob.getMetageneration()).thenReturn(3L);
    when(blob.getContentType()).thenReturn("text/csv");
    when(blob.getContentEncoding()).thenReturn("gzip");
    when(blob.getMd5()).thenReturn("md5==");
    when(blob.getCrc32c()).thenReturn("crc==");
    when(blob.getEtag()).thenReturn("etag");
    when(blob.getUpdateTimeOffsetDateTime())
        .thenReturn(java.time.Instant.ofEpochMilli(1234L).atOffset(java.time.ZoneOffset.UTC));
    when(blob.getCreateTimeOffsetDateTime())
        .thenReturn(java.time.Instant.ofEpochMilli(1000L).atOffset(java.time.ZoneOffset.UTC));
    when(mockDelegateV2.getBlob(path)).thenReturn(blob);

    StorageObject object = gcsUtil.getObject(path);

    assertEquals("bucket", object.getBucket());
    assertEquals("object", object.getName());
    assertEquals(BigInteger.valueOf(42L), object.getSize());
    assertEquals(Long.valueOf(7L), object.getGeneration());
    assertEquals(Long.valueOf(3L), object.getMetageneration());
    assertEquals("text/csv", object.getContentType());
    assertEquals("gzip", object.getContentEncoding());
    assertEquals("md5==", object.getMd5Hash());
    assertEquals("crc==", object.getCrc32c());
    assertEquals("etag", object.getEtag());
    assertEquals(1234L, object.getUpdated().getValue());
    assertEquals(1000L, object.getTimeCreated().getValue());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  /** Fields a blob may not carry (e.g. when fetched with a field mask) are left unset. */
  @Test
  public void testGetObjectLeavesMissingFieldsUnsetForV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    com.google.cloud.storage.Blob blob = mockBlob("bucket", "object");
    // Mockito would otherwise answer 0 for the boxed size.
    when(blob.getSize()).thenReturn(null);
    when(mockDelegateV2.getBlob(path)).thenReturn(blob);

    StorageObject object = gcsUtil.getObject(path);

    assertEquals("object", object.getName());
    assertNull(object.getSize());
    assertNull(object.getUpdated());
    assertNull(object.getTimeCreated());
  }

  @Test
  public void testCreateWithTypeIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");

    gcsUtil.create(path, "text/plain");
    gcsUtil.create(path, "text/plain", 1024);

    verify(mockDelegateV2)
        .create(path, GcsUtilV1.CreateOptions.builder().setContentType("text/plain").build());
    verify(mockDelegateV2)
        .create(
            path,
            GcsUtilV1.CreateOptions.builder()
                .setContentType("text/plain")
                .setUploadBufferSizeBytes(1024)
                .build());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  /** GcpOptions reads the soft delete policy of the temp bucket through this method. */
  @Test
  public void testGetBucketIsRoutedToV2AndConvertsTheBucket() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    com.google.cloud.storage.Bucket bucket = Mockito.mock(com.google.cloud.storage.Bucket.class);
    when(bucket.getName()).thenReturn("bucket");
    when(bucket.getLocation()).thenReturn("US-CENTRAL1");
    when(bucket.getProject()).thenReturn(BigInteger.valueOf(123L));
    when(bucket.getStorageClass()).thenReturn(StorageClass.NEARLINE);
    when(bucket.getSoftDeletePolicy())
        .thenReturn(
            BucketInfo.SoftDeletePolicy.newBuilder()
                .setRetentionDuration(java.time.Duration.ofDays(7))
                .build());
    when(mockDelegateV2.getBucket(path)).thenReturn(bucket);

    Bucket converted = gcsUtil.getBucket(path);

    assertNotNull(converted);
    assertEquals("bucket", converted.getName());
    assertEquals("US-CENTRAL1", converted.getLocation());
    assertEquals(BigInteger.valueOf(123L), converted.getProjectNumber());
    assertEquals("NEARLINE", converted.getStorageClass());
    assertEquals(
        Long.valueOf(java.time.Duration.ofDays(7).getSeconds()),
        converted.getSoftDeletePolicy().getRetentionDurationSeconds());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testGetBucketWithoutSoftDeletePolicyForV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    com.google.cloud.storage.Bucket bucket = Mockito.mock(com.google.cloud.storage.Bucket.class);
    when(bucket.getName()).thenReturn("bucket");
    when(mockDelegateV2.getBucket(path)).thenReturn(bucket);

    Bucket converted = gcsUtil.getBucket(path);

    assertNotNull(converted);
    assertNull(converted.getSoftDeletePolicy());
    assertNull(converted.getStorageClass());
  }

  /** Without the use_gcsutil_v2 experiment, the V2-only methods fail rather than fall back. */
  @Test
  public void testV2OnlyMethodsFailWithoutV2() {
    GcsUtil gcsUtil = gcsOptionsWithTestCredential().getGcsUtil();
    assertNull(gcsUtil.delegateV2);
    GcsUtilV1 v1 = Mockito.mock(GcsUtilV1.class);
    gcsUtil.delegate = v1;
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    List<GcsPath> paths = ImmutableList.of(path);

    List<ThrowingRunnable> calls =
        ImmutableList.of(
            () -> gcsUtil.getBlob(path),
            () -> gcsUtil.getBlobs(paths),
            () -> gcsUtil.listBlobs("bucket", "prefix", null),
            () -> gcsUtil.listBlobs("bucket", "prefix", null, "/"),
            () -> gcsUtil.openV2(path),
            () -> gcsUtil.createV2(path, GcsUtil.CreateOptions.builder().build()),
            () -> gcsUtil.createBucket(BucketInfo.of("bucket")),
            () -> gcsUtil.getBucketWithOptions(path),
            () -> gcsUtil.removeBucket(BucketInfo.of("bucket")),
            () -> gcsUtil.copyV2(paths, paths),
            () -> gcsUtil.copy(paths, paths, GcsUtilV2.OverwriteStrategy.ALWAYS_OVERWRITE),
            () -> gcsUtil.renameV2(paths, paths),
            () ->
                gcsUtil.rename(
                    paths,
                    paths,
                    GcsUtilV2.MissingStrategy.FAIL_IF_MISSING,
                    GcsUtilV2.OverwriteStrategy.ALWAYS_OVERWRITE),
            () -> gcsUtil.removeV2(paths),
            () -> gcsUtil.remove(paths, GcsUtilV2.MissingStrategy.FAIL_IF_MISSING));

    for (ThrowingRunnable call : calls) {
      IOException e = assertThrows(IOException.class, call);
      assertEquals("GcsUtil V2 not initialized.", e.getMessage());
    }
    Mockito.verifyNoInteractions(v1);
  }

  @Test
  public void testExpandIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath pattern = GcsPath.fromUri("gs://bucket/prefix/*");
    List<GcsPath> expanded = ImmutableList.of(GcsPath.fromUri("gs://bucket/prefix/a"));
    when(mockDelegateV2.expand(pattern)).thenReturn(expanded);

    assertSame(expanded, gcsUtil.expand(pattern));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testFileSizeIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    when(mockDelegateV2.fileSize(path)).thenReturn(42L);

    assertEquals(42L, gcsUtil.fileSize(path));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  /**
   * Only the routing of the delimiter overload is checked. The page conversion is covered by {@link
   * #testListObjectsIsRoutedToV2AndConvertsAPage}.
   */
  @Test
  public void testListObjectsWithDelimiterIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    com.google.cloud.storage.Blob object = mockBlob("bucket", "prefix/object");
    @SuppressWarnings("unchecked")
    Page<com.google.cloud.storage.Blob> page = Mockito.mock(Page.class);
    when(page.getValues()).thenReturn(ImmutableList.of(object));
    when(mockDelegateV2.listBlobs("bucket", "prefix/", "token", "/")).thenReturn(page);

    Objects objects = gcsUtil.listObjects("bucket", "prefix/", "token", "/");

    assertEquals("prefix/object", objects.getItems().get(0).getName());
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testOpenIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");
    SeekableByteChannel channel = Mockito.mock(SeekableByteChannel.class);
    when(mockDelegateV2.open(path)).thenReturn(channel);

    assertSame(channel, gcsUtil.open(path));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testVerifyBucketAccessibleIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath path = GcsPath.fromUri("gs://bucket/object");

    gcsUtil.verifyBucketAccessible(path);

    verify(mockDelegateV2).verifyBucketAccessible(path);
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  @Test
  public void testBucketAccessibleIsRoutedToV2() throws IOException {
    GcsUtil gcsUtil = gcsUtilRoutingToV2();
    GcsPath accessible = GcsPath.fromUri("gs://accessible/object");
    GcsPath inaccessible = GcsPath.fromUri("gs://inaccessible/object");
    when(mockDelegateV2.bucketAccessible(accessible)).thenReturn(true);
    when(mockDelegateV2.bucketAccessible(inaccessible)).thenReturn(false);

    assertTrue(gcsUtil.bucketAccessible(accessible));
    assertFalse(gcsUtil.bucketAccessible(inaccessible));
    Mockito.verifyNoMoreInteractions(mockDelegate);
  }

  // The tests below exercise a real GcsUtilV2 delegate whose java-storage client is mocked, to
  // cover behavior that GcsUtilV2 must share with GcsUtilV1.
  // TODO: Move these to a parity test that runs against both delegates.

  /**
   * Returns a {@link GcsUtil} backed by a real {@link GcsUtilV2} that issues every call to {@code
   * storage}. Performance metrics are off by default, so the per-operation clients of {@link
   * GcsUtilV2#storageWithHttpMetrics} resolve to this one as well.
   */
  private GcsUtil gcsUtilWithV2Storage(com.google.cloud.storage.Storage storage) {
    GcsOptions options = gcsOptionsWithTestCredential();
    options.setProject("my_project");
    GcsUtil gcsUtil = options.getGcsUtil();
    GcsUtilV2 delegateV2 = Mockito.spy(new GcsUtilV2(options));
    Mockito.doReturn(storage).when(delegateV2).storage();
    gcsUtil.delegateV2 = delegateV2;
    return gcsUtil;
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

  /**
   * Mirrors {@link GcsUtilV1Test#testGCSReadMetricsIsSet} for V2: opening a missing object records
   * a {@code not_found} request, even though V2 detects it through a lookup rather than a {@link
   * StorageException}.
   */
  @Test
  public void testV2OpenMissingObjectRecordsNotFoundMetric() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    // An unstubbed get() returns null, which is how java-storage reports a missing object.
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    assertThrows(
        FileNotFoundException.class,
        () -> gcsUtil.open(GcsPath.fromComponents("testbucket", "testobject")));

    verifyMetricWasSet("my_project", "testbucket", "GcsGet", "not_found", 1);
    verifyMetricWasSet("my_project", "testbucket", "GcsGet", "ok", 0);
  }

  /**
   * An upload is finalized when its channel is closed, so a failed precondition surfaces there. V2
   * must report it as an {@link IOException}, as V1 does, rather than an unchecked {@link
   * StorageException}.
   */
  @Test
  public void testV2WriteChannelCloseTranslatesStorageException() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    WriteChannel writer = Mockito.mock(WriteChannel.class);
    StorageException preconditionFailed = new StorageException(412, "Precondition Failed");
    Mockito.doThrow(preconditionFailed).when(writer).close();
    when(storage.writer(any(com.google.cloud.storage.BlobInfo.class), any())).thenReturn(writer);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    WritableByteChannel channel =
        gcsUtil.create(
            GcsPath.fromComponents("testbucket", "testobject"),
            CreateOptions.builder().setExpectFileToNotExist(true).build());

    IOException thrown = assertThrows(IOException.class, channel::close);
    assertSame(preconditionFailed, thrown.getCause());
  }

  /** Mirrors {@link GcsUtilV1Test#testBucketDoesNotExist} for V2. */
  @Test
  public void testV2BucketAccessibleIsFalseWhenBucketDoesNotExist() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    // An unstubbed get() returns null, which is how java-storage reports a missing bucket.
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    assertFalse(gcsUtil.bucketAccessible(GcsPath.fromComponents("testbucket", "testobject")));
  }

  /** Mirrors {@link GcsUtilV1Test#testBucketDoesNotExistBecauseOfAccessError} for V2. */
  @Test
  public void testV2BucketAccessibleIsFalseWhenAccessIsDenied() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    when(storage.get(Mockito.eq("testbucket"), any(BucketGetOption.class)))
        .thenThrow(new StorageException(403, "Forbidden"));
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    assertFalse(gcsUtil.bucketAccessible(GcsPath.fromComponents("testbucket", "testobject")));
  }

  /**
   * Any other failure (e.g. a 5xx) says nothing about whether the bucket is accessible, so it must
   * propagate rather than be reported as an inaccessible bucket, as V1 does.
   */
  @Test
  public void testV2BucketAccessiblePropagatesOtherFailures() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    StorageException serverError = new StorageException(503, "Service Unavailable");
    when(storage.get(Mockito.eq("testbucket"), any(BucketGetOption.class))).thenThrow(serverError);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    IOException thrown =
        assertThrows(
            IOException.class,
            () -> gcsUtil.bucketAccessible(GcsPath.fromComponents("testbucket", "testobject")));
    assertSame(serverError, thrown.getCause());
  }
}

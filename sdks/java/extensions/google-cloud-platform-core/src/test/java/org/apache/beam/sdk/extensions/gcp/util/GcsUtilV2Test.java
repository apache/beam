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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.hadoop.util.AsyncWriteChannelOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileAlreadyExistsException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.CreateOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil.StorageObjectOrIOException;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Unit tests for {@link GcsUtilV2}.
 *
 * <p>Covers three things without making any request to GCS:
 *
 * <ul>
 *   <li>The storage client is configured from the pipeline options (project, credentials,
 *       endpoint), matching what {@link GcsUtilV1} honors.
 *   <li>HTTP metrics and byte counters are installed only when their flags ask for them, and only
 *       when there is a container to report into. The counting logic itself is covered by {@link
 *       TransportTest}.
 *   <li>The results and exceptions that callers depend on match those of {@link GcsUtilV1}, for the
 *       responses of a mocked java-storage client.
 * </ul>
 *
 * <p>End-to-end parity with {@link GcsUtilV1} against real GCS is covered by {@link
 * GcsUtilParameterizedIT}.
 */
@RunWith(JUnit4.class)
public class GcsUtilV2Test {

  private static final String BUCKET = "test-bucket";
  private static final String READ_PREFIX = "test_read_prefix";
  private static final String WRITE_PREFIX = "test_write_prefix";
  private static final byte[] PAYLOAD = "some_bytes".getBytes(UTF_8);

  @After
  public void tearDown() {
    MetricsEnvironment.setProcessWideContainer(null);
    MetricsEnvironment.setCurrentContainer(null);
  }

  private static GcsOptions gcsOptions() {
    GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
    // Avoid resolving application default credentials; no request leaves the process.
    options.setGcpCredential(new TestCredential());
    options.setProject("test-project");
    return options;
  }

  private static GcsUtilV2 gcsUtilV2(boolean performanceMetrics) {
    GcsOptions options = gcsOptions();
    options.setGcsPerformanceMetrics(performanceMetrics);
    return new GcsUtilV2(options);
  }

  private static GcsUtilV2 gcsUtilV2(boolean performanceMetrics, boolean bucketCounters) {
    GcsOptions options = gcsOptions();
    options.setGcsPerformanceMetrics(performanceMetrics);
    options.setEnableBucketReadMetricCounter(bucketCounters);
    options.setEnableBucketWriteMetricCounter(bucketCounters);
    options.setGcsReadCounterPrefix(READ_PREFIX);
    options.setGcsWriteCounterPrefix(WRITE_PREFIX);
    return new GcsUtilV2(options);
  }

  /** The shared (unscoped) storage client of {@code gcsUtil}. */
  private static StorageOptions storageOptionsOf(GcsUtilV2 gcsUtil) {
    return gcsUtil.storageWithHttpMetrics(null, false).getOptions();
  }

  // ---------------------------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testStorageClientUsesPipelineProject() {
    assertEquals("test-project", storageOptionsOf(gcsUtilV2(false)).getProjectId());
  }

  @Test
  public void testStorageClientUsesPipelineCredentials() {
    GcsOptions options = gcsOptions();
    Credentials credentials = options.getGcpCredential();

    assertSame(credentials, storageOptionsOf(new GcsUtilV2(options)).getCredentials());
  }

  @Test
  public void testNullCredentialMapsToNoCredentials() {
    GcsOptions options = gcsOptions();
    options.setCredentialFactoryClass(NoopCredentialFactory.class);
    options.setGcpCredential(null);

    assertSame(
        NoCredentials.getInstance(), storageOptionsOf(new GcsUtilV2(options)).getCredentials());
  }

  @Test
  public void testDefaultHostWithoutGcsEndpoint() {
    String defaultHost =
        StorageOptions.newBuilder()
            .setProjectId("test-project")
            .setCredentials(NoCredentials.getInstance())
            .build()
            .getHost();

    assertEquals(defaultHost, storageOptionsOf(gcsUtilV2(false)).getHost());
  }

  /** Mirrors {@code GcsUtilV1Test#testGcsEndpoint}: only the root of the endpoint applies to V2. */
  @Test
  public void testGcsEndpointRootIsUsedAsHost() {
    GcsOptions options = gcsOptions();
    options.setGcsEndpoint("http://localhost:4443/storage/v1/");

    assertEquals("http://localhost:4443", storageOptionsOf(new GcsUtilV2(options)).getHost());
  }

  @Test
  public void testGcsEndpointWithoutPort() {
    GcsOptions options = gcsOptions();
    options.setGcsEndpoint("https://storage.example.com/storage/v1/");

    assertEquals("https://storage.example.com", storageOptionsOf(new GcsUtilV2(options)).getHost());
  }

  @Test
  public void testInvalidGcsEndpointIsRejected() {
    GcsOptions options = gcsOptions();
    options.setGcsEndpoint("not a url");

    assertThrows(IllegalArgumentException.class, () -> new GcsUtilV2(options));
  }

  // ---------------------------------------------------------------------------------------------
  // HTTP metrics binding
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testSharedClientIsReusedWithoutAContainer() {
    GcsUtilV2 gcsUtil = gcsUtilV2(true);

    Storage read = gcsUtil.storageWithHttpMetrics(null, false);
    Storage write = gcsUtil.storageWithHttpMetrics(null, true);

    // With nothing to count, no per-operation client should be built.
    assertSame(read, write);
  }

  @Test
  public void testScopedClientIsBuiltForAContainer() {
    GcsUtilV2 gcsUtil = gcsUtilV2(true);
    MetricsContainerImpl container = new MetricsContainerImpl(null);

    Storage shared = gcsUtil.storageWithHttpMetrics(null, false);
    Storage scoped = gcsUtil.storageWithHttpMetrics(container, false);

    assertNotSame(shared, scoped);
    assertNotSame(
        shared.getOptions().getTransportOptions(), scoped.getOptions().getTransportOptions());
    // Everything other than the transport must carry over from the shared client.
    assertEquals(shared.getOptions().getProjectId(), scoped.getOptions().getProjectId());
    assertEquals(shared.getOptions().getHost(), scoped.getOptions().getHost());
    assertSame(shared.getOptions().getCredentials(), scoped.getOptions().getCredentials());
  }

  private static long counter(MetricsContainerImpl container, String name) {
    return container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, name)).getCumulative();
  }

  /** Executes one request through {@code initializer} against a mock transport. */
  private static void executeOneRequest(HttpRequestInitializer initializer) throws IOException {
    MockHttpTransport transport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(new MockLowLevelHttpResponse().setStatusCode(200))
            .build();
    transport
        .createRequestFactory(initializer)
        .buildGetRequest(new GenericUrl("https://storage.googleapis.com/test"))
        .execute();
  }

  private static HttpRequestInitializer initializerOf(Storage client) {
    return ((HttpTransportOptions) client.getOptions().getTransportOptions())
        .getHttpRequestInitializer(client.getOptions());
  }

  /**
   * The scoped client must hand out a request initializer that counts, since that is the only way
   * the HTTP counters reach the container.
   */
  @Test
  public void testScopedClientInitializerCountsRequests() throws IOException {
    GcsUtilV2 gcsUtil = gcsUtilV2(true);
    MetricsContainerImpl container = new MetricsContainerImpl(null);

    executeOneRequest(initializerOf(gcsUtil.storageWithHttpMetrics(container, false)));

    assertEquals(1, counter(container, "gcs_http_read_request_count"));
    assertEquals(1, counter(container, "gcs_http_read_status_2xx"));
  }

  @Test
  public void testWriteDirectionIsCountedSeparately() throws IOException {
    GcsUtilV2 gcsUtil = gcsUtilV2(true);
    MetricsContainerImpl container = new MetricsContainerImpl(null);

    executeOneRequest(initializerOf(gcsUtil.storageWithHttpMetrics(container, true)));

    assertEquals(1, counter(container, "gcs_http_write_request_count"));
    assertEquals(0, counter(container, "gcs_http_read_request_count"));
  }

  /**
   * The flag is the actual gate: with performance metrics disabled no container is resolved, so
   * open() and create() pass null and no scoped client or HTTP counter is ever produced.
   */
  @Test
  public void testPerformanceMetricsFlagGatesTheContainer() {
    MetricsContainerImpl current = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(current);
    try {
      assertSame(current, gcsUtilV2(true).performanceMetricsContainer());
      assertNull(gcsUtilV2(false).performanceMetricsContainer());
    } finally {
      MetricsEnvironment.setCurrentContainer(null);
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Byte counters (mirrors GcsUtilV1Test#testReadMetrics / #testWriteMetrics for V1)
  // ---------------------------------------------------------------------------------------------

  private static @Nullable Long counterOrNull(MetricsContainerImpl container, MetricName name) {
    CounterCell cell = container.tryGetCounter(name);
    return cell == null ? null : cell.getCumulative();
  }

  private static @Nullable Long bucketCounter(MetricsContainerImpl container, String prefix) {
    return counterOrNull(container, MetricName.named(GcsUtil.class, prefix + "_" + BUCKET));
  }

  private static @Nullable Long gcsCounter(MetricsContainerImpl container, String name) {
    return counterOrNull(container, MetricName.named(GcsUtil.METRIC_NAMESPACE, name));
  }

  /** Reads {@link #PAYLOAD} through V2's read wrapper, bound to {@code bound}. */
  private static void readPayload(GcsUtilV2 gcsUtil, @Nullable MetricsContainerImpl bound)
      throws IOException {
    try (SeekableByteChannel channel =
        gcsUtil.wrapInCounting(new SeekableInMemoryByteChannel(PAYLOAD), BUCKET, bound)) {
      assertEquals(PAYLOAD.length, channel.read(ByteBuffer.allocate(PAYLOAD.length)));
    }
  }

  /** Writes {@link #PAYLOAD} through V2's write wrapper, bound to {@code bound}. */
  private static void writePayload(GcsUtilV2 gcsUtil, @Nullable MetricsContainerImpl bound)
      throws IOException {
    try (WritableByteChannel channel =
        gcsUtil.wrapInCounting(Channels.newChannel(new ByteArrayOutputStream()), BUCKET, bound)) {
      assertEquals(PAYLOAD.length, channel.write(ByteBuffer.wrap(PAYLOAD)));
    }
  }

  @Test
  public void testReadCountersWhenAllEnabled() throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);

    readPayload(gcsUtilV2(true, true), container);

    assertEquals(Long.valueOf(PAYLOAD.length), bucketCounter(container, READ_PREFIX));
    assertEquals(
        Long.valueOf(PAYLOAD.length), gcsCounter(container, "gcs_http_read_wire_bytes_received"));
    assertNull(bucketCounter(container, WRITE_PREFIX));
    assertNull(gcsCounter(container, "gcs_http_write_wire_bytes_sent"));
  }

  @Test
  public void testWriteCountersWhenAllEnabled() throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);

    writePayload(gcsUtilV2(true, true), container);

    assertEquals(Long.valueOf(PAYLOAD.length), bucketCounter(container, WRITE_PREFIX));
    assertEquals(
        Long.valueOf(PAYLOAD.length), gcsCounter(container, "gcs_http_write_wire_bytes_sent"));
    assertNull(bucketCounter(container, READ_PREFIX));
    assertNull(gcsCounter(container, "gcs_http_read_wire_bytes_received"));
  }

  @Test
  public void testChannelsAreNotWrappedWhenAllDisabled() throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);
    GcsUtilV2 gcsUtil = gcsUtilV2(false, false);
    SeekableByteChannel readChannel = new SeekableInMemoryByteChannel(PAYLOAD);
    WritableByteChannel writeChannel = Channels.newChannel(new ByteArrayOutputStream());

    assertSame(readChannel, gcsUtil.wrapInCounting(readChannel, BUCKET, container));
    assertSame(writeChannel, gcsUtil.wrapInCounting(writeChannel, BUCKET, container));
  }

  @Test
  public void testOnlyBucketCountersWhenPerformanceMetricsDisabled() throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);
    GcsUtilV2 gcsUtil = gcsUtilV2(false, true);

    readPayload(gcsUtil, container);
    writePayload(gcsUtil, container);

    assertEquals(Long.valueOf(PAYLOAD.length), bucketCounter(container, READ_PREFIX));
    assertEquals(Long.valueOf(PAYLOAD.length), bucketCounter(container, WRITE_PREFIX));
    assertNull(gcsCounter(container, "gcs_http_read_wire_bytes_received"));
    assertNull(gcsCounter(container, "gcs_http_write_wire_bytes_sent"));
  }

  @Test
  public void testOnlyWireBytesWhenBucketCountersDisabled() throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setCurrentContainer(container);
    GcsUtilV2 gcsUtil = gcsUtilV2(true, false);

    readPayload(gcsUtil, container);
    writePayload(gcsUtil, container);

    assertNull(bucketCounter(container, READ_PREFIX));
    assertNull(bucketCounter(container, WRITE_PREFIX));
    assertEquals(
        Long.valueOf(PAYLOAD.length), gcsCounter(container, "gcs_http_read_wire_bytes_received"));
    assertEquals(
        Long.valueOf(PAYLOAD.length), gcsCounter(container, "gcs_http_write_wire_bytes_sent"));
  }

  /** Without a bound container there is nowhere to report wire bytes, so nothing is wrapped. */
  @Test
  public void testWireBytesRequireABoundContainer() {
    GcsUtilV2 gcsUtil = gcsUtilV2(true, false);
    SeekableByteChannel readChannel = new SeekableInMemoryByteChannel(PAYLOAD);
    WritableByteChannel writeChannel = Channels.newChannel(new ByteArrayOutputStream());

    assertSame(readChannel, gcsUtil.wrapInCounting(readChannel, BUCKET, null));
    assertSame(writeChannel, gcsUtil.wrapInCounting(writeChannel, BUCKET, null));
  }

  /**
   * Wire bytes go to the container bound when the channel was created, even if a different
   * container is current while the bytes are read or written.
   */
  @Test
  public void testWireBytesAreAttributedToTheBoundContainer() throws IOException {
    MetricsContainerImpl bound = new MetricsContainerImpl("bound");
    MetricsContainerImpl other = new MetricsContainerImpl("other");
    GcsUtilV2 gcsUtil = gcsUtilV2(true, false);
    SeekableByteChannel readChannel =
        gcsUtil.wrapInCounting(new SeekableInMemoryByteChannel(PAYLOAD), BUCKET, bound);
    WritableByteChannel writeChannel =
        gcsUtil.wrapInCounting(Channels.newChannel(new ByteArrayOutputStream()), BUCKET, bound);

    MetricsEnvironment.setCurrentContainer(other);
    readChannel.read(ByteBuffer.allocate(PAYLOAD.length));
    writeChannel.write(ByteBuffer.wrap(PAYLOAD));
    readChannel.close();
    writeChannel.close();

    assertEquals(
        Long.valueOf(PAYLOAD.length), gcsCounter(bound, "gcs_http_read_wire_bytes_received"));
    assertEquals(Long.valueOf(PAYLOAD.length), gcsCounter(bound, "gcs_http_write_wire_bytes_sent"));
    assertNull(gcsCounter(other, "gcs_http_read_wire_bytes_received"));
    assertNull(gcsCounter(other, "gcs_http_write_wire_bytes_sent"));
  }

  /**
   * The chunk size decides how many requests a write costs, and the two clients do not default to
   * the same one, so a drift here is a silent throughput regression rather than a test failure.
   */
  @Test
  public void testDefaultUploadChunkSizeMatchesV1() {
    assertEquals(
        AsyncWriteChannelOptions.DEFAULT.getUploadChunkSize(),
        GcsUtilV2.DEFAULT_UPLOAD_CHUNK_SIZE_BYTES);
  }

  // ---------------------------------------------------------------------------------------------
  // Behavior shared with GcsUtilV1, through a mocked java-storage client. Each test mirrors the
  // GcsUtilV1Test case it names; end-to-end parity is covered by GcsUtilParameterizedIT.
  // ---------------------------------------------------------------------------------------------

  /**
   * Returns a {@link GcsUtil} backed by a real {@link GcsUtilV2} that issues every call to {@code
   * storage}. Performance metrics are off by default, so the per-operation clients of {@link
   * GcsUtilV2#storageWithHttpMetrics} resolve to this one as well.
   */
  private GcsUtil gcsUtilWithV2Storage(com.google.cloud.storage.Storage storage) {
    return gcsUtilWithV2Storage(gcsOptions(), storage);
  }

  /** As {@link #gcsUtilWithV2Storage(Storage)}, but configured from {@code options}. */
  private GcsUtil gcsUtilWithV2Storage(
      GcsOptions options, com.google.cloud.storage.Storage storage) {
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
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
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

  private static com.google.cloud.storage.Blob mockBlob(String bucket, String object, long size) {
    com.google.cloud.storage.Blob blob = Mockito.mock(com.google.cloud.storage.Blob.class);
    when(blob.getBucket()).thenReturn(bucket);
    when(blob.getName()).thenReturn(object);
    when(blob.getSize()).thenReturn(size);
    return blob;
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testFileSizeNonBatch}, plus the lookups that {@code getObject} and
   * a literal {@code expand} make.
   */
  @Test
  public void testV2FileSizeAndGetObject() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    com.google.cloud.storage.Blob blob = mockBlob("testbucket", "testobject", 1000L);
    when(storage.get(Mockito.eq("testbucket"), Mockito.eq("testobject"), any())).thenReturn(blob);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertEquals(1000, gcsUtil.fileSize(path));
    StorageObject object = gcsUtil.getObject(path);
    assertEquals("testbucket", object.getBucket());
    assertEquals("testobject", object.getName());
    assertEquals(BigInteger.valueOf(1000), object.getSize());
    assertEquals(ImmutableList.of(path), gcsUtil.expand(path));
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testFileSizeWhenFileNotFoundNonBatch} and {@link
   * GcsUtilV1Test#testNonExistentObjectReturnsEmptyResult}.
   */
  @Test
  public void testV2MissingObject() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    // An unstubbed get() returns null, which is how java-storage reports a missing object.
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertThrows(FileNotFoundException.class, () -> gcsUtil.fileSize(path));
    assertThrows(FileNotFoundException.class, () -> gcsUtil.getObject(path));
    assertEquals(ImmutableList.of(), gcsUtil.expand(path));
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testAccessDeniedObjectThrowsIOException}. V1 reports a plain
   * {@link IOException}; V2 reports the more specific {@link AccessDeniedException} (G4).
   */
  @Test
  public void testV2AccessDeniedObject() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    when(storage.get(Mockito.eq("testbucket"), Mockito.eq("testobject"), any()))
        .thenThrow(new StorageException(403, "Forbidden"));
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertThrows(AccessDeniedException.class, () -> gcsUtil.fileSize(path));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.getObject(path));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.expand(path));
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testBucketAccessible}, {@link
   * GcsUtilV1Test#testVerifyBucketAccessible} and {@link GcsUtilV1Test#testGetBucket}.
   */
  @Test
  public void testV2ExistingBucket() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    com.google.cloud.storage.Bucket bucket = Mockito.mock(com.google.cloud.storage.Bucket.class);
    when(bucket.getName()).thenReturn("testbucket");
    when(bucket.getProject()).thenReturn(BigInteger.valueOf(12345));
    when(storage.get(Mockito.eq("testbucket"), any())).thenReturn(bucket);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertTrue(gcsUtil.bucketAccessible(path));
    gcsUtil.verifyBucketAccessible(path);
    assertEquals("testbucket", gcsUtil.getBucket(path).getName());
    assertEquals(12345L, gcsUtil.bucketOwner(path));
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testVerifyBucketAccessibleDoesNotExist} and {@link
   * GcsUtilV1Test#testGetBucketNotExists}.
   */
  @Test
  public void testV2VerifyBucketAccessibleWhenBucketDoesNotExist() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    // An unstubbed get() returns null, which is how java-storage reports a missing bucket.
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertThrows(FileNotFoundException.class, () -> gcsUtil.verifyBucketAccessible(path));
    assertThrows(FileNotFoundException.class, () -> gcsUtil.getBucket(path));
    assertThrows(FileNotFoundException.class, () -> gcsUtil.bucketOwner(path));
  }

  /** Mirrors {@link GcsUtilV1Test#testVerifyBucketAccessibleAccessError}. */
  @Test
  public void testV2VerifyBucketAccessibleWhenAccessIsDenied() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    when(storage.get(Mockito.eq("testbucket"), any()))
        .thenThrow(new StorageException(403, "Forbidden"));
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertThrows(AccessDeniedException.class, () -> gcsUtil.verifyBucketAccessible(path));
    assertThrows(AccessDeniedException.class, () -> gcsUtil.getBucket(path));
  }

  /** A java-storage client whose uploads open {@code writer}. */
  private static com.google.cloud.storage.Storage storageWritingTo(WriteChannel writer) {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    when(storage.writer(any(BlobInfo.class), any())).thenReturn(writer);
    return storage;
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testCreate}. An upload that expects no object, or finds none, may
   * only create one. Otherwise it may only replace the generation it saw, so that a concurrent
   * change fails the upload rather than being overwritten, as gcsio does for V1 (G8).
   */
  @Test
  public void testV2CreatePreconditions() throws IOException {
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    // Expected not to exist: no lookup is made.
    com.google.cloud.storage.Storage expectedMissing =
        storageWritingTo(Mockito.mock(WriteChannel.class));
    gcsUtilWithV2Storage(expectedMissing)
        .create(path, CreateOptions.builder().setExpectFileToNotExist(true).build());
    verify(expectedMissing).writer(any(BlobInfo.class), Mockito.eq(BlobWriteOption.doesNotExist()));
    verify(expectedMissing, never()).get(anyString(), anyString(), any());

    // Looked up and missing. An unstubbed get() returns null, which is how java-storage reports a
    // missing object.
    com.google.cloud.storage.Storage missing = storageWritingTo(Mockito.mock(WriteChannel.class));
    gcsUtilWithV2Storage(missing).create(path, CreateOptions.builder().build());
    verify(missing).writer(any(BlobInfo.class), Mockito.eq(BlobWriteOption.doesNotExist()));

    // Looked up and found.
    com.google.cloud.storage.Storage existing = storageWritingTo(Mockito.mock(WriteChannel.class));
    com.google.cloud.storage.Blob blob = Mockito.mock(com.google.cloud.storage.Blob.class);
    when(blob.getGeneration()).thenReturn(42L);
    when(existing.get(Mockito.eq("testbucket"), Mockito.eq("testobject"), any())).thenReturn(blob);
    gcsUtilWithV2Storage(existing).create(path, CreateOptions.builder().build());
    verify(existing).writer(any(BlobInfo.class), Mockito.eq(BlobWriteOption.generationMatch(42L)));
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testUploadBufferSizeDefault} and {@link
   * GcsUtilV1Test#testUploadBufferSizeUserSpecified}: the upload chunk size comes from the create
   * options, then the pipeline options, then the default.
   */
  @Test
  public void testV2UploadChunkSize() throws IOException {
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");
    CreateOptions withSize =
        CreateOptions.builder()
            .setExpectFileToNotExist(true)
            .setUploadBufferSizeBytes(1024)
            .build();
    CreateOptions withoutSize = CreateOptions.builder().setExpectFileToNotExist(true).build();
    GcsOptions pipelineWithSize = gcsOptions();
    pipelineWithSize.setGcsUploadBufferSizeBytes(2048);

    WriteChannel writer = Mockito.mock(WriteChannel.class);
    gcsUtilWithV2Storage(pipelineWithSize, storageWritingTo(writer)).create(path, withSize);
    verify(writer).setChunkSize(1024);

    writer = Mockito.mock(WriteChannel.class);
    gcsUtilWithV2Storage(pipelineWithSize, storageWritingTo(writer)).create(path, withoutSize);
    verify(writer).setChunkSize(2048);

    writer = Mockito.mock(WriteChannel.class);
    gcsUtilWithV2Storage(storageWritingTo(writer)).create(path, withoutSize);
    verify(writer).setChunkSize(GcsUtilV2.DEFAULT_UPLOAD_CHUNK_SIZE_BYTES);
  }

  /** Mirrors {@link GcsUtilV1Test#testGCSWriteMetricsIsSet}. */
  @Test
  public void testV2WriteMetricsIsSet() {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    when(storage.writer(any(BlobInfo.class), any()))
        .thenThrow(new StorageException(403, "Forbidden"));
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    GcsPath path = GcsPath.fromComponents("testbucket", "testobject");

    assertThrows(
        AccessDeniedException.class,
        () -> gcsUtil.create(path, CreateOptions.builder().setExpectFileToNotExist(true).build()));

    verifyMetricWasSet("my_project", "testbucket", "GcsInsert", "permission_denied", 1);
    verifyMetricWasSet("my_project", "testbucket", "GcsInsert", "ok", 0);
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testCreateBucketAccessErrors}, plus the other failures that
   * callers tell apart: an existing bucket, and anything else, which keeps its cause.
   */
  @Test
  public void testV2CreateBucketErrors() {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    StorageException serverError = new StorageException(503, "Service Unavailable");
    when(storage.create(any(BucketInfo.class), any()))
        .thenThrow(new StorageException(403, "Forbidden"))
        .thenThrow(new StorageException(409, "Conflict"))
        .thenThrow(serverError);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);
    com.google.api.services.storage.model.Bucket bucket =
        new com.google.api.services.storage.model.Bucket().setName("testbucket");

    assertThrows(AccessDeniedException.class, () -> gcsUtil.createBucket("my_project", bucket));
    assertThrows(
        FileAlreadyExistsException.class, () -> gcsUtil.createBucket("my_project", bucket));
    IOException thrown =
        assertThrows(IOException.class, () -> gcsUtil.createBucket("my_project", bucket));
    assertSame(serverError, thrown.getCause());
  }

  /** A batched request whose result is {@code value}, or {@code error} if one is given. */
  @SuppressWarnings("unchecked")
  private static <T> StorageBatchResult<T> batchResult(
      @Nullable T value, @Nullable StorageException error) {
    StorageBatchResult<T> result = Mockito.mock(StorageBatchResult.class);
    if (error != null) {
      when(result.get()).thenThrow(error);
    } else {
      when(result.get()).thenReturn(value);
    }
    return result;
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testGetObjects} and {@link
   * GcsUtilV1Test#testGetObjectsWithException}: the lookups are sent in one batch, and each path
   * gets its own result, so a missing or forbidden object doesn't fail the others.
   */
  @Test
  public void testV2GetObjectsWithMixedResults() throws IOException {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    StorageBatch batch = Mockito.mock(StorageBatch.class);
    when(storage.batch()).thenReturn(batch);
    StorageBatchResult<com.google.cloud.storage.Blob> found =
        batchResult(mockBlob("testbucket", "found", 10L), null);
    StorageBatchResult<com.google.cloud.storage.Blob> missing = batchResult(null, null);
    StorageBatchResult<com.google.cloud.storage.Blob> forbidden =
        batchResult(null, new StorageException(403, "Forbidden"));
    when(batch.get(Mockito.eq("testbucket"), Mockito.eq("found"), any())).thenReturn(found);
    when(batch.get(Mockito.eq("testbucket"), Mockito.eq("missing"), any())).thenReturn(missing);
    when(batch.get(Mockito.eq("testbucket"), Mockito.eq("forbidden"), any())).thenReturn(forbidden);
    GcsUtil gcsUtil = gcsUtilWithV2Storage(storage);

    List<StorageObjectOrIOException> results =
        gcsUtil.getObjects(
            ImmutableList.of(
                GcsPath.fromComponents("testbucket", "found"),
                GcsPath.fromComponents("testbucket", "missing"),
                GcsPath.fromComponents("testbucket", "forbidden")));

    assertEquals(3, results.size());
    assertEquals("found", results.get(0).storageObject().getName());
    assertEquals(BigInteger.valueOf(10), results.get(0).storageObject().getSize());
    assertTrue(results.get(1).ioException() instanceof FileNotFoundException);
    assertTrue(results.get(2).ioException() instanceof AccessDeniedException);
    verify(storage).batch();
    verify(batch).submit();
  }

  /** A java-storage client whose batched deletes in "testbucket" report {@code results}. */
  private static com.google.cloud.storage.Storage storageDeleting(
      Map<String, StorageBatchResult<Boolean>> results) {
    com.google.cloud.storage.Storage storage = Mockito.mock(com.google.cloud.storage.Storage.class);
    StorageBatch batch = Mockito.mock(StorageBatch.class);
    when(storage.batch()).thenReturn(batch);
    results.forEach(
        (object, result) ->
            when(batch.delete(Mockito.eq("testbucket"), Mockito.eq(object), any()))
                .thenReturn(result));
    return storage;
  }

  /**
   * Mirrors {@link GcsUtilV1Test#testRemoveWhenFileNotFound}: the legacy remove skips a missing
   * object, as V1 does, and it fails only when asked to. Any other failure is thrown rather than
   * being mistaken for a missing object.
   */
  @Test
  public void testV2RemoveWithMixedResults() throws IOException {
    GcsPath deleted = GcsPath.fromComponents("testbucket", "deleted");
    GcsPath missing = GcsPath.fromComponents("testbucket", "missing");
    GcsPath forbidden = GcsPath.fromComponents("testbucket", "forbidden");

    // java-storage reports a missing object as a delete that returned false.
    GcsUtil skippingMissing =
        gcsUtilWithV2Storage(
            storageDeleting(
                ImmutableMap.of(
                    "deleted", batchResult(true, null), "missing", batchResult(false, null))));
    skippingMissing.remove(ImmutableList.of(deleted.toString(), missing.toString()));

    GcsUtil failingOnMissing =
        gcsUtilWithV2Storage(
            storageDeleting(
                ImmutableMap.of(
                    "deleted", batchResult(true, null), "missing", batchResult(false, null))));
    assertThrows(
        FileNotFoundException.class,
        () ->
            failingOnMissing.remove(
                ImmutableList.of(deleted, missing), GcsUtilV2.MissingStrategy.FAIL_IF_MISSING));

    GcsUtil withForbidden =
        gcsUtilWithV2Storage(
            storageDeleting(
                ImmutableMap.of(
                    "deleted", batchResult(true, null),
                    "missing", batchResult(false, null),
                    "forbidden", batchResult(null, new StorageException(403, "Forbidden")))));
    assertThrows(
        AccessDeniedException.class,
        () ->
            withForbidden.remove(
                ImmutableList.of(deleted.toString(), missing.toString(), forbidden.toString())));
  }
}

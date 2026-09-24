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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.auth.Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.repackaged.core.org.apache.commons.compress.utils.SeekableInMemoryByteChannel;
import org.apache.beam.runners.core.metrics.CounterCell;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.extensions.gcp.auth.NoopCredentialFactory;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link GcsUtilV2}.
 *
 * <p>Covers two things without making any request to GCS:
 *
 * <ul>
 *   <li>The storage client is configured from the pipeline options (project, credentials,
 *       endpoint), matching what {@link GcsUtilV1} honors.
 *   <li>HTTP metrics and byte counters are installed only when their flags ask for them, and only
 *       when there is a container to report into. The counting logic itself is covered by {@link
 *       TransportTest}.
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

  /** Mirrors {@code GcsUtilTest#testGcsEndpoint}: only the root of the endpoint applies to V2. */
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
  // Byte counters (mirrors GcsUtilTest#testReadMetrics / #testWriteMetrics for V1)
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
}

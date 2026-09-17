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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import java.io.IOException;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests that {@link GcsUtilV2} binds HTTP metrics to its storage client correctly.
 *
 * <p>The counting logic itself is covered by {@link TransportTest}; what matters here is that V2
 * installs it, and only does so when the counters are actually being collected.
 */
@RunWith(JUnit4.class)
public class GcsUtilV2Test {

  private static GcsUtilV2 gcsUtilV2(boolean performanceMetrics) {
    GcsOptions options = PipelineOptionsFactory.as(GcsOptions.class);
    // Avoid resolving application default credentials; no request leaves the process.
    options.setGcpCredential(new TestCredential());
    options.setProject("test-project");
    options.setGcsPerformanceMetrics(performanceMetrics);
    return new GcsUtilV2(options);
  }

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
}

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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions.GcsCustomAuditEntries;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransportTest {

  @Test
  public void testUserAgentAndCustomAuditInGcsRequestHeaders() throws IOException {
    GcsOptions gcsOptions = PipelineOptionsFactory.as(GcsOptions.class);
    gcsOptions.setGcpCredential(new TestCredential());
    gcsOptions.setJobName("test-job");
    gcsOptions.setAppName("test-app");
    GcsCustomAuditEntries entries = new GcsCustomAuditEntries();
    entries.put("job", "test-job-override");
    entries.put("user", "test-user");
    entries.put("id", "1234");
    entries.put("status", "ok");
    gcsOptions.setGcsCustomAuditEntries(entries);

    Storage storageClient = Transport.newStorageClient(gcsOptions).build();
    Storage.Objects.Get getObject = storageClient.objects().get("test-bucket", "test-object");
    HttpRequest request = getObject.buildHttpRequest();

    // An example of user agent string will be like
    // "test-app apache-beam/2.57.0.dev (GPN:Beam) Google-API-Java-Client/2.0.0"
    // For a valid user-agent string, a comment like "(GPN:Beam)" cannot be the first token.
    // https://www.rfc-editor.org/rfc/rfc7231#section-5.5.3
    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" ")).indexOf("test-app"),
        greaterThanOrEqualTo(0));

    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" "))
            .indexOf(String.format("apache-beam/%s", ReleaseInfo.getReleaseInfo().getSdkVersion())),
        greaterThan(0));

    assertThat(
        Arrays.asList(request.getHeaders().getUserAgent().split(" ")).indexOf("(GPN:Beam)"),
        greaterThan(0));

    // there should be one and only one custom audit entry for job name
    assertEquals(
        request.getHeaders().getHeaderStringValues("x-goog-custom-audit-job"),
        Collections.singletonList("test-job-override"));

    assertEquals(
        request.getHeaders().getHeaderStringValues("x-goog-custom-audit-user"),
        Collections.singletonList("test-user"));

    assertEquals(
        request.getHeaders().getHeaderStringValues("x-goog-custom-audit-id"),
        Collections.singletonList("1234"));

    assertEquals(
        request.getHeaders().getHeaderStringValues("x-goog-custom-audit-status"),
        Collections.singletonList("ok"));
  }

  private static final String READ_PREFIX = "gcs_http_read_";
  private static final String WRITE_PREFIX = "gcs_http_write_";

  private static long counter(MetricsContainerImpl container, String name) {
    return container.getCounter(MetricName.named(GcsUtil.METRIC_NAMESPACE, name)).getCumulative();
  }

  /**
   * Executes one request through a metrics-wrapped initializer against a mock transport, and
   * returns the container the counters were recorded against. An empty {@code range} sends no Range
   * header.
   */
  private static MetricsContainerImpl executeRequest(
      boolean isWrite, String method, int statusCode, String range) throws IOException {
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MockHttpTransport transport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(new MockLowLevelHttpResponse().setStatusCode(statusCode))
            .build();
    HttpRequest request =
        transport
            .createRequestFactory(Transport.withMetricsContainer(req -> {}, container, isWrite))
            .buildRequest(method, new GenericUrl("https://storage.googleapis.com/test"), null);
    if (!range.isEmpty()) {
      request.getHeaders().setRange(range);
    }
    // Observe the status that was actually returned, rather than throwing or following it.
    request.setThrowExceptionOnExecuteError(false);
    request.setFollowRedirects(false);
    request.execute();
    return container;
  }

  @Test
  public void testReadMetricsCountUnboundedGets() throws IOException {
    MetricsContainerImpl container = executeRequest(false, "GET", 200, "");

    assertEquals(1, counter(container, READ_PREFIX + "request_count"));
    assertEquals(1, counter(container, READ_PREFIX + "request_count_unbounded"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_ranged"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_other"));
    assertEquals(1, counter(container, READ_PREFIX + "status_2xx"));
    assertEquals(0, counter(container, READ_PREFIX + "request_no_response"));
  }

  @Test
  public void testReadMetricsCountRangedGets() throws IOException {
    MetricsContainerImpl container = executeRequest(false, "GET", 206, "bytes=0-9");

    assertEquals(1, counter(container, READ_PREFIX + "request_count"));
    assertEquals(1, counter(container, READ_PREFIX + "request_count_ranged"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_unbounded"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_other"));
    // 206 Partial Content is still a success.
    assertEquals(1, counter(container, READ_PREFIX + "status_2xx"));
  }

  @Test
  public void testReadMetricsClassifyNonGetsAsOther() throws IOException {
    MetricsContainerImpl container = executeRequest(false, "POST", 200, "");

    assertEquals(1, counter(container, READ_PREFIX + "request_count"));
    assertEquals(1, counter(container, READ_PREFIX + "request_count_other"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_ranged"));
    assertEquals(0, counter(container, READ_PREFIX + "request_count_unbounded"));
  }

  @Test
  public void testWriteMetricsAreNotClassifiedByRequestShape() throws IOException {
    MetricsContainerImpl container = executeRequest(true, "POST", 200, "");

    assertEquals(1, counter(container, WRITE_PREFIX + "request_count"));
    assertEquals(1, counter(container, WRITE_PREFIX + "status_2xx"));
    // Writes are POSTs and PUTs by construction, so the shape counters are never allocated.
    assertEquals(0, counter(container, WRITE_PREFIX + "request_count_ranged"));
    assertEquals(0, counter(container, WRITE_PREFIX + "request_count_unbounded"));
    assertEquals(0, counter(container, WRITE_PREFIX + "request_count_other"));
  }

  @Test
  public void testResponsesAreCountedByStatusClass() throws IOException {
    // A resumable upload answers 308 to every chunk but the last, so 3xx is not an error.
    assertEquals(1, counter(executeRequest(true, "PUT", 308, ""), WRITE_PREFIX + "status_3xx"));
    assertEquals(1, counter(executeRequest(false, "GET", 404, ""), READ_PREFIX + "status_4xx"));
    assertEquals(1, counter(executeRequest(false, "GET", 503, ""), READ_PREFIX + "status_5xx"));

    // Each of those is still exactly one request, and none of them lands in 2xx.
    MetricsContainerImpl notFound = executeRequest(false, "GET", 404, "");
    assertEquals(1, counter(notFound, READ_PREFIX + "request_count"));
    assertEquals(0, counter(notFound, READ_PREFIX + "status_2xx"));
  }

  @Test
  public void testInitializerIsUnchangedWithoutAContainer() {
    HttpRequestInitializer base = request -> {};
    assertSame(base, Transport.withMetricsContainer(base, null, false));
    assertSame(base, Transport.withMetricsContainer(base, null, true));
  }
}

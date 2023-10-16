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
package org.apache.beam.sdk.io.splunk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockserver.client.MockServerClient;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link SplunkEventWriter} class. */
public class SplunkEventWriterTest {

  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.HEC_URL_PATH;
  private static final long MAX_SOCKET_TIMEOUT_MILLIS = 180000;

  @BeforeClass
  public static void setup() {
    ConfigurationProperties.maxSocketTimeout(MAX_SOCKET_TIMEOUT_MILLIS);
  }

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Splunk HEC server.
  @Rule public MockServerRule mockServerRule = new MockServerRule(this);

  private MockServerClient mockServerClient;

  @Test
  public void testMissingURLProtocol() {
    assertFalse(SplunkEventWriter.isValidUrlFormat("test-url"));
  }

  @Test
  public void testInvalidURL() {
    assertFalse(SplunkEventWriter.isValidUrlFormat("http://1.2.3"));
  }

  @Test
  public void testValidURL() {
    assertTrue(SplunkEventWriter.isValidUrlFormat("http://test-url"));
  }

  @Test
  public void eventWriterMissingURL() {

    Exception thrown =
        assertThrows(NullPointerException.class, () -> SplunkEventWriter.newBuilder().build());

    assertTrue(thrown.getMessage().contains("url needs to be provided"));
  }

  @Test
  public void eventWriterMissingURLProtocol() {
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SplunkEventWriter.newBuilder().withUrl("test-url").build());

    assertTrue(thrown.getMessage().contains(SplunkEventWriter.INVALID_URL_FORMAT_MESSAGE));
  }

  /** Test building {@link SplunkEventWriter} with an invalid URL. */
  @Test
  public void eventWriterInvalidURL() {
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SplunkEventWriter.newBuilder().withUrl("http://1.2.3").build());

    assertTrue(thrown.getMessage().contains(SplunkEventWriter.INVALID_URL_FORMAT_MESSAGE));
  }

  /**
   * Test building {@link SplunkEventWriter} with the 'services/collector/event' path appended to
   * the URL.
   */
  @Test
  public void eventWriterFullEndpoint() {
    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SplunkEventWriter.newBuilder()
                    .withUrl("http://test-url:8088/services/collector/event")
                    .build());

    assertTrue(thrown.getMessage().contains(SplunkEventWriter.INVALID_URL_FORMAT_MESSAGE));
  }

  @Test
  public void eventWriterMissingToken() {

    Exception thrown =
        assertThrows(
            NullPointerException.class,
            () -> SplunkEventWriter.newBuilder().withUrl("http://test-url").build());

    assertTrue(thrown.getMessage().contains("token needs to be provided"));
  }

  @Test
  public void eventWriterDefaultBatchCountAndValidation() {

    SplunkEventWriter writer =
        SplunkEventWriter.newBuilder().withUrl("http://test-url").withToken("test-token").build();

    assertNull(writer.inputBatchCount());
    assertNull(writer.disableCertificateValidation());
  }

  @Test
  public void eventWriterCustomBatchCountAndValidation() {

    Integer batchCount = 30;
    Boolean certificateValidation = false;
    SplunkEventWriter writer =
        SplunkEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withToken("test-token")
            .withInputBatchCount(StaticValueProvider.of(batchCount))
            .withDisableCertificateValidation(StaticValueProvider.of(certificateValidation))
            .build();

    assertEquals(batchCount, writer.inputBatchCount().get());
    assertEquals(certificateValidation, writer.disableCertificateValidation().get());
  }

  @Test
  @Category(NeedsRunner.class)
  public void successfulSplunkWriteSingleBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SplunkEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SplunkEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .create()),
            KV.of(
                123,
                SplunkEvent.newBuilder()
                    .withEvent("test-event-2")
                    .withHost("test-host-2")
                    .withIndex("test-index-2")
                    .withSource("test-source-2")
                    .withSourceType("test-source-type-2")
                    .withTime(12345L)
                    .create()));

    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents))
            .apply(
                "SplunkEventWriter",
                ParDo.of(
                    SplunkEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(1)) // Test one request per SplunkEvent
                        .withToken("test-token")
                        .build()));

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly the expected number of POST requests.
    mockServerClient.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testEvents.size()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void successfulSplunkWriteMultiBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SplunkEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SplunkEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .create()),
            KV.of(
                123,
                SplunkEvent.newBuilder()
                    .withEvent("test-event-2")
                    .withHost("test-host-2")
                    .withIndex("test-index-2")
                    .withSource("test-source-2")
                    .withSourceType("test-source-type-2")
                    .withTime(12345L)
                    .create()));

    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents))
            .apply(
                "SplunkEventWriter",
                ParDo.of(
                    SplunkEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withToken("test-token")
                        .build()));

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  @Test
  @Category(NeedsRunner.class)
  public void failedSplunkWriteSingleBatchTest() {

    // Create server expectation for FAILURE.
    mockServerListening(404);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SplunkEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SplunkEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .create()));

    PCollection<SplunkWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(testEvents))
            .apply(
                "SplunkEventWriter",
                ParDo.of(
                    SplunkEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withToken("test-token")
                        .build()));

    // Expect a single 404 Not found SplunkWriteError
    PAssert.that(actual)
        .containsInAnyOrder(
            SplunkWriteError.newBuilder()
                .withStatusCode(404)
                .withStatusMessage("Not Found")
                .withPayload(
                    "{\"time\":12345,\"host\":\"test-host-1\","
                        + "\"source\":\"test-source-1\",\"sourcetype\":\"test-source-type-1\","
                        + "\"index\":\"test-index-1\",\"event\":\"test-event-1\"}")
                .create());

    pipeline.run();

    // Server received exactly one POST request.
    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  private void mockServerListening(int statusCode) {

    mockServerClient
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}

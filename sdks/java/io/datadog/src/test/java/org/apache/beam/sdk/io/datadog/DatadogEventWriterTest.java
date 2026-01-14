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
package org.apache.beam.sdk.io.datadog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.datadog.DatadogEventWriter} class. */
public class DatadogEventWriterTest {

  private static final String EXPECTED_PATH = "/" + DatadogEventPublisher.DD_URL_PATH;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Datadog API server.
  private ClientAndServer mockServer;

  @Before
  public void setup() {
    ConfigurationProperties.disableSystemOut(true);
    mockServer = startClientAndServer();
  }

  @After
  public void tearDown() {
    if (mockServer != null) {
      mockServer.stop();
    }
  }

  /** Test building {@link DatadogEventWriter} with missing URL. */
  @Test
  public void eventWriterMissingURL() {

    Exception thrown =
        assertThrows(NullPointerException.class, () -> DatadogEventWriter.newBuilder().build());

    assertThat(thrown).hasMessageThat().contains("url needs to be provided");
  }

  /** Test building {@link DatadogEventWriter} with missing URL protocol. */
  @Test
  public void eventWriterMissingURLProtocol() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("test-url").build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link DatadogEventWriter} with an invalid URL. */
  @Test
  public void eventWriterInvalidURL() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("http://1.2.3").build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link DatadogEventWriter} with the 'api/v2/logs' path appended to the URL. */
  @Test
  public void eventWriterFullEndpoint() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatadogEventWriter.newBuilder()
                    .withUrl("http://test-url:8088/api/v2/logs")
                    .build());

    assertThat(thrown).hasMessageThat().contains(DatadogEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link DatadogEventWriter} with missing token. */
  @Test
  public void eventWriterMissingToken() {

    Exception thrown =
        assertThrows(
            NullPointerException.class,
            () -> DatadogEventWriter.newBuilder().withUrl("http://test-url").build());

    assertThat(thrown).hasMessageThat().contains("apiKey needs to be provided");
  }

  /** Test building {@link DatadogEventWriter} with default batch count. */
  @Test
  public void eventWriterDefaultBatchCount() {

    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withApiKey("test-api-key")
            .build();

    assertThat(writer.inputBatchCount()).isNull();
  }

  /**
   * Test building {@link DatadogEventWriter} with a batchCount less than the configured minimum.
   */
  @Test
  public void eventWriterBatchCountTooSmall() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatadogEventWriter.newBuilder(7)
                    .withUrl("http://test-url")
                    .withApiKey("test-api-key")
                    .withInputBatchCount(6)
                    .build());

    assertThat(thrown)
        .hasMessageThat()
        .contains("inputBatchCount must be greater than or equal to 7");
  }

  /** Test building {@link DatadogEventWriter} with a batchCount greater than 1000. */
  @Test
  public void eventWriterBatchCountTooBig() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DatadogEventWriter.newBuilder()
                    .withUrl("http://test-url")
                    .withApiKey("test-api-key")
                    .withInputBatchCount(1001)
                    .build());

    assertThat(thrown)
        .hasMessageThat()
        .contains("inputBatchCount must be less than or equal to 1000");
  }

  /** Test building {@link DatadogEventWriter} with custom batchCount . */
  @Test
  public void eventWriterCustomBatchCountAndValidation() {

    Integer batchCount = 30;
    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withApiKey("test-api-key")
            .withInputBatchCount(batchCount)
            .build();

    assertThat(writer.inputBatchCount()).isEqualTo(batchCount);
  }

  /** Test building {@link DatadogEventWriter} with default maxBufferSize . */
  @Test
  public void eventWriterDefaultMaxBufferSize() {

    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withApiKey("test-api-key")
            .build();

    assertThat(writer.maxBufferSize()).isNull();
  }

  /** Test building {@link DatadogEventWriter} with custom maxBufferSize . */
  @Test
  public void eventWriterCustomMaxBufferSizeAndValidation() {

    Long maxBufferSize = 1_427_841L;
    DatadogEventWriter writer =
        DatadogEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withMaxBufferSize(maxBufferSize)
            .withApiKey("test-api-key")
            .build();

    assertThat(writer.maxBufferSize()).isEqualTo(maxBufferSize);
  }

  /** Test successful POST request for single batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogWriteSingleBatchTest() {

    // Create server expectation for success.
    addRequestExpectation(202);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()),
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-2")
                    .withTags("test-tags-2")
                    .withHostname("test-hostname-2")
                    .withService("test-service-2")
                    .withMessage("test-message-2")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder(1)
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(1) // Test one request per DatadogEvent
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly the expected number of POST requests.
    mockServer.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testEvents.size()));
  }

  /** Test successful POST request for multi batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogWriteMultiBatchTest() {

    // Create server expectation for success.
    addRequestExpectation(202);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()),
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-2")
                    .withTags("test-tags-2")
                    .withHostname("test-hostname-2")
                    .withService("test-service-2")
                    .withMessage("test-message-2")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder(1)
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(testEvents.size()) // all requests in a single batch.
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test successful POST requests for batch exceeding max buffer size. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogWriteExceedingMaxBufferSize() {

    // Create server expectation for success.
    addRequestExpectation(202);

    int testPort = mockServer.getPort();

    String payloadFormat = "{\"message\":\"%s\"}";
    long jsonSize = DatadogEventSerializer.getPayloadSize(String.format(payloadFormat, ""));

    long maxBufferSize = 100;
    long msgSize = 50;

    char[] bunchOfAs = new char[(int) (msgSize - jsonSize)];
    Arrays.fill(bunchOfAs, 'a');

    List<KV<Integer, DatadogEvent>> testEvents = new ArrayList<>();
    for (int i = 1; i <= 3; i++) {
      testEvents.add(
          KV.of(123, DatadogEvent.newBuilder().withMessage(new String(bunchOfAs)).build()));
    }

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder(1)
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(testEvents.size())
                        .withMaxBufferSize(maxBufferSize)
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly two POST requests:
    // 1st batch of size=2 due to next msg exceeding max buffer size
    // 2nd batch of size=1 due to timer
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(2));
  }

  /** Test failed POST request. */
  @Test
  @Category(NeedsRunner.class)
  public void failedDatadogWriteSingleBatchTest() {

    // Create server expectation for FAILURE.
    addRequestExpectation(404);

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder(1)
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(testEvents.size()) // all requests in a single batch.
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // Expect a single 404 Not found DatadogWriteError
    PAssert.that(actual)
        .containsInAnyOrder(
            DatadogWriteError.newBuilder()
                .withStatusCode(404)
                .withStatusMessage("Not Found")
                .withPayload(
                    "{\"ddsource\":\"test-source-1\","
                        + "\"ddtags\":\"test-tags-1\",\"hostname\":\"test-hostname-1\","
                        + "\"service\":\"test-service-1\",\"message\":\"test-message-1\"}")
                .build());

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test failed due to single event exceeding max buffer size. */
  @Test
  @Category(NeedsRunner.class)
  public void failedDatadogEventTooBig() {

    // Create server expectation for FAILURE.
    addRequestExpectation(404);

    int testPort = mockServer.getPort();

    String payloadFormat = "{\"message\":\"%s\"}";

    long maxBufferSize = 100;
    char[] bunchOfAs =
        new char
            [(int)
                (maxBufferSize
                    + 1L
                    - DatadogEventSerializer.getPayloadSize(String.format(payloadFormat, "")))];
    Arrays.fill(bunchOfAs, 'a');
    String messageTooBig = new String(bunchOfAs);

    String expectedPayload = String.format(payloadFormat, messageTooBig);
    long expectedPayloadSize = DatadogEventSerializer.getPayloadSize(expectedPayload);
    assertThat(maxBufferSize + 1L).isEqualTo(expectedPayloadSize);

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(KV.of(123, DatadogEvent.newBuilder().withMessage(messageTooBig).build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withMaxBufferSize(maxBufferSize)
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    // Expect a single DatadogWriteError due to exceeding max buffer size
    PAssert.that(actual)
        .containsInAnyOrder(DatadogWriteError.newBuilder().withPayload(expectedPayload).build());

    pipeline.run();

    // Server did not receive any requests.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(0));
  }

  /** Test retryable POST request. */
  @Test
  @Category(NeedsRunner.class)
  public void retryableDatadogWriteSingleBatchTest() {

    // Create server expectations for 3 retryable failures, 1 success.
    addRequestExpectation(408, Times.once());
    addRequestExpectation(429, Times.once());
    addRequestExpectation(502, Times.once());
    addRequestExpectation(202, Times.once());

    int testPort = mockServer.getPort();

    List<KV<Integer, DatadogEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                DatadogEvent.newBuilder()
                    .withSource("test-source-1")
                    .withTags("test-tags-1")
                    .withHostname("test-hostname-1")
                    .withService("test-service-1")
                    .withMessage("test-message-1")
                    .build()));

    PCollection<DatadogWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), DatadogEventCoder.of())))
            .apply(
                "DatadogEventWriter",
                ParDo.of(
                    DatadogEventWriter.newBuilder(1)
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(testEvents.size()) // all requests in a single batch.
                        .withApiKey("test-api-key")
                        .build()))
            .setCoder(DatadogWriteErrorCoder.of());

    PAssert.that(actual).empty();

    // All successful responses, eventually.
    pipeline.run();

    // Server received exactly 4 POST requests (3 retryable failures, 1 success).
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(4));
  }

  private void addRequestExpectation(int statusCode) {
    addRequestExpectation(statusCode, Times.unlimited());
  }

  private void addRequestExpectation(int statusCode, Times times) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH), times)
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}

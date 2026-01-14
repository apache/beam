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

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.datadog.DatadogIO} class. */
public class DatadogIOTest {

  private static final DatadogEvent DATADOG_TEST_EVENT_1 =
      DatadogEvent.newBuilder()
          .withSource("test-source-1")
          .withTags("test-tags-1")
          .withHostname("test-hostname-1")
          .withService("test-service-1")
          .withMessage("test-message-1")
          .build();

  private static final DatadogEvent DATADOG_TEST_EVENT_2 =
      DatadogEvent.newBuilder()
          .withSource("test-source-2")
          .withTags("test-tags-2")
          .withHostname("test-hostname-2")
          .withService("test-service-2")
          .withMessage("test-message-2")
          .build();

  private static final List<DatadogEvent> DATADOG_EVENTS =
      ImmutableList.of(DATADOG_TEST_EVENT_1, DATADOG_TEST_EVENT_2);

  private static final String EXPECTED_PATH = "/" + DatadogEventPublisher.DD_URL_PATH;
  private static final int TEST_PARALLELISM = 2;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a mock server to simulate an actual Datadog API server.
  private ClientAndServer mockServer;

  @Before
  public void setup() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    mockServer = startClientAndServer();
  }

  /** Test successful multi-event POST request for DatadogIO without parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOMultiBatchNoParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder(1)
                    .withParallelism(1)
                    .withBatchCount(DATADOG_EVENTS.size())
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test successful multi-event POST request for DatadogIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOMultiBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder(1)
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(DATADOG_EVENTS.size())
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request per parallelism
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.atLeast(1));
  }

  /** Test successful multi-event POST request for DatadogIO with parallelism. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulDatadogIOSingleBatchParallelismTest() {

    // Create server expectation for success.
    mockServerListening(200);
    PCollection<DatadogWriteError> actual =
        pipeline
            .apply("Create Input data", Create.of(DATADOG_EVENTS).withCoder(DatadogEventCoder.of()))
            .apply(
                "DatadogIO",
                DatadogIO.writeBuilder(1)
                    .withParallelism(TEST_PARALLELISM)
                    .withBatchCount(1)
                    .withApiKey("test-api-key")
                    .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
                    .build())
            .setCoder(DatadogWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly 1 post request per DatadogEvent
    mockServer.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(DATADOG_EVENTS.size()));
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}

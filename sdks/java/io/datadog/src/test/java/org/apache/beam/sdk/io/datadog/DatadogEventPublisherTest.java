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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.util.ExponentialBackOff;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link DatadogEventPublisher} class. */
public class DatadogEventPublisherTest {

  private static final String EXPECTED_PATH = "/" + DatadogEventPublisher.DD_URL_PATH;

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

  /** Test whether {@link HttpContent} is created from the list of {@link DatadogEvent}s. */
  @Test
  public void contentTest() throws NoSuchAlgorithmException, KeyManagementException, IOException {

    DatadogEventPublisher publisher =
        DatadogEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withApiKey("test-api-key")
            .build();

    String expectedString =
        "["
            + "{\"ddsource\":\"test-source-1\",\"ddtags\":\"test-tags-1\","
            + "\"hostname\":\"test-hostname-1\",\"service\":\"test-service-1\","
            + "\"message\":\"test-message-1\"},"
            + "{\"ddsource\":\"test-source-2\",\"ddtags\":\"test-tags-2\","
            + "\"hostname\":\"test-hostname-2\",\"service\":\"test-service-2\","
            + "\"message\":\"test-message-2\"}"
            + "]";

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      HttpContent actualContent = publisher.getContent(DATADOG_EVENTS);
      actualContent.writeTo(bos);
      String actualString = new String(bos.toByteArray(), StandardCharsets.UTF_8);
      assertThat(actualString, is(equalTo(expectedString)));
    }
  }

  @Test
  public void genericURLTest() throws IOException {

    String baseURL = "http://example.com";
    DatadogEventPublisher.Builder builder =
        DatadogEventPublisher.newBuilder().withUrl(baseURL).withApiKey("test-api-key");

    assertThat(
        builder.genericUrl(),
        is(equalTo(new GenericUrl(Joiner.on('/').join(baseURL, "api/v2/logs")))));
  }

  @Test
  public void configureBackOffDefaultTest()
      throws NoSuchAlgorithmException, KeyManagementException, IOException {

    DatadogEventPublisher publisherDefaultBackOff =
        DatadogEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withApiKey("test-api-key")
            .build();

    assertThat(
        publisherDefaultBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS)));
  }

  @Test
  public void configureBackOffCustomTest()
      throws NoSuchAlgorithmException, KeyManagementException, IOException {

    int timeoutInMillis = 600000; // 10 minutes
    DatadogEventPublisher publisherWithBackOff =
        DatadogEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withApiKey("test-api-key")
            .withMaxElapsedMillis(timeoutInMillis)
            .build();

    assertThat(
        publisherWithBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis(),
        is(equalTo(timeoutInMillis)));
  }

  @Test
  public void requestHeadersTest() throws Exception {
    ConfigurationProperties.disableSystemOut(true);
    try (ClientAndServer mockServer = startClientAndServer()) {
      mockServer
          .when(org.mockserver.model.HttpRequest.request(EXPECTED_PATH))
          .respond(org.mockserver.model.HttpResponse.response().withStatusCode(202));

      DatadogEventPublisher publisher =
          DatadogEventPublisher.newBuilder()
              .withUrl(Joiner.on(':').join("http://localhost", mockServer.getPort()))
              .withApiKey("test-api-key")
              .build();

      DatadogEvent event =
          DatadogEvent.newBuilder()
              .withSource("test-source-1")
              .withTags("test-tags-1")
              .withHostname("test-hostname-1")
              .withService("test-service-1")
              .withMessage("test-message-1")
              .build();

      HttpResponse response = publisher.execute(ImmutableList.of(event));
      assertThat(response.getStatusCode(), is(equalTo(202)));

      mockServer.verify(
          org.mockserver.model.HttpRequest.request(EXPECTED_PATH)
              .withContentType(MediaType.APPLICATION_JSON)
              .withHeader("dd-api-key", "test-api-key")
              .withHeader("dd-evp-origin", "dataflow")
              .withHeader("Accept-Encoding", "gzip"),
          VerificationTimes.once());
    }
  }
}

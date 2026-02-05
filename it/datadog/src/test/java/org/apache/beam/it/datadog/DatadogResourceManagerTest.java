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
package org.apache.beam.it.datadog;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockserver.client.ForwardChainExpectation;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.HttpRequest;
import org.testcontainers.containers.MockServerContainer;

/** Unit tests for {@link com.google.cloud.teleport.it.datadog.DatadogResourceManager}. */
@RunWith(JUnit4.class)
public class DatadogResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private DatadogClientFactory clientFactory;
  @Mock private CloseableHttpClient httpClient;

  @Mock private MockServerClient serviceClient;

  @Mock private MockServerContainer container;

  private static final String TEST_ID = "test-id";
  private static final String HOSTNAME = "localhost";
  private static final String API_KEY = "token";
  private static final String MESSAGE = "myEvent";

  private static final int DEFAULT_DATADOG_HEC_INTERNAL_PORT = MockServerContainer.PORT;
  private static final int MAPPED_DATADOG_HEC_INTERNAL_PORT = 50000;

  private DatadogResourceManager testManager;

  @Before
  public void setUp() {
    when(container.getMappedPort(DEFAULT_DATADOG_HEC_INTERNAL_PORT))
        .thenReturn(MAPPED_DATADOG_HEC_INTERNAL_PORT);

    doReturn(container).when(container).withLogConsumer(any());

    when(serviceClient.when(any(HttpRequest.class)))
        .thenReturn(mock(ForwardChainExpectation.class));

    when(clientFactory.getServiceClient(HOSTNAME, MAPPED_DATADOG_HEC_INTERNAL_PORT))
        .thenReturn(serviceClient);

    testManager =
        new DatadogResourceManager(
            clientFactory, container, DatadogResourceManager.builder(TEST_ID));
  }

  @Test
  public void testGetHttpEndpointReturnsCorrectValue() {
    assertThat(testManager.getHttpEndpoint())
        .isEqualTo(String.format("http://%s:%d", HOSTNAME, MAPPED_DATADOG_HEC_INTERNAL_PORT));
  }

  @Test
  public void testGetHecEndpointReturnsCorrectValue() {
    assertThat(testManager.getApiEndpoint())
        .isEqualTo(
            String.format("http://%s:%d/api/v2/logs", HOSTNAME, MAPPED_DATADOG_HEC_INTERNAL_PORT));
  }

  @Test
  public void testGetHecTokenReturnsCorrectValueWhenSet() {
    assertThat(
            new DatadogResourceManager(
                    clientFactory,
                    container,
                    DatadogResourceManager.builder(TEST_ID).setApiKey(API_KEY))
                .getApiKey())
        .isEqualTo(API_KEY);
  }

  @Test
  public void testSendHttpEventsShouldThrowErrorWhenHttpClientFailsToExecuteRequest()
      throws IOException {
    DatadogLogEntry event = DatadogLogEntry.newBuilder().withMessage(MESSAGE).build();

    when(clientFactory.getHttpClient()).thenReturn(httpClient);
    doThrow(IOException.class).when(httpClient).execute(any(HttpPost.class));

    assertThrows(DatadogResourceManagerException.class, () -> testManager.sendHttpEvent(event));
  }

  @Test
  public void testSendHttpEventsShouldThrowErrorWhenHttpClientReturnsErrorCode()
      throws IOException {
    DatadogLogEntry event = DatadogLogEntry.newBuilder().withMessage(MESSAGE).build();

    try (CloseableHttpResponse mockResponse =
        mock(CloseableHttpResponse.class, Answers.RETURNS_DEEP_STUBS)) {
      when(clientFactory.getHttpClient()).thenReturn(httpClient);
      when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine().getStatusCode()).thenReturn(404);
    }

    assertThrows(DatadogResourceManagerException.class, () -> testManager.sendHttpEvent(event));
  }

  @Test
  public void testSendHttpEventsShouldReturnTrueIfDatadogDoesNotThrowAnyError() throws IOException {
    DatadogLogEntry event = DatadogLogEntry.newBuilder().withMessage(MESSAGE).build();

    try (CloseableHttpResponse mockResponse =
        mock(CloseableHttpResponse.class, Answers.RETURNS_DEEP_STUBS)) {
      when(clientFactory.getHttpClient()).thenReturn(httpClient);
      when(httpClient.execute(any(HttpPost.class))).thenReturn(mockResponse);
      when(mockResponse.getStatusLine().getStatusCode()).thenReturn(202);
    }

    assertThat(testManager.sendHttpEvents(java.util.Arrays.asList(event, event))).isTrue();
    verify(httpClient).execute(any(HttpPost.class));
  }

  @Test
  public void testGetEventsShouldThrowErrorWhenServiceClientFailsToExecuteRequest() {
    doThrow(RuntimeException.class)
        .when(serviceClient)
        .retrieveRecordedRequests(any(HttpRequest.class));

    assertThrows(RuntimeException.class, () -> testManager.getEntries());
  }

  @Test
  public void testGetEventsShouldReturnTrueIfDatadogDoesNotThrowAnyError() {
    String httpRequestBody =
        "[{\n"
            + "\"message\": \"message\",\n"
            + "\"hostname\": \"hostname\",\n"
            + "\"ddsource\": \"ddsource\"\n"
            + "}]";

    HttpRequest mockHttpRequest = mock(HttpRequest.class);
    when(mockHttpRequest.getBodyAsString()).thenReturn(httpRequestBody);

    when(serviceClient.retrieveRecordedRequests(any(HttpRequest.class)))
        .thenReturn(new HttpRequest[] {mockHttpRequest});

    DatadogLogEntry datadogEvent =
        DatadogLogEntry.newBuilder()
            .withMessage("message")
            .withHostname("hostname")
            .withSource("ddsource")
            .build();

    assertThat(testManager.getEntries())
        .containsExactlyElementsIn(Collections.singletonList(datadogEvent));
  }
}

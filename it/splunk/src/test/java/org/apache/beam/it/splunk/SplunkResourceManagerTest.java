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
package org.apache.beam.it.splunk;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.splunk.Job;
import com.splunk.ServiceArgs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.images.builder.Transferable;

/** Unit tests for {@link SplunkResourceManager}. */
@RunWith(JUnit4.class)
public class SplunkResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private SplunkClientFactory clientFactory;

  @Mock private SplunkContainer container;

  private static final String TEST_ID = "test-id";
  private static final String HOST = "localhost";
  private static final String HEC_SCHEMA = "http";
  private static final String HEC_TOKEN = "token";
  private static final String QUERY = "query";
  private static final String EVENT = "myEvent";
  private static final int DEFAULT_SPLUNK_HEC_INTERNAL_PORT = 8088;
  private static final int MAPPED_SPLUNK_HEC_INTERNAL_PORT = 50000;
  private static final int DEFAULT_SPLUNKD_INTERNAL_PORT = 8089;
  private static final int MAPPED_SPLUNKD_INTERNAL_PORT = 50001;

  private SplunkResourceManager testManager;

  @Before
  public void setUp() {
    when(container.withDefaultsFile(any(Transferable.class))).thenReturn(container);
    when(container.withPassword(anyString())).thenReturn(container);
    when(container.getMappedPort(DEFAULT_SPLUNKD_INTERNAL_PORT))
        .thenReturn(MAPPED_SPLUNKD_INTERNAL_PORT);

    testManager =
        new SplunkResourceManager(clientFactory, container, SplunkResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsSplunkResourceManager() {
    assertThat(
            SplunkResourceManager.builder(TEST_ID)
                .setHecPort(DEFAULT_SPLUNK_HEC_INTERNAL_PORT)
                .setSplunkdPort(DEFAULT_SPLUNKD_INTERNAL_PORT)
                .setHost(HOST)
                .useStaticContainer()
                .build())
        .isInstanceOf(SplunkResourceManager.class);
  }

  @Test
  public void testCreateResourceManagerThrowsCustomPortErrorWhenUsingStaticContainer() {
    assertThat(
            assertThrows(
                    SplunkResourceManagerException.class,
                    () ->
                        SplunkResourceManager.builder(TEST_ID)
                            .setHost(HOST)
                            .useStaticContainer()
                            .build())
                .getMessage())
        .containsMatch("the hecPort and splunkdPort were not properly set");
  }

  @Test
  public void testGetHttpEndpointReturnsCorrectValue() {
    when(container.getMappedPort(DEFAULT_SPLUNK_HEC_INTERNAL_PORT))
        .thenReturn(MAPPED_SPLUNK_HEC_INTERNAL_PORT);
    assertThat(
            new SplunkResourceManager(
                    clientFactory, container, SplunkResourceManager.builder(TEST_ID))
                .getHttpEndpoint())
        .isEqualTo(String.format("%s://%s:%d", HEC_SCHEMA, HOST, MAPPED_SPLUNK_HEC_INTERNAL_PORT));
  }

  @Test
  public void testGetHecEndpointReturnsCorrectValue() {
    when(container.getMappedPort(DEFAULT_SPLUNK_HEC_INTERNAL_PORT))
        .thenReturn(MAPPED_SPLUNK_HEC_INTERNAL_PORT);
    assertThat(
            new SplunkResourceManager(
                    clientFactory, container, SplunkResourceManager.builder(TEST_ID))
                .getHecEndpoint())
        .isEqualTo(
            String.format(
                "%s://%s:%d/services/collector/event",
                HEC_SCHEMA, HOST, MAPPED_SPLUNK_HEC_INTERNAL_PORT));
  }

  @Test
  public void testGetHecTokenReturnsCorrectValueWhenSet() {
    assertThat(
            new SplunkResourceManager(
                    clientFactory,
                    container,
                    SplunkResourceManager.builder(TEST_ID).setHecToken(HEC_TOKEN))
                .getHecToken())
        .isEqualTo(HEC_TOKEN);
  }

  @Test
  public void testSendHttpEventsShouldThrowErrorWhenHttpClientFailsToExecuteRequest()
      throws IOException {
    SplunkEvent event = SplunkEvent.newBuilder().withEvent(EVENT).create();

    CloseableHttpClient mockHttpClient = clientFactory.getHttpClient();
    doThrow(IOException.class).when(mockHttpClient).execute(any(HttpPost.class));

    assertThrows(SplunkResourceManagerException.class, () -> testManager.sendHttpEvent(event));
  }

  @Test
  public void testSendHttpEventsShouldThrowErrorWhenHttpClientReturnsErrorCode()
      throws IOException {
    SplunkEvent event = SplunkEvent.newBuilder().withEvent(EVENT).create();

    try (CloseableHttpResponse mockResponse =
        clientFactory.getHttpClient().execute(any(HttpPost.class))) {
      when(mockResponse.getStatusLine().getStatusCode()).thenReturn(404);
    }

    assertThrows(SplunkResourceManagerException.class, () -> testManager.sendHttpEvent(event));
  }

  @Test
  public void testSendHttpEventsShouldReturnTrueIfSplunkDoesNotThrowAnyError() throws IOException {
    SplunkEvent event = SplunkEvent.newBuilder().withEvent(EVENT).create();

    try (CloseableHttpResponse mockResponse =
        clientFactory.getHttpClient().execute(any(HttpPost.class))) {
      when(mockResponse.getStatusLine().getStatusCode()).thenReturn(200);
    }

    assertThat(testManager.sendHttpEvents(ImmutableList.of(event, event))).isTrue();
    verify(clientFactory.getHttpClient()).execute(any(HttpPost.class));
  }

  @Test
  public void testGetEventsShouldThrowErrorWhenServiceClientFailsToExecuteRequest() {
    Job mockJob =
        clientFactory.getServiceClient(any(ServiceArgs.class)).getJobs().create(anyString());
    doThrow(ConditionTimeoutException.class).when(mockJob).isDone();

    assertThrows(ConditionTimeoutException.class, () -> testManager.getEvents(QUERY));
  }

  @Test
  public void testGetEventsShouldThrowErrorWhenXmlReaderFailsToParseResponse() {
    Job mockJob =
        clientFactory.getServiceClient(any(ServiceArgs.class)).getJobs().create(anyString());

    when(mockJob.isDone()).thenReturn(true);
    when(mockJob.getEvents())
        .thenReturn(
            new InputStream() {
              @Override
              public int read() throws IOException {
                throw new IOException();
              }
            });

    assertThrows(SplunkResourceManagerException.class, () -> testManager.getEvents(QUERY));
  }

  @Test
  public void testGetEventsShouldReturnTrueIfSplunkDoesNotThrowAnyError() {
    Job mockJob =
        clientFactory.getServiceClient(any(ServiceArgs.class)).getJobs().create(anyString());
    String rawEvent =
        "<results preview='0'>"
            + "<meta><fieldOrder>"
            + "<field>_raw</field><field>_sourcetype</field><field>_time</field>"
            + "<field>host</field><field>index</field><field>source</field>"
            + "</fieldOrder></meta>"
            + "<result offset='0'>"
            + "<field k='_raw'><v xml:space='preserve' trunc='0'>myEvent</v></field>"
            + "<field k='_sourcetype'><value><text>mySourceType</text></value></field>"
            + "<field k='_time'><value><text>1970-01-01T00:00:00.123+00:00</text></value></field>"
            + "<field k='host'><value><text>myHost</text></value></field>"
            + "<field k='index'><value><text>myIndex</text></value></field>"
            + "<field k='source'><value><text>mySource</text></value></field>"
            + "</result></results>";
    InputStream inputStream = new ByteArrayInputStream(rawEvent.getBytes(StandardCharsets.UTF_8));
    SplunkEvent splunkEvent =
        SplunkEvent.newBuilder()
            .withEvent("myEvent")
            .withHost("myHost")
            .withSource("mySource")
            .withSourceType("mySourceType")
            .withIndex("myIndex")
            .withTime(123L)
            .create();

    when(mockJob.isDone()).thenReturn(true);
    when(mockJob.getEvents()).thenReturn(inputStream);

    assertThat(testManager.getEvents())
        .containsExactlyElementsIn(Collections.singletonList(splunkEvent));
  }
}

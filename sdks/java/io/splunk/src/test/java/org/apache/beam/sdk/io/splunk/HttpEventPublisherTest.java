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
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.util.ExponentialBackOff;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import javax.net.ssl.SSLHandshakeException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.configuration.ConfigurationProperties;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link HttpEventPublisher}. */
public class HttpEventPublisherTest {

  private static final SplunkEvent SPLUNK_TEST_EVENT_1 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-1")
          .withHost("test-host-1")
          .withIndex("test-index-1")
          .withSource("test-source-1")
          .withSourceType("test-source-type-1")
          .withTime(12345L)
          .create();

  private static final SplunkEvent SPLUNK_TEST_EVENT_2 =
      SplunkEvent.newBuilder()
          .withEvent("test-event-2")
          .withHost("test-host-2")
          .withIndex("test-index-2")
          .withSource("test-source-2")
          .withSourceType("test-source-type-2")
          .withTime(12345L)
          .create();

  private static final ImmutableList<SplunkEvent> SPLUNK_EVENTS =
      ImmutableList.of(SPLUNK_TEST_EVENT_1, SPLUNK_TEST_EVENT_2);
  private static final String ROOT_CA_PATH = "SplunkTestCerts/RootCA.crt";
  private static final String ROOT_CA_KEY_PATH =
      Resources.getResource("SplunkTestCerts/RootCA_PrivateKey.pem").getPath();
  private static final String INCORRECT_ROOT_CA_PATH = "SplunkTestCerts/RootCA_2.crt";
  private static final String CERTIFICATE_PATH = "SplunkTestCerts/RecognizedCertificate.crt";
  private static final String KEY_PATH =
      Resources.getResource("SplunkTestCerts/PrivateKey.pem").getPath();
  private static final String EXPECTED_PATH = "/" + HttpEventPublisher.HEC_URL_PATH;
  private ClientAndServer mockServer;

  @Before
  public void setUp() throws IOException {
    ConfigurationProperties.disableSystemOut(true);
    ConfigurationProperties.privateKeyPath(KEY_PATH);
    ConfigurationProperties.x509CertificatePath(CERTIFICATE_PATH);
    ConfigurationProperties.certificateAuthorityCertificate(ROOT_CA_PATH);
    ConfigurationProperties.certificateAuthorityPrivateKey(ROOT_CA_KEY_PATH);
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    mockServer = startClientAndServer("localhost", port);
  }

  @Test
  public void stringPayloadTest()
      throws IOException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException,
          CertificateException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withEnableGzipHttpCompression(true)
            .build();

    String actual = publisher.getStringPayload(SPLUNK_EVENTS);

    String expected =
        "{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
            + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
            + "\"event\":\"test-event-1\"}{\"time\":12345,\"host\":\"test-host-2\","
            + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
            + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}";

    assertEquals(expected, actual);
  }

  @Test
  public void contentTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
          CertificateException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withEnableGzipHttpCompression(true)
            .build();

    String expectedString =
        "{\"time\":12345,\"host\":\"test-host-1\",\"source\":\"test-source-1\","
            + "\"sourcetype\":\"test-source-type-1\",\"index\":\"test-index-1\","
            + "\"event\":\"test-event-1\"}{\"time\":12345,\"host\":\"test-host-2\","
            + "\"source\":\"test-source-2\",\"sourcetype\":\"test-source-type-2\","
            + "\"index\":\"test-index-2\",\"event\":\"test-event-2\"}";

    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      HttpContent actualContent = publisher.getContent(SPLUNK_EVENTS);
      actualContent.writeTo(bos);
      String actualString = new String(bos.toByteArray(), StandardCharsets.UTF_8);
      assertEquals(expectedString, actualString);
    }
  }

  @Test
  public void genericURLTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    String baseURL = "http://example.com";
    HttpEventPublisher.Builder builder =
        HttpEventPublisher.newBuilder()
            .withUrl(baseURL)
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withEnableGzipHttpCompression(true);

    assertEquals(
        new GenericUrl(Joiner.on('/').join(baseURL, "services/collector/event")),
        builder.genericUrl());
  }

  @Test
  public void configureBackOffDefaultTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
          CertificateException {

    HttpEventPublisher publisherDefaultBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withEnableGzipHttpCompression(true)
            .build();

    assertEquals(
        ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS,
        publisherDefaultBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis());
  }

  @Test
  public void configureBackOffCustomTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException,
          CertificateException {

    int timeoutInMillis = 600000; // 10 minutes
    HttpEventPublisher publisherWithBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withEnableGzipHttpCompression(true)
            .withMaxElapsedMillis(timeoutInMillis)
            .build();

    assertEquals(
        timeoutInMillis, publisherWithBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis());
  }

  @Test(expected = CertificateException.class)
  public void invalidRootCaTest() throws Exception {
    HttpEventPublisher publisherWithInvalidCert =
        HttpEventPublisher.newBuilder()
            .withUrl("https://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withRootCaCertificate("invalid_ca".getBytes(StandardCharsets.UTF_8))
            .withEnableGzipHttpCompression(true)
            .build();
    System.out.println(publisherWithInvalidCert);
  }

  /** Tests if a self-signed certificate is trusted if its root CA is passed. */
  @Test
  public void recognizedSelfSignedCertificateTest() throws Exception {
    mockServerListening(200);
    byte[] rootCa =
        Resources.toString(Resources.getResource(ROOT_CA_PATH), StandardCharsets.UTF_8)
            .getBytes(StandardCharsets.UTF_8);
    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("https://localhost:" + String.valueOf(mockServer.getLocalPort()))
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withRootCaCertificate(rootCa)
            .withEnableGzipHttpCompression(true)
            .build();
    publisher.execute(SPLUNK_EVENTS);

    // Server received exactly one POST request.
    mockServer.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /**
   * Tests if a self-signed certificate is not trusted if it is not derived by the root CA which is
   * passed.
   */
  @Test(expected = SSLHandshakeException.class)
  public void unrecognizedSelfSignedCertificateTest() throws Exception {
    mockServerListening(200);
    byte[] rootCa =
        Resources.toString(Resources.getResource(INCORRECT_ROOT_CA_PATH), StandardCharsets.UTF_8)
            .getBytes(StandardCharsets.UTF_8);

    int timeoutInMillis = 5000; // 5 seconds
    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("https://localhost:" + String.valueOf(mockServer.getLocalPort()))
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withRootCaCertificate(rootCa)
            .withMaxElapsedMillis(timeoutInMillis)
            .withEnableGzipHttpCompression(true)
            .build();
    publisher.execute(SPLUNK_EVENTS);
  }

  private void mockServerListening(int statusCode) {
    mockServer
        .when(HttpRequest.request(EXPECTED_PATH))
        .respond(HttpResponse.response().withStatusCode(statusCode));
  }
}

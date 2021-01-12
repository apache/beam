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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpContent;
import com.google.api.client.util.ExponentialBackOff;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;

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

  @Test
  public void stringPayloadTest()
      throws UnsupportedEncodingException, NoSuchAlgorithmException, KeyStoreException,
          KeyManagementException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
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
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    HttpEventPublisher publisher =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
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
            .withDisableCertificateValidation(false);

    assertEquals(
        new GenericUrl(Joiner.on('/').join(baseURL, "services/collector/event")),
        builder.genericUrl());
  }

  @Test
  public void configureBackOffDefaultTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    HttpEventPublisher publisherDefaultBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .build();

    assertEquals(
        ExponentialBackOff.DEFAULT_MAX_ELAPSED_TIME_MILLIS,
        publisherDefaultBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis());
  }

  @Test
  public void configureBackOffCustomTest()
      throws NoSuchAlgorithmException, KeyStoreException, KeyManagementException, IOException {

    int timeoutInMillis = 600000; // 10 minutes
    HttpEventPublisher publisherWithBackOff =
        HttpEventPublisher.newBuilder()
            .withUrl("http://example.com")
            .withToken("test-token")
            .withDisableCertificateValidation(false)
            .withMaxElapsedMillis(timeoutInMillis)
            .build();

    assertEquals(
        timeoutInMillis, publisherWithBackOff.getConfiguredBackOff().getMaxElapsedTimeMillis());
  }
}

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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.storage.Storage;
import java.io.IOException;
import org.apache.beam.sdk.metrics.Histogram;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for LatencyRecordingHttpRequestInitializer. */
@RunWith(JUnit4.class)
public class LatencyRecordingHttpRequestInitializerTest {

  @Rule
  public ExpectedLogs expectedLogs =
      ExpectedLogs.none(LatencyRecordingHttpRequestInitializer.class);

  @Mock private LowLevelHttpRequest mockLowLevelRequest;
  @Mock private LowLevelHttpResponse mockLowLevelResponse;
  @Mock private Histogram mockHistogram;

  private final JsonFactory jsonFactory = GsonFactory.getDefaultInstance();
  private Storage storage;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    HttpTransport lowLevelTransport =
        new HttpTransport() {
          @Override
          protected LowLevelHttpRequest buildRequest(String method, String url) throws IOException {
            return mockLowLevelRequest;
          }
        };

    LatencyRecordingHttpRequestInitializer initializer =
        new LatencyRecordingHttpRequestInitializer(mockHistogram);
    storage =
        new Storage.Builder(lowLevelTransport, jsonFactory, initializer)
            .setApplicationName("test")
            .build();
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockLowLevelRequest);
  }

  @Test
  public void testOkResponse() throws IOException {
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode()).thenReturn(200);

    Storage.Buckets.Get result = storage.buckets().get("test");
    HttpResponse response = result.executeUnparsed();
    assertNotNull(response);

    verify(mockHistogram, only()).update(anyDouble());
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest).execute();
    verify(mockLowLevelResponse, atLeastOnce()).getStatusCode();
  }

  @Test
  public void testErrorResponse() throws IOException {
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode()).thenReturn(403);

    try {
      Storage.Buckets.Get result = storage.buckets().get("test");
      HttpResponse response = result.executeUnparsed();
      assertNotNull(response);
    } catch (HttpResponseException e) {
      assertThat(e.getMessage(), Matchers.containsString("403"));
    }

    verify(mockHistogram, only()).update(anyDouble());
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest).execute();
    verify(mockLowLevelResponse, atLeastOnce()).getStatusCode();
  }
}

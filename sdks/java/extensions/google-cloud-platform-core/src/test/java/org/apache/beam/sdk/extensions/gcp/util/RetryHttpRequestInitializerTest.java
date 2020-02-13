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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.util.NanoClock;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.Storage.Objects.Get;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.security.PrivateKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for RetryHttpRequestInitializer. */
@RunWith(JUnit4.class)
public class RetryHttpRequestInitializerTest {

  @Rule public ExpectedLogs expectedLogs = ExpectedLogs.none(RetryHttpRequestInitializer.class);

  @Mock private PrivateKey mockPrivateKey;
  @Mock private LowLevelHttpRequest mockLowLevelRequest;
  @Mock private LowLevelHttpResponse mockLowLevelResponse;
  @Mock private HttpResponseInterceptor mockHttpResponseInterceptor;

  private final JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
  private Storage storage;

  // Used to test retrying a request more than the default 10 times.
  static class MockNanoClock implements NanoClock {
    private int[] timesMs = {
      500, 750, 1125, 1688, 2531, 3797, 5695, 8543, 12814, 19222, 28833, 43249, 64873, 97310,
      145965, 218945, 328420
    };
    private int i = 0;

    @Override
    public long nanoTime() {
      return timesMs[i++ / 2] * 1000000L;
    }
  }

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

    // Retry initializer will pass through to credential, since we can have
    // only a single HttpRequestInitializer, and we use multiple Credential
    // types in the SDK, not all of which allow for retry configuration.
    RetryHttpRequestInitializer initializer =
        new RetryHttpRequestInitializer(
            new MockNanoClock(),
            millis -> {},
            Arrays.asList(418 /* I'm a teapot */),
            mockHttpResponseInterceptor);
    storage =
        new Storage.Builder(lowLevelTransport, jsonFactory, initializer)
            .setApplicationName("test")
            .build();
  }

  @After
  public void tearDown() {
    verifyNoMoreInteractions(mockPrivateKey);
    verifyNoMoreInteractions(mockLowLevelRequest);
    verifyNoMoreInteractions(mockHttpResponseInterceptor);
  }

  @Test
  public void testBasicOperation() throws IOException {
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode()).thenReturn(200);

    Storage.Buckets.Get result = storage.buckets().get("test");
    HttpResponse response = result.executeUnparsed();
    assertNotNull(response);

    verify(mockHttpResponseInterceptor).interceptResponse(any(HttpResponse.class));
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest).execute();
    verify(mockLowLevelResponse).getStatusCode();
    expectedLogs.verifyNotLogged("Request failed");
  }

  /** Tests that a non-retriable error is not retried. */
  @Test
  public void testErrorCodeForbidden() throws IOException {
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(403) // Non-retryable error.
        .thenReturn(200); // Shouldn't happen.

    try {
      Storage.Buckets.Get result = storage.buckets().get("test");
      HttpResponse response = result.executeUnparsed();
      assertNotNull(response);
    } catch (HttpResponseException e) {
      assertThat(e.getMessage(), Matchers.containsString("403"));
    }

    verify(mockHttpResponseInterceptor).interceptResponse(any(HttpResponse.class));
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest).execute();
    verify(mockLowLevelResponse).getStatusCode();
    expectedLogs.verifyWarn("Request failed with code 403");
  }

  /** Tests that a retriable error is retried. */
  @Test
  public void testRetryableError() throws IOException {
    when(mockLowLevelRequest.execute())
        .thenReturn(mockLowLevelResponse)
        .thenReturn(mockLowLevelResponse)
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode())
        .thenReturn(503) // Retryable
        .thenReturn(429) // We also retry on 429 Too Many Requests.
        .thenReturn(200);

    Storage.Buckets.Get result = storage.buckets().get("test");
    HttpResponse response = result.executeUnparsed();
    assertNotNull(response);

    verify(mockHttpResponseInterceptor).interceptResponse(any(HttpResponse.class));
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest, times(3)).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest, times(3)).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest, times(3)).execute();
    verify(mockLowLevelResponse, times(3)).getStatusCode();
    expectedLogs.verifyDebug("Request failed with code 503");
  }

  /** Tests that an IOException is retried. */
  @Test
  public void testThrowIOException() throws IOException {
    when(mockLowLevelRequest.execute())
        .thenThrow(new IOException("Fake Error"))
        .thenReturn(mockLowLevelResponse);
    when(mockLowLevelResponse.getStatusCode()).thenReturn(200);

    Storage.Buckets.Get result = storage.buckets().get("test");
    HttpResponse response = result.executeUnparsed();
    assertNotNull(response);

    verify(mockHttpResponseInterceptor).interceptResponse(any(HttpResponse.class));
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest, times(2)).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest, times(2)).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest, times(2)).execute();
    verify(mockLowLevelResponse).getStatusCode();
    expectedLogs.verifyDebug("Request failed with IOException");
  }

  /** Tests that a retryable error is retried enough times. */
  @Test
  public void testRetryableErrorRetryEnoughTimes() throws IOException {
    when(mockLowLevelRequest.execute()).thenReturn(mockLowLevelResponse);
    final int retries = 10;
    when(mockLowLevelResponse.getStatusCode())
        .thenAnswer(
            new Answer<Integer>() {
              int n = 0;

              @Override
              public Integer answer(InvocationOnMock invocation) {
                return n++ < retries ? 503 : 9999;
              }
            });

    Storage.Buckets.Get result = storage.buckets().get("test");
    try {
      result.executeUnparsed();
      fail();
    } catch (IOException e) {
    }

    verify(mockHttpResponseInterceptor).interceptResponse(any(HttpResponse.class));
    verify(mockLowLevelRequest, atLeastOnce()).addHeader(anyString(), anyString());
    verify(mockLowLevelRequest, times(retries + 1)).setTimeout(anyInt(), anyInt());
    verify(mockLowLevelRequest, times(retries + 1)).setWriteTimeout(anyInt());
    verify(mockLowLevelRequest, times(retries + 1)).execute();
    verify(mockLowLevelResponse, times(retries + 1)).getStatusCode();
    expectedLogs.verifyWarn("performed 10 retries due to unsuccessful status codes");
  }

  /**
   * Tests that when RPCs fail with {@link SocketTimeoutException}, the IO exception handler is
   * invoked.
   */
  @Test
  @SuppressWarnings("AssertionFailureIgnored")
  public void testIOExceptionHandlerIsInvokedOnTimeout() throws Exception {
    FastNanoClockAndSleeper fakeClockAndSleeper = new FastNanoClockAndSleeper();
    // Counts the number of calls to execute the HTTP request.
    final AtomicLong executeCount = new AtomicLong();

    // 10 is a private internal constant in the Google API Client library. See
    // com.google.api.client.http.HttpRequest#setNumberOfRetries
    // TODO: update this test once the private internal constant is public.
    final int defaultNumberOfRetries = 10;

    // A mock HTTP request that always throws SocketTimeoutException.
    MockHttpTransport transport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpRequest(
                new MockLowLevelHttpRequest() {
                  @Override
                  public LowLevelHttpResponse execute() throws IOException {
                    executeCount.incrementAndGet();
                    throw new SocketTimeoutException("Fake forced timeout exception");
                  }
                })
            .build();

    // A sample HTTP request to Google Cloud Storage that uses both a default Transport and
    // effectively a default RetryHttpRequestInitializer (same args as default with fake
    // clock/sleeper).
    Storage storage =
        new Storage.Builder(
                transport,
                Transport.getJsonFactory(),
                new RetryHttpRequestInitializer(
                    fakeClockAndSleeper, fakeClockAndSleeper, Collections.emptyList(), null))
            .build();

    Get getRequest = storage.objects().get("gs://fake", "file");

    Throwable thrown = null;
    try {
      getRequest.execute();
    } catch (Throwable e) {
      thrown = e;
    }
    assertNotNull("Expected execute to throw an exception", thrown);
    assertThat(thrown, Matchers.instanceOf(SocketTimeoutException.class));
    assertEquals(1 + defaultNumberOfRetries, executeCount.get());
    expectedLogs.verifyWarn("performed 10 retries due to IOExceptions");
  }
}

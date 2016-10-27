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
package org.apache.beam.sdk.util;

import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.NanoClock;
import com.google.api.client.util.Sleeper;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a request initializer that adds retry handlers to all
 * HttpRequests.
 *
 * <p>This allows chaining through to another HttpRequestInitializer, since
 * clients have exactly one HttpRequestInitializer, and Credential is also
 * a required HttpRequestInitializer.
 *
 * <p>Also can take a HttpResponseInterceptor to be applied to the responses.
 */
public class RetryHttpRequestInitializer implements HttpRequestInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(RetryHttpRequestInitializer.class);

  /**
   * Http response codes that should be silently ignored.
   */
  private static final Set<Integer> DEFAULT_IGNORED_RESPONSE_CODES = new HashSet<>(
      Arrays.asList(307 /* Redirect, handled by the client library */,
                    308 /* Resume Incomplete, handled by the client library */));

  /**
   * Http response timeout to use for hanging gets.
   */
  private static final int HANGING_GET_TIMEOUT_SEC = 80;

  private static class LoggingHttpBackOffIOExceptionHandler
      extends HttpBackOffIOExceptionHandler {
    public LoggingHttpBackOffIOExceptionHandler(BackOff backOff) {
      super(backOff);
    }

    @Override
    public boolean handleIOException(HttpRequest request, boolean supportsRetry)
        throws IOException {
      boolean willRetry = super.handleIOException(request, supportsRetry);
      if (willRetry) {
        LOG.debug("Request failed with IOException, will retry: {}", request.getUrl());
      } else {
        LOG.warn("Request failed with IOException, will NOT retry: {}", request.getUrl());
      }
      return willRetry;
    }
  }

  private static class LoggingHttpBackoffUnsuccessfulResponseHandler
      implements HttpUnsuccessfulResponseHandler {
    private final HttpBackOffUnsuccessfulResponseHandler handler;
    private final Set<Integer> ignoredResponseCodes;

    public LoggingHttpBackoffUnsuccessfulResponseHandler(BackOff backoff,
        Sleeper sleeper, Set<Integer> ignoredResponseCodes) {
      this.ignoredResponseCodes = ignoredResponseCodes;
      handler = new HttpBackOffUnsuccessfulResponseHandler(backoff);
      handler.setSleeper(sleeper);
      handler.setBackOffRequired(
          new HttpBackOffUnsuccessfulResponseHandler.BackOffRequired() {
            @Override
            public boolean isRequired(HttpResponse response) {
              int statusCode = response.getStatusCode();
              return (statusCode / 100 == 5) ||  // 5xx: server error
                  statusCode == 429;             // 429: Too many requests
            }
          });
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response,
        boolean supportsRetry) throws IOException {
      boolean retry = handler.handleResponse(request, response, supportsRetry);
      if (retry) {
        LOG.debug("Request failed with code {} will retry: {}",
            response.getStatusCode(), request.getUrl());

      } else if (!ignoredResponseCodes.contains(response.getStatusCode())) {
        LOG.warn("Request failed with code {}, will NOT retry: {}",
            response.getStatusCode(), request.getUrl());
      }

      return retry;
    }
  }

  @Deprecated
  private final HttpRequestInitializer chained;

  private final HttpResponseInterceptor responseInterceptor;  // response Interceptor to use

  private final NanoClock nanoClock;  // used for testing

  private final Sleeper sleeper;  // used for testing

  private Set<Integer> ignoredResponseCodes = new HashSet<>(DEFAULT_IGNORED_RESPONSE_CODES);

  public RetryHttpRequestInitializer() {
    this(Collections.<Integer>emptyList());
  }

  /**
   * @param chained a downstream HttpRequestInitializer, which will also be
   *                applied to HttpRequest initialization.  May be null.
   *
   * @deprecated use {@link #RetryHttpRequestInitializer}.
   */
  @Deprecated
  public RetryHttpRequestInitializer(@Nullable HttpRequestInitializer chained) {
    this(chained, Collections.<Integer>emptyList());
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  public RetryHttpRequestInitializer(Collection<Integer> additionalIgnoredResponseCodes) {
    this(additionalIgnoredResponseCodes, null);
  }


  /**
   * @param chained a downstream HttpRequestInitializer, which will also be
   *                applied to HttpRequest initialization.  May be null.
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   *
   * @deprecated use {@link #RetryHttpRequestInitializer(Collection)}.
   */
  @Deprecated
  public RetryHttpRequestInitializer(@Nullable HttpRequestInitializer chained,
      Collection<Integer> additionalIgnoredResponseCodes) {
    this(chained, additionalIgnoredResponseCodes, null);
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   * @param responseInterceptor HttpResponseInterceptor to be applied on all requests. May be null.
   */
  public RetryHttpRequestInitializer(
      Collection<Integer> additionalIgnoredResponseCodes,
      @Nullable HttpResponseInterceptor responseInterceptor) {
    this(null, NanoClock.SYSTEM, Sleeper.DEFAULT, additionalIgnoredResponseCodes,
        responseInterceptor);
  }

  /**
   * @param chained a downstream HttpRequestInitializer, which will also be applied to HttpRequest
   * initialization.  May be null.
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   * @param responseInterceptor HttpResponseInterceptor to be applied on all requests. May be null.
   *
   * @deprecated use {@link #RetryHttpRequestInitializer(Collection, HttpResponseInterceptor)}.
   */
  @Deprecated
  public RetryHttpRequestInitializer(
      @Nullable HttpRequestInitializer chained,
      Collection<Integer> additionalIgnoredResponseCodes,
      @Nullable HttpResponseInterceptor responseInterceptor) {
    this(chained, NanoClock.SYSTEM, Sleeper.DEFAULT, additionalIgnoredResponseCodes,
        responseInterceptor);
  }

  /**
   * Visible for testing.
   *
   * @param chained a downstream HttpRequestInitializer, which will also be
   *                applied to HttpRequest initialization.  May be null.
   * @param nanoClock used as a timing source for knowing how much time has elapsed.
   * @param sleeper used to sleep between retries.
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  RetryHttpRequestInitializer(@Nullable HttpRequestInitializer chained,
      NanoClock nanoClock, Sleeper sleeper, Collection<Integer> additionalIgnoredResponseCodes,
      HttpResponseInterceptor responseInterceptor) {
    this.chained = chained;
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
    this.ignoredResponseCodes.addAll(additionalIgnoredResponseCodes);
    this.responseInterceptor = responseInterceptor;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    if (chained != null) {
      chained.initialize(request);
    }

    // Set a timeout for hanging-gets.
    // TODO: Do this exclusively for work requests.
    request.setReadTimeout(HANGING_GET_TIMEOUT_SEC * 1000);

    // Back off on retryable http errors.
    request.setUnsuccessfulResponseHandler(
        // A back-off multiplier of 2 raises the maximum request retrying time
        // to approximately 5 minutes (keeping other back-off parameters to
        // their default values).
        new LoggingHttpBackoffUnsuccessfulResponseHandler(
            new ExponentialBackOff.Builder().setNanoClock(nanoClock)
                                            .setMultiplier(2).build(),
            sleeper, ignoredResponseCodes));

    // Retry immediately on IOExceptions.
    LoggingHttpBackOffIOExceptionHandler loggingBackoffHandler =
        new LoggingHttpBackOffIOExceptionHandler(BackOff.ZERO_BACKOFF);
    request.setIOExceptionHandler(loggingBackoffHandler);

    // Set response initializer
    if (responseInterceptor != null) {
      request.setResponseInterceptor(responseInterceptor);
    }
  }
}

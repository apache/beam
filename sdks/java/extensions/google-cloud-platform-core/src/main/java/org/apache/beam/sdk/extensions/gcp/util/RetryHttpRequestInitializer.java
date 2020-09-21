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

import com.google.api.client.http.HttpIOExceptionHandler;
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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a request initializer that adds retry handlers to all HttpRequests.
 *
 * <p>Also can take an HttpResponseInterceptor to be applied to the responses.
 */
public class RetryHttpRequestInitializer implements HttpRequestInitializer {

  private static final Logger LOG = LoggerFactory.getLogger(RetryHttpRequestInitializer.class);

  /** Http response codes that should be silently ignored. */
  private static final Set<Integer> DEFAULT_IGNORED_RESPONSE_CODES =
      new HashSet<>(
          Arrays.asList(
              307 /* Redirect, handled by the client library */,
              308 /* Resume Incomplete, handled by the client library */));

  /** Http response timeout to use for hanging gets. */
  private static final int HANGING_GET_TIMEOUT_SEC = 80;

  private int writeTimeout;

  /** Handlers used to provide additional logging information on unsuccessful HTTP requests. */
  private static class LoggingHttpBackOffHandler
      implements HttpIOExceptionHandler, HttpUnsuccessfulResponseHandler {

    private final Sleeper sleeper;
    private final BackOff ioExceptionBackOff;
    private final BackOff unsuccessfulResponseBackOff;
    private final Set<Integer> ignoredResponseCodes;
    // aggregate the total time spent in exponential backoff
    private final Counter throttlingMsecs =
        Metrics.counter(LoggingHttpBackOffHandler.class, "throttling-msecs");
    private int ioExceptionRetries;
    private int unsuccessfulResponseRetries;
    private @Nullable CustomHttpErrors customHttpErrors;

    private LoggingHttpBackOffHandler(
        Sleeper sleeper,
        BackOff ioExceptionBackOff,
        BackOff unsucessfulResponseBackOff,
        Set<Integer> ignoredResponseCodes,
        @Nullable CustomHttpErrors customHttpErrors) {
      this.sleeper = sleeper;
      this.ioExceptionBackOff = ioExceptionBackOff;
      this.unsuccessfulResponseBackOff = unsucessfulResponseBackOff;
      this.ignoredResponseCodes = ignoredResponseCodes;
      this.customHttpErrors = customHttpErrors;
    }

    @Override
    public boolean handleIOException(HttpRequest request, boolean supportsRetry)
        throws IOException {
      // We will retry if the request supports retry or the backoff was successful.
      // Note that the order of these checks is important since
      // backOffWasSuccessful will perform a sleep.
      boolean willRetry = supportsRetry && backOffWasSuccessful(ioExceptionBackOff);
      if (willRetry) {
        ioExceptionRetries += 1;
        LOG.debug("Request failed with IOException, will retry: {}", request.getUrl());
      } else {
        String message =
            "Request failed with IOException, "
                + "performed {} retries due to IOExceptions, "
                + "performed {} retries due to unsuccessful status codes, "
                + "HTTP framework says request {} be retried, "
                + "(caller responsible for retrying): {}";
        LOG.warn(
            message,
            ioExceptionRetries,
            unsuccessfulResponseRetries,
            supportsRetry ? "can" : "cannot",
            request.getUrl());
      }
      return willRetry;
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry)
        throws IOException {
      // We will retry if the request supports retry and the status code requires a backoff
      // and the backoff was successful. Note that the order of these checks is important since
      // backOffWasSuccessful will perform a sleep.
      boolean willRetry =
          supportsRetry
              && retryOnStatusCode(response.getStatusCode())
              && backOffWasSuccessful(unsuccessfulResponseBackOff);
      if (willRetry) {
        unsuccessfulResponseRetries += 1;
        LOG.debug(
            "Request failed with code {}, will retry: {}",
            response.getStatusCode(),
            request.getUrl());
      } else {

        String message =
            "Request failed with code {}, "
                + "performed {} retries due to IOExceptions, "
                + "performed {} retries due to unsuccessful status codes, "
                + "HTTP framework says request {} be retried, "
                + "(caller responsible for retrying): {}. {}";
        String customLogMessage = "";
        if (customHttpErrors != null) {
          String error =
              customHttpErrors.getCustomError(
                  new HttpRequestWrapper(request), new HttpResponseWrapper(response));
          if (error != null) {
            customLogMessage = error;
          }
        }
        if (ignoredResponseCodes.contains(response.getStatusCode())) {
          // Log ignored response codes at a lower level
          LOG.debug(
              message,
              response.getStatusCode(),
              ioExceptionRetries,
              unsuccessfulResponseRetries,
              supportsRetry ? "can" : "cannot",
              request.getUrl(),
              customLogMessage);
        } else {
          LOG.warn(
              message,
              response.getStatusCode(),
              ioExceptionRetries,
              unsuccessfulResponseRetries,
              supportsRetry ? "can" : "cannot",
              request.getUrl(),
              customLogMessage);
        }
      }
      return willRetry;
    }

    /** Returns true iff performing the backoff was successful. */
    private boolean backOffWasSuccessful(BackOff backOff) {
      try {
        long backOffTime = backOff.nextBackOffMillis();
        if (backOffTime == BackOff.STOP) {
          return false;
        }
        throttlingMsecs.inc(backOffTime);
        sleeper.sleep(backOffTime);
        return true;
      } catch (InterruptedException | IOException e) {
        return false;
      }
    }

    /** Returns true iff the {@code statusCode} represents an error that should be retried. */
    private boolean retryOnStatusCode(int statusCode) {
      return (statusCode == 0) // Code 0 usually means no response / network error
          || (statusCode / 100 == 5) // 5xx: server error
          || statusCode == 429; // 429: Too many requests
    }
  }

  private final HttpResponseInterceptor responseInterceptor; // response Interceptor to use

  private CustomHttpErrors customHttpErrors = null;

  private final NanoClock nanoClock; // used for testing

  private final Sleeper sleeper; // used for testing

  private Set<Integer> ignoredResponseCodes = new HashSet<>(DEFAULT_IGNORED_RESPONSE_CODES);

  public RetryHttpRequestInitializer() {
    this(Collections.emptyList());
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  public RetryHttpRequestInitializer(Collection<Integer> additionalIgnoredResponseCodes) {
    this(additionalIgnoredResponseCodes, null);
  }

  /**
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   * @param responseInterceptor HttpResponseInterceptor to be applied on all requests. May be null.
   */
  public RetryHttpRequestInitializer(
      Collection<Integer> additionalIgnoredResponseCodes,
      @Nullable HttpResponseInterceptor responseInterceptor) {
    this(NanoClock.SYSTEM, Sleeper.DEFAULT, additionalIgnoredResponseCodes, responseInterceptor);
  }

  /**
   * Visible for testing.
   *
   * @param nanoClock used as a timing source for knowing how much time has elapsed.
   * @param sleeper used to sleep between retries.
   * @param additionalIgnoredResponseCodes a list of HTTP status codes that should not be logged.
   */
  RetryHttpRequestInitializer(
      NanoClock nanoClock,
      Sleeper sleeper,
      Collection<Integer> additionalIgnoredResponseCodes,
      HttpResponseInterceptor responseInterceptor) {
    this.nanoClock = nanoClock;
    this.sleeper = sleeper;
    this.ignoredResponseCodes.addAll(additionalIgnoredResponseCodes);
    this.responseInterceptor = responseInterceptor;
    this.writeTimeout = 0;
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    // Set a timeout for hanging-gets.
    // TODO: Do this exclusively for work requests.
    request.setReadTimeout(HANGING_GET_TIMEOUT_SEC * 1000);
    request.setWriteTimeout(this.writeTimeout * 1000);

    LoggingHttpBackOffHandler loggingHttpBackOffHandler =
        new LoggingHttpBackOffHandler(
            sleeper,
            // Back off on retryable http errors and IOExceptions.
            // A back-off multiplier of 2 raises the maximum request retrying time
            // to approximately 5 minutes (keeping other back-off parameters to
            // their default values).
            new ExponentialBackOff.Builder().setNanoClock(nanoClock).setMultiplier(2).build(),
            new ExponentialBackOff.Builder().setNanoClock(nanoClock).setMultiplier(2).build(),
            ignoredResponseCodes,
            this.customHttpErrors);

    request.setUnsuccessfulResponseHandler(loggingHttpBackOffHandler);
    request.setIOExceptionHandler(loggingHttpBackOffHandler);

    // Set response initializer
    if (responseInterceptor != null) {
      request.setResponseInterceptor(responseInterceptor);
    }
  }

  public void setCustomErrors(CustomHttpErrors customErrors) {
    this.customHttpErrors = customErrors;
  }

  /**
   * @param writeTimeout in seconds.
   */
  public void setWriteTimeout(int writeTimeout) {
    this.writeTimeout = writeTimeout;
  }
}

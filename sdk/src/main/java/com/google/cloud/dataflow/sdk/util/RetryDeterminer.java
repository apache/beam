/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/


package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.http.HttpResponseException;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import javax.net.ssl.SSLException;

/**
 * This abstract class is designed to tell if an exception is transient and should result in a
 * retry or not, and should result in a returned exception to the caller. Meant to be used with
 * a {@link ResilientOperation}.
 *
 * @param <X> The type of exception you are checking and could possibly return.
 */
public abstract class RetryDeterminer<X extends Exception> {
  /**
   *  Retries when either SOCKET_ERRORS or SERVER_ERRORS would retry.
   */
  public static final RetryDeterminer<Exception> DEFAULT = new RetryDeterminer<Exception>() {
    @Override
    public boolean shouldRetry(Exception e) {
      if (e instanceof IOException) {
        return SOCKET_ERRORS.shouldRetry((IOException) e)
            || SERVER_ERRORS.shouldRetry((IOException) e);
      }
      return false;
    }
  };

  /**
   * Socket errors retry determiner retries on socket exceptions and ssl exceptions. Note:
   * Assumes that the new SSL connection would be re-established inside the retry. If this is not
   * true, then retrying after a failed SSL connection would not help.
   */
  public static final RetryDeterminer<IOException> SOCKET_ERRORS =
      new RetryDeterminer<IOException>() {
    @Override
    public boolean shouldRetry(IOException e) {
      /* Assumes that the ssl connection happens within the retry. This is true for the {@link
       * AbstractGoogleClientRequest} execute functions. SocketTimeoutExceptions are thrown only
       * for timeouts and it is safe to retry on them even if connect isn't new. We want to pass
       * any other exceptions back up including InterruptedExceptions.
       */
      return e instanceof SSLException || e instanceof SocketException
          || e instanceof SocketTimeoutException;
    }
  };

  /**
   *  Server errors RetryDeterminer decides to retry on HttpResponseExceptions that return a 500.
   */
  public static final RetryDeterminer<IOException> SERVER_ERRORS =
      new RetryDeterminer<IOException>() {
    @Override
    public boolean shouldRetry(IOException e) {
      if (e instanceof HttpResponseException) {
        HttpResponseException httpException = (HttpResponseException) e;
        // TODO: Find what we should do for 500 codes that are not always transient.
        return httpException.getStatusCode() / 100 == 5;
      }
      return false;
    }
  };

  /**
   * Determines if we should attempt a retry depending on the caught exception.
   * <p>
   * To indicate that no retry should be made, return false. If no retry,
   * the exception should be returned to the user.
   *
   * @param e Exception of type X that can be examined to determine if a retry is possible.
   * @return true if should retry, false otherwise
   */
  public abstract boolean shouldRetry(X e);
}

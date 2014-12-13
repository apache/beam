/**
 * Copyright 2013 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

/**
 * Translates exceptions from API calls into higher-level meaning, while allowing injectability
 * for testing how API errors are handled.
 */
public class ApiErrorExtractor {

  public static final int STATUS_CODE_CONFLICT = 409;
  public static final int STATUS_CODE_RANGE_NOT_SATISFIABLE = 416;

  /**
   * Determines if the given exception indicates 'item not found'.
   */
  public boolean itemNotFound(IOException e) {
    if (e instanceof GoogleJsonResponseException) {
      return (getHttpStatusCode((GoogleJsonResponseException) e)) ==
          HttpStatusCodes.STATUS_CODE_NOT_FOUND;
    }
    return false;
  }

  /**
   * Determines if the given GoogleJsonError indicates 'item not found'.
   */
  public boolean itemNotFound(GoogleJsonError e) {
    return e.getCode() == HttpStatusCodes.STATUS_CODE_NOT_FOUND;
  }

  /**
   * Checks if HTTP status code indicates the error specified.
   */
  private boolean hasHttpCode(IOException e, int code) {
    if (e instanceof GoogleJsonResponseException) {
      return (getHttpStatusCode((GoogleJsonResponseException) e)) == code;
    }
    return false;
  }

  /**
   * Determines if the given exception indicates 'conflict' (already exists).
   */
  public boolean alreadyExists(IOException e) {
    return hasHttpCode(e, STATUS_CODE_CONFLICT);
  }

  /**
   * Determines if the given exception indicates 'range not satisfiable'.
   */
  public boolean rangeNotSatisfiable(IOException e) {
    return hasHttpCode(e, STATUS_CODE_RANGE_NOT_SATISFIABLE);
  }

  /**
   * Determines if the given exception indicates 'access denied'.
   */
  public boolean accessDenied(GoogleJsonResponseException e) {
    return getHttpStatusCode(e) == HttpStatusCodes.STATUS_CODE_FORBIDDEN;
  }

  /**
   * Determines if the given exception indicates 'access denied', recursively checking inner
   * getCause() if outer exception isn't an instance of the correct class.
   */
  public boolean accessDenied(IOException e) {
    return (e.getCause() != null) &&
        (e.getCause() instanceof GoogleJsonResponseException) &&
        accessDenied((GoogleJsonResponseException) e.getCause());
  }

  /**
   * Returns HTTP status code from the given exception.
   *
   * Note: GoogleJsonResponseException.getStatusCode() method is marked final therefore
   * it cannot be mocked using Mockito. We use this helper so that we can override it in tests.
   */
  @VisibleForTesting
  int getHttpStatusCode(GoogleJsonResponseException e) {
    return e.getStatusCode();
  }
}

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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An optional component to use with the RetryHttpRequestInitializer in order to provide custom
 * errors for failing http calls. This class allows you to specify custom error messages which match
 * specific error codes and containing strings in the URL.
 *
 * <p>The intended use case here is to examine one of the logs emitted by a failing call made by the
 * RetryHttpRequestInitializer, and then adding a custom error message which matches the URL and
 * code for it.
 *
 * <p>Usage: See more in CustomHttpErrorsTest.
 *
 * <p>CustomHttpErrors.Builder builder = new CustomHttpErrors.Builder();
 * builder.addErrorForCodeAndUrlContains(403,"/tables?", "Custom Error Msg"); CustomHttpErrors
 * customErrors = builder.build();
 *
 * <p>RetryHttpRequestInitializer initializer = ... initializer.setCustomErrors(customErrors);
 *
 * <p>Suggestions for future enhancements to anyone upgrading this file: - This class is left open
 * for extension, to allow different functions for HttpCallMatcher and HttpCallCustomError to match
 * and log errors. For example, new functionality may including matching and error based on the
 * HttpResponse body, and logging may include extracting and logging strings from the HttpResponse
 * body as well. - Add a methods to add custom errors based on inspecting the contents of the
 * HttpRequest and HttpResponse - Be sure to update the HttpRequestWrapper and HttpResponseWrapper
 * with any new getters that you may use. The wrappers were introduced to add a layer of indirection
 * which could be mocked out in tests. This was unfortunately needed because mockito cannot mock
 * final classes and its non trivial to just construct HttpRequest and HttpResponse objects.
 */
public class CustomHttpErrors {

  private static final Logger LOG = LoggerFactory.getLogger(CustomHttpErrors.class);

  /**
   * A simple Tuple class for creating a list of HttpResponseMatcher and HttpResponseCustomError to
   * print for the responses.
   */
  @AutoValue
  public abstract static class MatcherAndError implements Serializable {
    static MatcherAndError create(HttpCallMatcher matcher, HttpCallCustomError customError) {
      return new AutoValue_CustomHttpErrors_MatcherAndError(matcher, customError);
    }

    public abstract HttpCallMatcher getMatcher();

    public abstract HttpCallCustomError getCustomError();
  }

  /**
   * A Builder which allows building immutable CustomHttpErrors object.
   */
  public static class Builder {

    private List<MatcherAndError> matchersAndLogs = new ArrayList<MatcherAndError>();

    public CustomHttpErrors build() {
      return new CustomHttpErrors(this.matchersAndLogs);
    }

    /** Adds a matcher to log the provided string if the error matches a particular status code. */
    public void addErrorForCode(int statusCode, String errorMessage) {
      HttpCallMatcher matcher =
          (request, response) -> {
            if (response.getStatusCode() == statusCode) {
              return true;
            }
            return false;
          };
      this.matchersAndLogs.add(MatcherAndError.create(matcher, simpleErrorMessage(errorMessage)));
    }

    /**
     * Adds a matcher to log the provided string if the error matches a particular status code and
     * the url contains a certain string.
     */
    public void addErrorForCodeAndUrlContains(
        int statusCode, String urlContains, String errorMessage) {
      HttpCallMatcher matcher =
          (request, response) -> {
            if (response.getStatusCode() == statusCode
                && request.getUrl().toString().contains(urlContains)) {
              return true;
            }
            return false;
          };
      this.matchersAndLogs.add(MatcherAndError.create(matcher, simpleErrorMessage(errorMessage)));
    }

    private static HttpCallCustomError simpleErrorMessage(String errorMessage) {
      return (request, response) -> {
        return errorMessage;
      };
    }
  }

  // The list of HttpRequest/Response matchers and functions to generate error strings.
  private List<MatcherAndError> matchersAndLogs = new ArrayList<MatcherAndError>();

  private CustomHttpErrors(List<MatcherAndError> matchersAndLogs) {
    // Deep copy the matchersAndLogs, which allows the builder to be reused.
    for (MatcherAndError m : matchersAndLogs) {
      this.matchersAndLogs.add(m);
    }
  }

  /** @return The a custom error for the failing request and response if one matches, or null. */
  public String getCustomError(HttpRequestWrapper req, HttpResponseWrapper res) {
    for (MatcherAndError m : matchersAndLogs) {
      if (m.getMatcher().matchResponse(req, res)) {
        return m.getCustomError().customError(req, res);
      }
    }
    return null;
  }
}

/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.options;

import com.google.api.client.googleapis.services.AbstractGoogleClient;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * These options configure debug settings for Google API clients created within the Dataflow SDK.
 */
public interface GoogleApiDebugOptions extends PipelineOptions {
  /**
   * This option enables tracing of API calls to Google services used within the Dataflow SDK.
   */
  @Description("This option enables tracing of API calls to Google services used within the "
      + "Dataflow SDK. Values are expected in the format \"ApiName#TraceDestination\" where the "
      + "ApiName represents the request classes canonical name. The TraceDestination is a "
      + "logical trace consumer to whom the trace will be reported. Typically, \"producer\" is "
      + "the right destination to use: this makes API traces available to the team offering the "
      + "API. Note that by enabling this option, the contents of the requests to and from "
      + "Google Cloud services will be made available to Google. For example, by specifying "
      + "\"Dataflow#producer\", all calls to the Dataflow service will be made available to "
      + "Google, specifically to the Google Cloud Dataflow team.")
  GoogleApiTracer[] getGoogleApiTrace();
  void setGoogleApiTrace(GoogleApiTracer... commands);

  /**
   * A {@link GoogleClientRequestInitializer} that adds the trace destination to Google API calls.
   */
  public static class GoogleApiTracer implements GoogleClientRequestInitializer {
    private static final Pattern COMMAND_LINE_PATTERN = Pattern.compile("([^#]*)#(.*)");
    /**
     * Creates a {@link GoogleApiTracer} that sets the trace destination on all
     * calls that match the given client type.
     */
    public static GoogleApiTracer create(AbstractGoogleClient client, String traceDestination) {
      return new GoogleApiTracer(client.getClass().getCanonicalName(), traceDestination);
    }

    /**
     * Creates a {@link GoogleApiTracer} that sets the trace {@code traceDestination} on all
     * calls that match for the given request type.
     */
    public static GoogleApiTracer create(
        AbstractGoogleClientRequest<?> request, String traceDestination) {
      return new GoogleApiTracer(request.getClass().getCanonicalName(), traceDestination);
    }

    /**
     * Creates a {@link GoogleClientRequestInitializer} that adds the trace destination
     * based upon the passed in value.
     * <p>
     * The {@code value} represents a string containing {@code ApiName#TraceDestination}.
     * The {@code ApiName} is used to match against the request class
     * {@link Class#getCanonicalName() canonical name} to determine the requests to which the
     * {@code TraceDestination} should be added.
     * <p>
     * For example, to match:
     * <ul>
     *   <li>all Google API calls: {@code #TraceDestination}
     *   <li>all Dataflow API calls: {@code Dataflow#TraceDestination}
     *   <li>all Dataflow V1B3 API calls: {@code Dataflow.V1b3#TraceDestination}
     *   <li>all Dataflow V1B3 Jobs API calls: {@code Dataflow.V1b3.Projects.Jobs#TraceDestination}
     *   <li>all Dataflow V1B3 Jobs Get calls:
     *       {@code Dataflow.V1b3.Projects.Jobs.Get#TraceDestination}
     *   <li>all Job creation calls in any version: {@code Jobs.Create#TraceDestination}
     * </ul>
     */
    @JsonCreator
    public static GoogleApiTracer create(String value) {
      Matcher matcher = COMMAND_LINE_PATTERN.matcher(value);
      Preconditions.checkArgument(matcher.find() && matcher.groupCount() == 2,
          "Unable to parse '%s', expected format 'ClientRequestName#TraceDestination'", value);
      return new GoogleApiTracer(matcher.group(1), matcher.group(2));
    }

    private final String clientRequestName;
    private final String traceDestination;

    private GoogleApiTracer(String clientRequestName, String traceDestination) {
      this.clientRequestName = clientRequestName;
      this.traceDestination = traceDestination;
    }

    @Override
    public void initialize(AbstractGoogleClientRequest<?> request) throws IOException {
      if (request.getClass().getCanonicalName().contains(clientRequestName)) {
        request.set("$trace", traceDestination);
      }
    }

    @JsonValue
    @Override
    public String toString() {
      return clientRequestName + "#" + traceDestination;
    }
  }
}

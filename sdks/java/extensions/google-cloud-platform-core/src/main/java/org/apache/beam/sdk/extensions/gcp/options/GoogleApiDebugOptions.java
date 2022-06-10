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
package org.apache.beam.sdk.extensions.gcp.options;

import com.google.api.client.googleapis.services.AbstractGoogleClient;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * These options configure debug settings for Google API clients created within the Apache Beam SDK.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface GoogleApiDebugOptions extends PipelineOptions {
  /**
   * This option enables tracing of API calls to Google services used within the Apache Beam SDK.
   * Values are expected in JSON format <code>{"ApiName":"TraceDestination",...}
   * </code> where the {@code ApiName} represents the request classes canonical name. The {@code
   * TraceDestination} is a logical trace consumer to whom the trace will be reported. Typically,
   * "producer" is the right destination to use: this makes API traces available to the team
   * offering the API. Note that by enabling this option, the contents of the requests to and from
   * Google Cloud services will be made available to Google. For example, by specifying <code>
   * {"Dataflow":"producer"}</code>, all calls to the Dataflow service will be made available to
   * Google, specifically to the Google Cloud Dataflow team.
   */
  @Description(
      "This option enables tracing of API calls to Google services used within the Apache "
          + "Beam SDK. Values are expected in JSON format {\"ApiName\":\"TraceDestination\",...} "
          + "where the ApiName represents the request classes canonical name. The TraceDestination is "
          + "a logical trace consumer to whom the trace will be reported. Typically, \"producer\" is "
          + "the right destination to use: this makes API traces available to the team offering the "
          + "API. Note that by enabling this option, the contents of the requests to and from "
          + "Google Cloud services will be made available to Google. For example, by specifying "
          + "{\"Dataflow\":\"producer\"}, all calls to the Dataflow service will be made available to "
          + "Google, specifically to the Google Cloud Dataflow team.")
  GoogleApiTracer getGoogleApiTrace();

  void setGoogleApiTrace(GoogleApiTracer commands);

  /**
   * A {@link GoogleClientRequestInitializer} that adds the trace destination to Google API calls.
   */
  class GoogleApiTracer extends HashMap<String, String> implements GoogleClientRequestInitializer {
    /**
     * Creates a {@link GoogleApiTracer} that sets the trace destination on all calls that match the
     * given client type.
     */
    public GoogleApiTracer addTraceFor(AbstractGoogleClient client, String traceDestination) {
      put(client.getClass().getCanonicalName(), traceDestination);
      return this;
    }

    /**
     * Creates a {@link GoogleApiTracer} that sets the trace {@code traceDestination} on all calls
     * that match for the given request type.
     */
    public GoogleApiTracer addTraceFor(
        AbstractGoogleClientRequest<?> request, String traceDestination) {
      put(request.getClass().getCanonicalName(), traceDestination);
      return this;
    }

    @Override
    public void initialize(AbstractGoogleClientRequest<?> request) throws IOException {
      for (Map.Entry<String, String> entry : this.entrySet()) {
        if (request.getClass().getCanonicalName().contains(entry.getKey())) {
          request.set("$trace", entry.getValue());
        }
      }
    }
  }
}

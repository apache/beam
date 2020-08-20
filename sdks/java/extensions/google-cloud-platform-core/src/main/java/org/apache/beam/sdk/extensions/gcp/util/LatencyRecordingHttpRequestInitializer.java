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

import com.google.api.client.http.HttpExecuteInterceptor;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseInterceptor;
import java.io.IOException;
import org.apache.beam.sdk.util.Histogram;

/** HttpRequestInitializer for recording request to response latency of Http-based API calls. */
public class LatencyRecordingHttpRequestInitializer implements HttpRequestInitializer {
  private final Histogram histogram;

  public LatencyRecordingHttpRequestInitializer(Histogram histogram) {
    this.histogram = histogram;
  }

  private static class LoggingInterceptor
      implements HttpResponseInterceptor, HttpExecuteInterceptor {
    private final Histogram histogram;
    private long startTime;

    public LoggingInterceptor(Histogram histogram) {
      this.histogram = histogram;
    }

    @Override
    public void interceptResponse(HttpResponse response) throws IOException {
      long timeToResponse = System.currentTimeMillis() - startTime;
      histogram.record(timeToResponse);
    }

    @Override
    public void intercept(HttpRequest request) throws IOException {
      startTime = System.currentTimeMillis();
    }
  }

  @Override
  public void initialize(HttpRequest request) throws IOException {
    LoggingInterceptor interceptor = new LoggingInterceptor(histogram);
    request.setInterceptor(interceptor);
    request.setResponseInterceptor(interceptor);
  }
}

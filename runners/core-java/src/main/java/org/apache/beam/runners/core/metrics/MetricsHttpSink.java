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

package org.apache.beam.runners.core.metrics;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/** HTTP Sink to push metrics in a POST HTTP request. */
public class MetricsHttpSink extends MetricsSink<String> {
  private final String url;

  /** @param url the URL of the endpoint */
  public MetricsHttpSink(String url) {
    this.url = url;
  }

  @Override
  protected MetricsSerializer<String> provideSerializer() {
    return new JsonMetricsSerializer();
  }

  @Override
  protected void writeSerializedMetrics(String metrics) throws Exception {
    HttpPost httpPost = new HttpPost(url);
    HttpEntity entity = new StringEntity(metrics);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setEntity(entity);
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      try (CloseableHttpResponse execute = httpClient.execute(httpPost)) {}
    }
  }
}

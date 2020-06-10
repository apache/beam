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
package org.apache.beam.sdk.testutils.publishing;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfluxDBPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(InfluxDBPublisher.class);

  private InfluxDBPublisher() {}

  public static void publishNexmarkResults(
      final Collection<Map<String, Object>> results, final InfluxDBSettings settings) {
    publishWithCheck(settings, () -> publishNexmark(results, settings));
  }

  public static void publishWithSettings(
      final Collection<NamedTestResult> results, final InfluxDBSettings settings) {
    publishWithCheck(settings, () -> publishCommon(results, settings));
  }

  private static void publishWithCheck(
      final InfluxDBSettings settings, final PublishFunction publishFunction) {
    requireNonNull(settings, "InfluxDB settings must not be null");
    if (isNoneBlank(settings.measurement, settings.database)) {
      try {
        publishFunction.publish();
      } catch (Exception exception) {
        LOG.warn("Unable to publish metrics due to error: {}", exception.getMessage(), exception);
      }
    } else {
      LOG.warn("Missing property -- measurement/database. Metrics won't be published.");
    }
  }

  private static void publishNexmark(
      final Collection<Map<String, Object>> results, final InfluxDBSettings settings)
      throws Exception {

    final HttpClientBuilder builder = provideHttpBuilder(settings);
    final HttpPost postRequest = providePOSTRequest(settings);
    final StringBuilder metricBuilder = new StringBuilder();
    results.forEach(
        map ->
            metricBuilder
                .append(map.get("measurement"))
                .append(",")
                .append(getKV(map, "runner"))
                .append(" ")
                .append(getKV(map, "runtimeMs"))
                .append(",")
                .append(getKV(map, "numResults"))
                .append(" ")
                .append(map.get("timestamp"))
                .append('\n'));

    postRequest.setEntity(
        new GzipCompressingEntity(new ByteArrayEntity(metricBuilder.toString().getBytes(UTF_8))));

    executeWithVerification(postRequest, builder);
  }

  private static String getKV(final Map<String, Object> map, final String key) {
    return key + "=" + map.get(key);
  }

  private static void publishCommon(
      final Collection<NamedTestResult> results, final InfluxDBSettings settings) throws Exception {

    final HttpClientBuilder builder = provideHttpBuilder(settings);
    final HttpPost postRequest = providePOSTRequest(settings);
    final StringBuilder metricBuilder = new StringBuilder();
    results.stream()
        .map(NamedTestResult::toMap)
        .forEach(
            map ->
                metricBuilder
                    .append(settings.measurement)
                    .append(",")
                    .append(getKV(map, "test_id"))
                    .append(",")
                    .append(getKV(map, "metric"))
                    .append(" ")
                    .append(getKV(map, "value"))
                    .append('\n'));

    postRequest.setEntity(new ByteArrayEntity(metricBuilder.toString().getBytes(UTF_8)));

    executeWithVerification(postRequest, builder);
  }

  private static HttpClientBuilder provideHttpBuilder(final InfluxDBSettings settings) {
    final HttpClientBuilder builder = HttpClientBuilder.create();

    if (isNoneBlank(settings.userName, settings.userPassword)) {
      final CredentialsProvider provider = new BasicCredentialsProvider();
      provider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(settings.userName, settings.userPassword));
      builder.setDefaultCredentialsProvider(provider);
    }

    return builder;
  }

  private static HttpPost providePOSTRequest(final InfluxDBSettings settings) {
    final String retentionPolicy =
        "rp" + (isBlank(settings.retentionPolicy) ? "" : "=" + settings.retentionPolicy);
    return new HttpPost(
        settings.host + "/write?db=" + settings.database + "&" + retentionPolicy + "&precision=s");
  }

  private static void executeWithVerification(
      final HttpPost postRequest, final HttpClientBuilder builder) throws IOException {
    try (final CloseableHttpResponse response = builder.build().execute(postRequest)) {
      is2xx(response.getStatusLine().getStatusCode());
    }
  }

  private static void is2xx(final int code) throws IOException {
    if (code < 200 || code >= 300) {
      throw new IOException("Response code: " + code);
    }
  }

  @FunctionalInterface
  private interface PublishFunction {
    void publish() throws Exception;
  }
}

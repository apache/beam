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

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Collection;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class InfluxDBPublisher {
  private static final Logger LOGGER = LoggerFactory.getLogger(InfluxDBPublisher.class);

  private InfluxDBPublisher() {}

  public static void publishWithSettings(
      final Collection<NamedTestResult> results, final InfluxDBSettings settings) {
    requireNonNull(settings, "InfluxDB settings must not be null");
    try {
      publish(results, settings);
    } catch (final Exception exception) {
      LOGGER.warn("Unable to publish metrics due to error: {}", exception.getMessage());
    }
  }

  private static void publish(
      final Collection<NamedTestResult> results, final InfluxDBSettings settings) throws Exception {
    final HttpURLConnection connection =
        (HttpURLConnection)
            new URL(settings.host + "write?db=" + settings.database).openConnection();
    connection.setDoOutput(true);
    connection.setRequestMethod("POST");
    connection.setRequestProperty(
        "Authorization", getTokenAsString(settings.userName, settings.userPassword));

    try (final AutoCloseable ignored = connection::disconnect;
        final DataOutputStream stream = new DataOutputStream(connection.getOutputStream())) {

      final StringBuilder builder = new StringBuilder();
      results.stream()
          .map(NamedTestResult::toMap)
          .forEach(
              map ->
                  builder
                      .append(settings.measurement)
                      .append(",")
                      .append("test_id")
                      .append("=")
                      .append(map.get("test_id"))
                      .append(",")
                      .append("metric")
                      .append("=")
                      .append(map.get("metric"))
                      .append(" ")
                      .append("value")
                      .append("=")
                      .append(map.get("value"))
                      .append('\n'));

      stream.writeBytes(builder.toString());
      stream.flush();

      is2xx(connection.getResponseCode());
    }
  }

  private static String getTokenAsString(final String user, final String password) {
    final String auth = user + ":" + password;
    final byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(UTF_8));
    return "Basic " + new String(encodedAuth, UTF_8);
  }

  private static void is2xx(final int code) throws IOException {
    if (code < 200 || code >= 300) {
      throw new IOException("Response code: " + code);
    }
  }
}

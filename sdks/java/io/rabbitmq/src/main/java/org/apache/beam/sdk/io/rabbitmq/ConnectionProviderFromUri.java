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
package org.apache.beam.sdk.io.rabbitmq;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

/**
 * Modeled after {@link
 * org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceProviderFromDataSourceConfiguration}, providing a
 * means of obtaining a {@link ConnectionHandler} based on a static cache and a URI to promote
 * re-use of existing connections rather than establishing a new connection for each RabbitMq
 * interaction.
 *
 * <p>The cache is oriented around the full URI, which will not be parsed or validated until {@link
 * #apply(Void)} is called.
 *
 * <p>Note that this can be shut down but it's unclear what aspect of the Beam runtime should be
 * responsible for ensure this happens.
 */
public class ConnectionProviderFromUri
    implements SerializableFunction<Void, ConnectionHandler>, HasDisplayData, Closeable {
  private static final ConcurrentHashMap<String, ConnectionHandler> instances =
      new ConcurrentHashMap<>();

  private final String uri;
  private final String displayableUri;

  public ConnectionProviderFromUri(String uri) {
    this.uri = uri;
    String displayable = uri;
    try {
      displayable = stripPasswordFromUri(uri);
    } catch (URISyntaxException e) {
      /* ignored */
    }
    this.displayableUri = displayable;
  }

  @VisibleForTesting
  static String stripPasswordFromUri(String uri) throws URISyntaxException {
    // using new URI vs URI.create as this form throws URISyntaxException
    // while the other throws IllegalArgumentException
    URI parsed = new URI(uri);
    String userInfo =
        Optional.ofNullable(parsed.getUserInfo())
            .map(x -> x.split(":", 2)[0]) // user:pass
            .map(username -> username + ":")
            .orElse("");
    URI result =
        new URI(
            parsed.getScheme(),
            userInfo,
            parsed.getHost(),
            parsed.getPort(),
            parsed.getPath(),
            null,
            null);
    return result.toString();
  }

  @Override
  public ConnectionHandler apply(Void input) {
    return instances.computeIfAbsent(uri, ConnectionHandler::new);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("rabbitUri", displayableUri));
  }

  public void shutdownAll() {
    instances.forEach(
        (uri, connectionHandler) -> {
          try {
            connectionHandler.close();
          } catch (IOException e) {
            /* ignore */
          }
        });
  }

  @Override
  public void close() {
    shutdownAll();
  }
}

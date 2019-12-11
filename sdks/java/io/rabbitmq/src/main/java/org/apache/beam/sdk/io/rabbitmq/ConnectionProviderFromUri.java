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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;

/** Modeled after JdbcIO.DataSourceProviderFromDataSourceConfiguration. */
public class ConnectionProviderFromUri
    implements SerializableFunction<Void, ConnectionHandler>, HasDisplayData {
  private static final ConcurrentHashMap<String, ConnectionHandler> instances =
      new ConcurrentHashMap<>();

  private final String uri;
  private final String displayableUri;

  public ConnectionProviderFromUri(String uri) {
    this.uri = uri;
    String displayable = uri;
    try {
      URI parsed = URI.create(uri);
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
      displayable = result.toString();
    } catch (URISyntaxException e) {
      /* ignored */
    }
    this.displayableUri = displayable;
  }

  @Override
  public ConnectionHandler apply(Void input) {
    return instances.computeIfAbsent(uri, ConnectionHandler::new);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    builder.add(DisplayData.item("rabbitUri", displayableUri));
  }
}

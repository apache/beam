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
package org.apache.beam.sdk.io.solace.broker;

import com.google.api.client.http.HttpResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import org.checkerframework.checker.nullness.qual.Nullable;

public class BrokerResponse {
  final int code;
  final String message;
  @Nullable String content;

  public BrokerResponse(int responseCode, String message, @Nullable InputStream content) {
    this.code = responseCode;
    this.message = message;
    if (content != null) {
      // Use try-with-resources so the underlying InputStream is always closed once the
      // response body has been read; otherwise the HTTP connection stream leaks.
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(content, StandardCharsets.UTF_8))) {
        this.content = reader.lines().collect(Collectors.joining("\n"));
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to read broker response content", e);
      }
    }
  }

  public static BrokerResponse fromHttpResponse(HttpResponse response) throws IOException {
    return new BrokerResponse(
        response.getStatusCode(), response.getStatusMessage(), response.getContent());
  }

  @Override
  public String toString() {
    return "BrokerResponse{"
        + "code="
        + code
        + ", message='"
        + message
        + '\''
        + ", content="
        + content
        + '}';
  }
}

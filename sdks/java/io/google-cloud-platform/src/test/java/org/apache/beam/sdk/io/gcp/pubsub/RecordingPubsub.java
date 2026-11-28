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
package org.apache.beam.sdk.io.gcp.pubsub;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.util.Transport;

/**
 * A Pub/Sub JSON API client whose HTTP transport records the body of every publish request that it
 * receives, so that tests can compare what a client says it sends with what it really sends.
 */
final class RecordingPubsub {
  private static final JsonFactory JSON_FACTORY = Transport.getJsonFactory();

  /** The size in bytes of the body of each request, after the transport decoded its gzip form. */
  final List<Integer> bodyBytes = new ArrayList<>();

  /** The number of messages in each request. */
  final List<Integer> messageCounts = new ArrayList<>();

  Pubsub newPubsub() {
    MockLowLevelHttpRequest request =
        new MockLowLevelHttpRequest() {
          @Override
          public LowLevelHttpResponse execute() throws IOException {
            // Decodes the gzip encoding that the Google API client applies to the body.
            String body = getContentAsString();
            PublishRequest published = JSON_FACTORY.fromString(body, PublishRequest.class);
            int count = published.getMessages().size();
            bodyBytes.add(body.getBytes(StandardCharsets.UTF_8).length);
            messageCounts.add(count);
            PublishResponse response =
                new PublishResponse().setMessageIds(Collections.nCopies(count, "id"));
            return new MockLowLevelHttpResponse()
                .setContentType(Json.MEDIA_TYPE)
                .setContent(JSON_FACTORY.toString(response));
          }
        };
    HttpTransport transport =
        new MockHttpTransport.Builder().setLowLevelHttpRequest(request).build();
    return new Pubsub.Builder(transport, JSON_FACTORY, null).setApplicationName("test").build();
  }
}

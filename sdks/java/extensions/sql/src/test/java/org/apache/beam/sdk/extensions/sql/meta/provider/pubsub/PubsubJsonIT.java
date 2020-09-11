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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.testing.JsonMatcher.jsonBytesLike;
import static org.hamcrest.Matchers.hasProperty;

import java.io.IOException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.ToJson;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matcher;
import org.joda.time.Instant;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration tests for querying Pubsub JSON messages with SQL. */
@RunWith(JUnit4.class)
public class PubsubJsonIT extends PubsubTableProviderIT {

  // Pubsub table provider should default to json
  @Override
  protected String getPayloadFormat() {
    return null;
  }

  @Override
  protected PubsubMessage messageIdName(Instant timestamp, int id, String name) {
    String jsonString = "{ \"id\" : " + id + ", \"name\" : \"" + name + "\" }";
    return message(timestamp, jsonString);
  }

  @Override
  protected PCollection<String> applyRowsToStrings(PCollection<Row> rows) {
    return rows.apply(ToJson.of());
  }

  @Override
  protected Matcher<PubsubMessage> matcherNames(String name) {
    return hasProperty("payload", toJsonByteLike(String.format("{\"name\":\"%s\"}", name)));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeightKnowsJS(
      String name, int height, boolean knowsJS) {
    String jsonString =
        String.format(
            "{\"name\":\"%s\", \"height\": %s, \"knowsJavascript\": %s}", name, height, knowsJS);

    return hasProperty("payload", toJsonByteLike(jsonString));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeight(String name, int height) {
    String jsonString = String.format("{\"name\":\"%s\", \"height\": %s}", name, height);
    return hasProperty("payload", toJsonByteLike(jsonString));
  }

  private PubsubMessage message(Instant timestamp, String jsonPayload) {
    return message(timestamp, jsonPayload.getBytes(UTF_8));
  }

  private Matcher<byte[]> toJsonByteLike(String jsonString) {
    try {
      return jsonBytesLike(jsonString);
    } catch (IOException e) {
      throw new RuntimeException(" jsonBytesLike thrown error");
    }
  }
}

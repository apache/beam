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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.hamcrest.Matcher;
import org.joda.time.Instant;

public class PubsubProtoIT extends PubsubTableProviderIT {
  @Override
  protected String getPayloadFormat() {
    return "proto";
  }

  @Override
  protected PCollection<String> applyRowsToStrings(PCollection<Row> rows) {
    return rows.apply(
        MapElements.into(TypeDescriptors.strings())
            .via(row -> new String(rowToBytes(row), StandardCharsets.US_ASCII)));
  }

  @Override
  protected PubsubMessage messageIdName(Instant timestamp, int id, String name) {
    Row row = row(PAYLOAD_SCHEMA, id, name);
    return message(timestamp, rowToBytes(row));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNames(String name) {
    Schema schema = Schema.builder().addStringField("name").build();
    Row row = row(schema, name);
    return hasProperty("payload", equalTo(rowToBytes(row)));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeight(String name, int height) {
    Row row = row(NAME_HEIGHT_SCHEMA, name, height);
    return hasProperty("payload", equalTo(rowToBytes(row)));
  }

  @Override
  protected Matcher<PubsubMessage> matcherNameHeightKnowsJS(
      String name, int height, boolean knowsJS) {
    Row row = row(NAME_HEIGHT_KNOWS_JS_SCHEMA, name, height, knowsJS);
    return hasProperty("payload", equalTo(rowToBytes(row)));
  }

  private byte[] rowToBytes(Row row) {
    try {
      RowCoder coder = RowCoder.of(row.getSchema());
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      coder.encode(row, outputStream);
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Could not convert row to bytes", e);
    }
  }
}

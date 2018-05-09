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

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link PubsubMessageToRow}.
 */
public class PubsubMessageToRowTest implements Serializable {

  @Rule
  public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertsMessages() {
    Schema payloadSchema =
        RowSqlTypes
            .builder()
            .withIntegerField("id")
            .withVarcharField("name")
            .build();

    Schema messageSchema =
        RowSqlTypes
            .builder()
            .withTimestampField("event_timestamp")
            .withMapField("attributes", VARCHAR, VARCHAR)
            .withRowField("payload", payloadSchema)
            .build();

    PCollection<Row> rows = pipeline
        .apply("create",
               Create.timestamped(
                   message(1, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                   message(2, map("bttr", "vbl"), "{ \"name\" : \"baz\", \"id\" : 5 }"),
                   message(3, map("cttr", "vcl"), "{ \"id\" : 7, \"name\" : \"bar\" }"),
                   message(4, map("dttr", "vdl"), "{ \"name\" : \"qaz\", \"id\" : 8 }")))
        .apply("convert", PubsubMessageToRow.forSchema(messageSchema));

    PAssert
        .that(rows)
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
               .addValues(ts(1), map("attr", "val"), row(payloadSchema, 3, "foo"))
               .build(),
            Row.withSchema(messageSchema)
               .addValues(ts(2), map("bttr", "vbl"), row(payloadSchema, 5, "baz"))
               .build(),
            Row.withSchema(messageSchema)
               .addValues(ts(3), map("cttr", "vcl"), row(payloadSchema, 7, "bar"))
               .build(),
            Row.withSchema(messageSchema)
               .addValues(ts(4), map("dttr", "vdl"), row(payloadSchema, 8, "qaz"))
               .build()
        );

    pipeline.run();
  }

  private Row row(Schema schema, int id, String name) {
    return Row.withSchema(schema).addValues(id, name).build();
  }

  private Map<String, String> map(String attr, String val) {
    return ImmutableMap.of(attr, val);
  }

  private TimestampedValue<PubsubMessage> message(
      int timestamp,
      Map<String, String> attributes,
      String payload) {

    return TimestampedValue.of(
        new PubsubMessage(payload.getBytes(StandardCharsets.UTF_8), attributes),
        ts(timestamp));
  }

  private Instant ts(long timestamp) {
    return new DateTime(timestamp).toInstant();
  }
}

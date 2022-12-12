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
package org.apache.beam.io.debezium;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SourceRecordJsonTest implements Serializable {
  @Test
  public void testSourceRecordJson() {
    SourceRecord record = buildSourceRecord();
    SourceRecordJson json = new SourceRecordJson(record);

    String jsonString = json.toJson();

    String expectedJson =
        "{\"metadata\":"
            + "{\"connector\":\"test-connector\",\"version\":\"version-connector\","
            + "\"name\":\"test-connector-sql\","
            + "\"database\":\"test-db\",\"schema\":\"test-schema\",\"table\":\"test-table\"},"
            + "\"before\":{\"fields\":{\"country\":null,\"distance\":123.423,\"birthYear\":null,"
            + "\"name\":\"before-name\","
            + "\"temperature\":104.4,\"childrenAndAge\":null,\"age\":16}},"
            + "\"after\":{\"fields\":{\"country\":null,\"distance\":123.423,\"birthYear\":null,"
            + "\"name\":\"after-name\","
            + "\"temperature\":104.4,\"childrenAndAge\":null,\"age\":16}}}";

    assertEquals(expectedJson, jsonString);
  }

  @Test
  public void testSourceRecordJsonWhenSourceRecordIsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SourceRecordJson(null));
  }

  private static Schema buildSourceSchema() {
    return SchemaBuilder.struct()
        .field("connector", Schema.STRING_SCHEMA)
        .field("version", Schema.STRING_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("db", Schema.STRING_SCHEMA)
        .field("schema", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA)
        .build();
  }

  public static Schema buildTableSchema() {
    return SchemaBuilder.struct()
        .field("name", Schema.STRING_SCHEMA)
        .field("age", SchemaBuilder.int8().doc("age of the person").build())
        .field("temperature", Schema.FLOAT32_SCHEMA)
        .field("distance", Schema.FLOAT64_SCHEMA)
        .field("birthYear", Schema.OPTIONAL_INT64_SCHEMA)
        .field(
            "country",
            SchemaBuilder.struct()
                .optional()
                .field("name", Schema.STRING_SCHEMA)
                .field("population", Schema.OPTIONAL_INT64_SCHEMA)
                .field("latitude", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional())
                .field("longitude", SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional())
                .build())
        .field(
            "childrenAndAge",
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional())
        .build();
  }

  static SourceRecord buildSourceRecord() {
    final Schema sourceSchema = buildSourceSchema();
    final Schema beforeSchema = buildTableSchema();
    final Schema afterSchema = buildTableSchema();

    final Schema schema =
        SchemaBuilder.struct()
            .name("test")
            .field("source", sourceSchema)
            .field("before", beforeSchema)
            .field("after", afterSchema)
            .build();

    final Struct source = new Struct(sourceSchema);
    final Struct before = new Struct(beforeSchema);
    final Struct after = new Struct(afterSchema);
    final Struct value = new Struct(schema);

    source.put("connector", "test-connector");
    source.put("version", "version-connector");
    source.put("name", "test-connector-sql");
    source.put("db", "test-db");
    source.put("schema", "test-schema");
    source.put("table", "test-table");

    before
        .put("name", "before-name")
        .put("age", (byte) 16)
        .put("temperature", (float) 104.4)
        .put("distance", 123.423);
    after
        .put("name", "after-name")
        .put("age", (byte) 16)
        .put("temperature", (float) 104.4)
        .put("distance", 123.423);

    value.put("source", source);
    value.put("before", before);
    value.put("after", after);

    return new SourceRecord(null, null, null, schema, value);
  }
}

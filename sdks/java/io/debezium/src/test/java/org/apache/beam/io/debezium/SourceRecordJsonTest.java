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
    SourceRecord record = this.buildSourceRecord();
    SourceRecordJson json = new SourceRecordJson(record);

    String jsonString = json.toJson();

    String expectedJson =
        "{\"metadata\":"
            + "{\"connector\":\"test-connector\","
            + "\"version\":\"version-connector\","
            + "\"name\":\"test-connector-sql\","
            + "\"database\":\"test-db\","
            + "\"schema\":\"test-schema\","
            + "\"table\":\"test-table\"},"
            + "\"before\":{\"fields\":{\"column1\":\"before-name\"}},"
            + "\"after\":{\"fields\":{\"column1\":\"after-name\"}}}";

    assertEquals(expectedJson, jsonString);
  }

  @Test
  public void testSourceRecordJsonWhenSourceRecordIsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SourceRecordJson(null));
  }

  private Schema buildSourceSchema() {
    return SchemaBuilder.struct()
        .field("connector", Schema.STRING_SCHEMA)
        .field("version", Schema.STRING_SCHEMA)
        .field("name", Schema.STRING_SCHEMA)
        .field("db", Schema.STRING_SCHEMA)
        .field("schema", Schema.STRING_SCHEMA)
        .field("table", Schema.STRING_SCHEMA)
        .build();
  }

  private Schema buildBeforeSchema() {
    return SchemaBuilder.struct().field("column1", Schema.STRING_SCHEMA).build();
  }

  private Schema buildAfterSchema() {
    return SchemaBuilder.struct().field("column1", Schema.STRING_SCHEMA).build();
  }

  private SourceRecord buildSourceRecord() {
    final Schema sourceSchema = this.buildSourceSchema();
    final Schema beforeSchema = this.buildBeforeSchema();
    final Schema afterSchema = this.buildAfterSchema();

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

    before.put("column1", "before-name");
    after.put("column1", "after-name");

    value.put("source", source);
    value.put("before", before);
    value.put("after", after);

    return new SourceRecord(null, null, null, schema, value);
  }
}

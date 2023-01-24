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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.schemas.Schema;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KafkaConnectSchemaTest {

  @Test
  public void testSimpleSourceRecordSchemaConversion() {
    org.apache.kafka.connect.data.Schema valueSchema = SourceRecordJsonTest.buildTableSchema();

    Schema beamValueSchema = KafkaConnectUtils.beamSchemaFromKafkaConnectSchema(valueSchema);
    assertThat(
        beamValueSchema.getFields(),
        Matchers.containsInAnyOrder(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("age", Schema.FieldType.BYTE).withDescription("age of the person"),
            Schema.Field.of("temperature", Schema.FieldType.FLOAT),
            Schema.Field.of("distance", Schema.FieldType.DOUBLE),
            Schema.Field.nullable("birthYear", Schema.FieldType.INT64),
            Schema.Field.nullable(
                "country",
                Schema.FieldType.row(
                    Schema.of(
                        Schema.Field.of("name", Schema.FieldType.STRING),
                        Schema.Field.nullable("population", Schema.FieldType.INT64),
                        Schema.Field.nullable(
                            "latitude", Schema.FieldType.array(Schema.FieldType.FLOAT)),
                        Schema.Field.nullable(
                            "longitude", Schema.FieldType.array(Schema.FieldType.FLOAT))))),
            Schema.Field.nullable(
                "childrenAndAge",
                Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32))));
  }

  @Test
  public void testTimestampRequired() {
    org.apache.kafka.connect.source.SourceRecord record = SourceRecordJsonTest.buildSourceRecord();

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> KafkaConnectUtils.debeziumRecordInstant(record));
    assertThat(e.getMessage(), Matchers.containsString("Should be STRUCT with ts_ms field"));
  }
}

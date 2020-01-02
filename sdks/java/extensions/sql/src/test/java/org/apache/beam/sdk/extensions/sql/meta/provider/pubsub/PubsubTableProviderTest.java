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

import static junit.framework.TestCase.assertNotNull;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link PubsubTableProvider}. */
public class PubsubTableProviderTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String FORMAT = "format";
  private static final String AVRO_FORMAT = "avro";
  private static final String JSON_FORMAT = "json";

  @Test
  public void testTableTypePubsub() {
    PubsubTableProvider provider = new PubsubTableProvider();
    assertEquals("pubsub", provider.getTableType());
  }

  @Test
  public void testCreatesJsonTable() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addMapField("attributes", VARCHAR, VARCHAR)
            .addRowField("payload", Schema.builder().build())
            .build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");
    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(tableDefinition);

    assertNotNull(pubsubTable);
    assertEquals(messageSchema, pubsubTable.getSchema());
  }

  @Test
  public void testCreatesNestedSchemaAvroTable() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addMapField("attributes", VARCHAR, VARCHAR)
            .addRowField("payload", Schema.builder().build())
            .build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");
    properties.put(FORMAT, AVRO_FORMAT);

    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("doesn't support nested schema for Avro format");
    provider.buildBeamSqlTable(tableDefinition);
  }

  @Test
  public void testThrowsIfTimestampFieldNotProvided() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema =
        Schema.builder()
            .addMapField("attributes", VARCHAR, VARCHAR)
            .addRowField("payload", Schema.builder().build())
            .build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");

    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("'event_timestamp'");
    provider.buildBeamSqlTable(tableDefinition);
  }

  @Test
  public void testCreatesTableWithJustTimestamp() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema = Schema.builder().addDateTimeField("event_timestamp").build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");
    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(tableDefinition);

    assertNotNull(pubsubTable);
    assertEquals(messageSchema, pubsubTable.getSchema());
  }

  @Test
  public void testCreatesFlatJsonTable() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema =
        Schema.builder().addDateTimeField("event_timestamp").addStringField("someField").build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");

    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(tableDefinition);

    assertNotNull(pubsubTable);
    assertEquals(messageSchema, pubsubTable.getSchema());
  }

  @Test
  public void testCreatesFlatAvroTable() {
    PubsubTableProvider provider = new PubsubTableProvider();
    Schema messageSchema =
        Schema.builder().addDateTimeField("event_timestamp").addStringField("someField").build();

    JSONObject properties = JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }");
    properties.put(FORMAT, AVRO_FORMAT);

    Table tableDefinition = tableDefinition().schema(messageSchema).properties(properties).build();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(tableDefinition);

    assertNotNull(pubsubTable);
    assertEquals(messageSchema, pubsubTable.getSchema());
  }

  private static Table.Builder tableDefinition() {
    return Table.builder()
        .name("FakeTable")
        .comment("fake table")
        .location("projects/project/topics/topic")
        .schema(Schema.builder().build())
        .type("pubsub");
  }
}

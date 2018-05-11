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
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link PubsubJsonTableProvider}.
 */
public class PubsubJsonTableProviderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTableTypePubsub() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    assertEquals("pubsub", provider.getTableType());
  }

  @Test
  public void testCreatesTable() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    Schema messageSchema = RowSqlTypes
        .builder()
        .withTimestampField("event_timestamp")
        .withMapField("attributes", VARCHAR, VARCHAR)
        .withRowField("payload", Schema.builder().build())
        .build();

    Table tableDefinition = tableDefinition().schema(messageSchema).build();

    BeamSqlTable pubsubTable = provider.buildBeamSqlTable(tableDefinition);

    assertNotNull(pubsubTable);
    assertEquals(messageSchema, pubsubTable.getSchema());
  }

  @Test
  public void testThrowsIfTimestampFieldNotProvided() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    Schema messageSchema = RowSqlTypes
        .builder()
        .withMapField("attributes", VARCHAR, VARCHAR)
        .withRowField("payload", Schema.builder().build())
        .build();

    Table tableDefinition = tableDefinition().schema(messageSchema).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("'event_timestamp'");
    provider.buildBeamSqlTable(tableDefinition);
  }

  @Test
  public void testThrowsIfAttributesFieldNotProvided() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    Schema messageSchema = RowSqlTypes
        .builder()
        .withTimestampField("event_timestamp")
        .withRowField("payload", Schema.builder().build())
        .build();

    Table tableDefinition = tableDefinition().schema(messageSchema).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("'attributes'");
    provider.buildBeamSqlTable(tableDefinition);
  }

  @Test
  public void testThrowsIfPayloadFieldNotProvided() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    Schema messageSchema = RowSqlTypes
        .builder()
        .withTimestampField("event_timestamp")
        .withMapField("attributes", VARCHAR, VARCHAR)
        .build();

    Table tableDefinition = tableDefinition().schema(messageSchema).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("'payload'");
    provider.buildBeamSqlTable(tableDefinition);
  }

  @Test
  public void testThrowsIfExtraFieldsExist() {
    PubsubJsonTableProvider provider = new PubsubJsonTableProvider();
    Schema messageSchema = RowSqlTypes
        .builder()
        .withTimestampField("event_timestamp")
        .withMapField("attributes", VARCHAR, VARCHAR)
        .withVarcharField("someField")
        .withRowField("payload", Schema.builder().build())
        .build();

    Table tableDefinition = tableDefinition().schema(messageSchema).build();

    thrown.expectMessage("Unsupported");
    thrown.expectMessage("'event_timestamp'");
    provider.buildBeamSqlTable(tableDefinition);
  }

  private static Table.Builder tableDefinition() {
    return
        Table
            .builder()
            .name("FakeTable")
            .comment("fake table")
            .location("projects/project/topics/topic")
            .schema(Schema.builder().build())
            .type("pubsub")
            .properties(JSON.parseObject("{ \"timestampAttributeKey\" : \"ts_field\" }"));
  }
}

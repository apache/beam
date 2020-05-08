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
package org.apache.beam.sdk.extensions.sql.meta.provider.datastore;

import static org.apache.beam.sdk.extensions.sql.meta.provider.datastore.DataStoreV1Table.DEFAULT_KEY_FIELD;
import static org.apache.beam.sdk.extensions.sql.meta.provider.datastore.DataStoreV1Table.KEY_FIELD_PROPERTY;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.alibaba.fastjson.JSON;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataStoreTableProviderTest {
  private DataStoreV1TableProvider provider = new DataStoreV1TableProvider();

  @Test
  public void testGetTableType() {
    assertEquals("datastoreV1", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() {
    final String location = "projectId/batch_kind";
    Table table = fakeTable("TEST", location);
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof DataStoreV1Table);

    DataStoreV1Table datastoreTable = (DataStoreV1Table) sqlTable;
    assertEquals("projectId", datastoreTable.projectId);
    assertEquals("batch_kind", datastoreTable.kind);
    assertEquals(DEFAULT_KEY_FIELD, datastoreTable.keyField);
  }

  @Test
  public void testTableProperty() {
    final String location = "projectId/batch_kind";
    Table table =
        fakeTableWithProperties("TEST", location, "{ " + KEY_FIELD_PROPERTY + ": \"field_name\" }");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof DataStoreV1Table);

    DataStoreV1Table datastoreTable = (DataStoreV1Table) sqlTable;
    assertEquals("projectId", datastoreTable.projectId);
    assertEquals("batch_kind", datastoreTable.kind);
    assertEquals("field_name", datastoreTable.keyField);
  }

  @Test
  public void testTableProperty_nullValue_throwsException() {
    final String location = "projectId/batch_kind";
    Table table = fakeTableWithProperties("TEST", location, "{ " + KEY_FIELD_PROPERTY + ": \"\" }");
    assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
  }

  private static Table fakeTable(String name, String location) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(location)
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("datastoreV1")
        .build();
  }

  private static Table fakeTableWithProperties(String name, String location, String properties) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location(location)
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
        .type("datastoreV1")
        .properties(JSON.parseObject(properties))
        .build();
  }
}

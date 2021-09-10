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
package org.apache.beam.sdk.extensions.sql.meta.provider.mongodb;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.calcite.v1_26_0.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MongoDbTableProviderTest {
  private MongoDbTableProvider provider = new MongoDbTableProvider();

  @Test
  public void testGetTableType() {
    assertEquals("mongodb", provider.getTableType());
  }

  @Test
  public void testBuildBeamSqlTable() {
    Table table = fakeTable("TEST", "mongodb://localhost:27017/database/collection");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof MongoDbTable);

    MongoDbTable mongoTable = (MongoDbTable) sqlTable;
    assertEquals("mongodb://localhost:27017", mongoTable.dbUri);
    assertEquals("database", mongoTable.dbName);
    assertEquals("collection", mongoTable.dbCollection);
  }

  @Test
  public void testBuildBeamSqlTable_withUsernameOnly() {
    Table table = fakeTable("TEST", "mongodb://username@localhost:27017/database/collection");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof MongoDbTable);

    MongoDbTable mongoTable = (MongoDbTable) sqlTable;
    assertEquals("mongodb://username@localhost:27017", mongoTable.dbUri);
    assertEquals("database", mongoTable.dbName);
    assertEquals("collection", mongoTable.dbCollection);
  }

  @Test
  public void testBuildBeamSqlTable_withUsernameAndPassword() {
    Table table =
        fakeTable("TEST", "mongodb://username:pasword@localhost:27017/database/collection");
    BeamSqlTable sqlTable = provider.buildBeamSqlTable(table);

    assertNotNull(sqlTable);
    assertTrue(sqlTable instanceof MongoDbTable);

    MongoDbTable mongoTable = (MongoDbTable) sqlTable;
    assertEquals("mongodb://username:pasword@localhost:27017", mongoTable.dbUri);
    assertEquals("database", mongoTable.dbName);
    assertEquals("collection", mongoTable.dbCollection);
  }

  @Test
  public void testBuildBeamSqlTable_withBadLocation_throwsException() {
    ImmutableList<String> badLocations =
        ImmutableList.of(
            "mongodb://localhost:27017/database/",
            "mongodb://localhost:27017/database",
            "localhost:27017/database/collection",
            "mongodb://:27017/database/collection",
            "mongodb://localhost:27017//collection",
            "mongodb://localhost/database/collection",
            "mongodb://localhost:/database/collection");

    for (String badLocation : badLocations) {
      Table table = fakeTable("TEST", badLocation);
      assertThrows(IllegalArgumentException.class, () -> provider.buildBeamSqlTable(table));
    }
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
        .type("mongodb")
        .build();
  }
}

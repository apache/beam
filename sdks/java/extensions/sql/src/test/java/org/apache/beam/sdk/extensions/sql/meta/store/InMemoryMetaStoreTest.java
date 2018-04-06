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
package org.apache.beam.sdk.extensions.sql.meta.store;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.RowSqlTypes;
import org.apache.beam.sdk.extensions.sql.meta.Column;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

/**
 * UnitTest for {@link InMemoryMetaStore}.
 */
public class InMemoryMetaStoreTest {
  private InMemoryMetaStore store;

  @Before
  public void setUp() {
    store = new InMemoryMetaStore();
    store.registerProvider(new TextTableProvider());
  }

  @Test
  public void testCreateTable() throws Exception {
    Table table = mockTable("person");
    store.createTable(table);
    Table actualTable = store.getTable("person");
    assertEquals(table, actualTable);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTable_invalidTableType() throws Exception {
    Table table = mockTable("person", "invalid");

    store.createTable(table);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateTable_duplicatedName() throws Exception {
    Table table = mockTable("person");
    store.createTable(table);
    store.createTable(table);
  }

  @Test
  public void testGetTable_nullName() throws Exception {
    Table table = store.getTable(null);
    assertNull(table);
  }

  @Test public void testListTables() throws Exception {
    store.createTable(mockTable("hello"));
    store.createTable(mockTable("world"));

    assertThat(store.listTables(),
        Matchers.containsInAnyOrder(mockTable("hello"), mockTable("world")));
  }

  @Test public void testBuildBeamSqlTable() throws Exception {
    store.createTable(mockTable("hello"));
    BeamSqlTable actualSqlTable = store.buildBeamSqlTable("hello");
    assertNotNull(actualSqlTable);
    assertEquals(
        RowSqlTypes.builder().withIntegerField("id").withVarcharField("name").build(),
        actualSqlTable.getSchema()
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBuildBeamSqlTable_tableNotExist() throws Exception {
    store.buildBeamSqlTable("world");
  }

  @Test
  public void testRegisterProvider() throws Exception {
    store.registerProvider(new MockTableProvider("mock", "hello", "world"));
    assertNotNull(store.getProviders());
    assertEquals(2, store.getProviders().size());
    assertEquals("text", store.getProviders().get("text").getTableType());
    assertEquals("mock", store.getProviders().get("mock").getTableType());

    assertEquals(2, store.listTables().size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterProvider_duplicatedTableType() throws Exception {
    store.registerProvider(new MockTableProvider("mock"));
    store.registerProvider(new MockTableProvider("mock"));
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterProvider_duplicatedTableName() throws Exception {
    store.registerProvider(new MockTableProvider("mock", "hello", "world"));
    store.registerProvider(new MockTableProvider("mock1", "hello", "world"));
  }

  private static Table mockTable(String name, String type) {
    return Table.builder()
        .name(name)
        .comment(name + " table")
        .location("/home/admin/" + name)
        .columns(ImmutableList.of(
            Column.builder()
                .name("id")
                .fieldType(TypeName.INT32.type())
                .build(),
            Column.builder()
                .name("name")
                .fieldType(RowSqlTypes.VARCHAR)
                .build()))
        .type(type)
        .properties(new JSONObject())
        .build();
  }

  private static Table mockTable(String name) {
    return mockTable(name, "text");
  }

  private static class MockTableProvider implements TableProvider {
    private String type;
    private String[] names;
    public MockTableProvider(String type, String... names) {
      this.type = type;
      this.names = names;
    }

    @Override public void init() {

    }

    @Override public String getTableType() {
      return type;
    }

    @Override public void createTable(Table table) {

    }

    @Override public void dropTable(String tableName) {

    }

    @Override public List<Table> listTables() {
      List<Table> ret = new ArrayList<>(names.length);
      for (String name : names) {
        ret.add(mockTable(name, "mock"));
      }

      return ret;
    }

    @Override public BeamSqlTable buildBeamSqlTable(Table table) {
      return null;
    }

    @Override public void close() {

    }
  }
}

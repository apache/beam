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

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

/** UnitTest for {@link InMemoryMetaStore}. */
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
    Table actualTable = store.getTables().get("person");
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
  public void testGetTables() throws Exception {
    store.createTable(mockTable("hello"));
    store.createTable(mockTable("world"));

    assertEquals(2, store.getTables().size());
    assertThat(store.getTables(), Matchers.hasValue(mockTable("hello")));
    assertThat(store.getTables(), Matchers.hasValue(mockTable("world")));
  }

  @Test
  public void testBuildBeamSqlTable() throws Exception {
    Table table = mockTable("hello");
    store.createTable(table);
    BeamSqlTable actualSqlTable = store.buildBeamSqlTable(table);
    assertNotNull(actualSqlTable);
    assertEquals(
        Schema.builder()
            .addNullableField("id", Schema.FieldType.INT32)
            .addNullableField("name", Schema.FieldType.STRING)
            .build(),
        actualSqlTable.getSchema());
  }

  @Test
  public void testRegisterProvider() throws Exception {
    store.registerProvider(new MockTableProvider("mock", "hello", "world"));
    assertNotNull(store.getProviders());
    assertEquals(2, store.getProviders().size());
    assertEquals("text", store.getProviders().get("text").getTableType());
    assertEquals("mock", store.getProviders().get("mock").getTableType());

    assertEquals(2, store.getTables().size());
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
        .schema(
            Stream.of(
                    Schema.Field.nullable("id", Schema.FieldType.INT32),
                    Schema.Field.nullable("name", Schema.FieldType.STRING))
                .collect(toSchema()))
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

    @Override
    public String getTableType() {
      return type;
    }

    @Override
    public void createTable(Table table) {}

    @Override
    public void dropTable(String tableName) {}

    @Override
    public Map<String, Table> getTables() {
      Map<String, Table> ret = new HashMap(names.length);
      for (String name : names) {
        ret.put(name, mockTable(name, "mock"));
      }

      return ret;
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
      return null;
    }
  }
}

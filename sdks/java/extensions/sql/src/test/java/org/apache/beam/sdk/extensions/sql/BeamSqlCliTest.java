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
package org.apache.beam.sdk.extensions.sql;

import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.BOOLEAN;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.INTEGER;
import static org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils.VARCHAR;
import static org.apache.beam.sdk.extensions.sql.utils.DateTimeUtils.parseTimestampWithUTCTimeZone;
import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** UnitTest for {@link BeamSqlCli}. */
public class BeamSqlCliTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testExecute_createTextTable_invalidPartitioningError() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Invalid use of 'PARTITIONED BY()': Table 'person' of type 'text' does not support partitioning.");
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "PARTITIONED BY ('id', 'name') \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
  }

  @Test
  public void testExecute_createTextTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixArrayField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "tags ARRAY<VARCHAR>, \n"
            + "matrix ARRAY<ARRAY<INTEGER>> \n"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags", Schema.FieldType.array(VARCHAR)).withNullable(true),
                Field.of("matrix", Schema.FieldType.array(Schema.FieldType.array(INTEGER)))
                    .withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithPrefixMapField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "tags MAP<VARCHAR, VARCHAR>, \n"
            + "nestedMap MAP<INTEGER, MAP<VARCHAR, INTEGER>> \n"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of("tags", Schema.FieldType.map(VARCHAR, VARCHAR)).withNullable(true),
                Field.of(
                        "nestedMap",
                        Schema.FieldType.map(INTEGER, Schema.FieldType.map(VARCHAR, INTEGER)))
                    .withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_createTableWithRowField() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age', \n"
            + "address ROW ( \n"
            + "  street VARCHAR, \n"
            + "  country VARCHAR \n"
            + "  ), \n"
            + "addressAngular ROW< \n"
            + "  street VARCHAR, \n"
            + "  country VARCHAR \n"
            + "  >, \n"
            + "isRobot BOOLEAN"
            + ") \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);
    assertEquals(
        Stream.of(
                Field.of("id", INTEGER).withDescription("id").withNullable(true),
                Field.of("name", VARCHAR).withDescription("name").withNullable(true),
                Field.of("age", INTEGER).withDescription("age").withNullable(true),
                Field.of(
                        "address",
                        Schema.FieldType.row(
                            Schema.builder()
                                .addNullableField("street", VARCHAR)
                                .addNullableField("country", VARCHAR)
                                .build()))
                    .withNullable(true),
                Field.of(
                        "addressAngular",
                        Schema.FieldType.row(
                            Schema.builder()
                                .addNullableField("street", VARCHAR)
                                .addNullableField("country", VARCHAR)
                                .build()))
                    .withNullable(true),
                Field.of("isRobot", BOOLEAN).withNullable(true))
            .collect(toSchema()),
        table.getSchema());
  }

  @Test
  public void testExecute_dropTable() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    Table table = metaStore.getTables().get("person");
    assertNotNull(table);

    cli.execute("drop table person");
    table = metaStore.getTables().get("person");
    assertNull(table);
  }

  @Test(expected = ParseException.class)
  public void testExecute_dropTable_assertTableRemovedFromPlanner() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    cli.execute("drop table person");
    cli.explainQuery("select * from person");
  }

  @Test
  public void testExecute_createCatalog_invalidTypeError() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Could not find type 'abcdef' for catalog 'invalid_catalog'.");
    cli.execute("CREATE CATALOG invalid_catalog TYPE abcdef");
  }

  @Test
  public void testExecute_createCatalog_duplicateCatalogError() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    cli.execute("CREATE CATALOG my_catalog TYPE 'local'");

    // this should be fine.
    cli.execute("CREATE CATALOG IF NOT EXISTS my_catalog TYPE 'local'");

    // without "IF NOT EXISTS", Beam will throw an error
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Catalog 'my_catalog' already exists.");
    cli.execute("CREATE CATALOG my_catalog TYPE 'local'");
  }

  @Test
  public void testExecute_createCatalog() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    assertNull(catalogManager.getCatalog("my_catalog"));
    cli.execute(
        "CREATE CATALOG my_catalog \n"
            + "TYPE 'local' \n"
            + "PROPERTIES (\n"
            + "  'foo' = 'bar', \n"
            + "  'abc' = 'xyz', \n"
            + "  'beam.test.prop' = '123'\n"
            + ")");
    assertNotNull(catalogManager.getCatalog("my_catalog"));
    // we only created the catalog, but have not switched to it
    assertNotEquals("my_catalog", catalogManager.currentCatalog().name());

    Map<String, String> expectedProps =
        ImmutableMap.of(
            "foo", "bar",
            "abc", "xyz",
            "beam.test.prop", "123");
    Catalog catalog = catalogManager.getCatalog("my_catalog");

    assertEquals("my_catalog", catalog.name());
    assertEquals("local", catalog.type());
    assertEquals(expectedProps, catalog.properties());
  }

  @Test
  public void testExecute_setCatalog_doesNotExistError() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot use catalog: 'my_catalog' not found.");
    cli.execute("USE CATALOG my_catalog");
  }

  @Test
  public void testExecute_setCatalog() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    assertNull(catalogManager.getCatalog("catalog_1"));
    assertNull(catalogManager.getCatalog("catalog_2"));
    Map<String, String> catalog1Props =
        ImmutableMap.of("foo", "bar", "abc", "xyz", "beam.test.prop", "123");
    Map<String, String> catalog2Props = ImmutableMap.of("a", "b", "c", "d");
    cli.execute(
        "CREATE CATALOG catalog_1 \n"
            + "TYPE 'local' \n"
            + "PROPERTIES (\n"
            + "  'foo' = 'bar', \n"
            + "  'abc' = 'xyz', \n"
            + "  'beam.test.prop' = '123'\n"
            + ")");
    cli.execute(
        "CREATE CATALOG catalog_2 \n"
            + "TYPE 'local' \n"
            + "PROPERTIES (\n"
            + "  'a' = 'b', \n"
            + "  'c' = 'd' \n"
            + ")");
    assertNotNull(catalogManager.getCatalog("catalog_1"));
    assertNotNull(catalogManager.getCatalog("catalog_2"));

    // catalog manager always starts with a "default" catalog
    assertEquals("default", catalogManager.currentCatalog().name());
    cli.execute("USE CATALOG catalog_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    assertEquals(catalog1Props, catalogManager.currentCatalog().properties());
    cli.execute("USE CATALOG catalog_2");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    assertEquals(catalog2Props, catalogManager.currentCatalog().properties());

    // DEFAULT is a reserved keyword, so need to encapsulate in backticks
    cli.execute("USE CATALOG 'default'");
    assertEquals("default", catalogManager.currentCatalog().name());
  }

  @Test
  public void testExecute_dropCatalog_doesNotExistError() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot drop catalog: 'my_catalog' not found.");
    cli.execute("DROP CATALOG 'my_catalog'");
  }

  @Test
  public void testExecute_dropCatalog_activelyUsedError() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    thrown.expect(CalciteContextException.class);
    thrown.expectMessage(
        "Unable to drop active catalog 'default'. Please switch to another catalog first.");
    cli.execute("DROP CATALOG 'default'");
  }

  @Test
  public void testExecute_dropCatalog() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    assertNull(catalogManager.getCatalog("my_catalog"));
    cli.execute(
        "CREATE CATALOG my_catalog \n"
            + "TYPE 'local' \n"
            + "PROPERTIES (\n"
            + "  'foo' = 'bar', \n"
            + "  'abc' = 'xyz', \n"
            + "  'beam.test.prop' = '123'\n"
            + ")");
    assertNotNull(catalogManager.getCatalog("my_catalog"));

    assertNotEquals("my_catalog", catalogManager.currentCatalog().name());
    cli.execute("DROP CATALOG my_catalog");
    assertNull(catalogManager.getCatalog("my_catalog"));
  }

  @Test
  public void testExecute_tableScopeAcrossCatalogs() throws Exception {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new TextTableProvider());
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    cli.execute("CREATE CATALOG my_catalog TYPE 'local'");
    cli.execute("USE CATALOG my_catalog");
    cli.execute(
        "CREATE EXTERNAL TABLE person (\n" + "id int, name varchar, age int) \n" + "TYPE 'text'");

    assertEquals("my_catalog", catalogManager.currentCatalog().name());
    assertNotNull(catalogManager.currentCatalog().metaStore().getTables().get("person"));

    cli.execute("CREATE CATALOG my_other_catalog TYPE 'local'");
    cli.execute("USE CATALOG my_other_catalog");
    assertEquals("my_other_catalog", catalogManager.currentCatalog().name());
    assertNull(catalogManager.currentCatalog().metaStore().getTables().get("person"));
  }

  @Test
  public void testExplainQuery() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new TextTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);

    cli.execute(
        "CREATE EXTERNAL TABLE person (\n"
            + "id int COMMENT 'id', \n"
            + "name varchar COMMENT 'name', \n"
            + "age int COMMENT 'age') \n"
            + "TYPE 'text' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");

    String plan = cli.explainQuery("select * from person");
    assertThat(
        plan,
        equalTo(
            "BeamCalcRel(expr#0..2=[{inputs}], proj#0..2=[{exprs}])\n"
                + "  BeamIOSourceRel(table=[[beam, person]])\n"));
  }

  @Test
  public void test_time_types() throws Exception {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    TestTableProvider testTableProvider = new TestTableProvider();
    metaStore.registerProvider(testTableProvider);

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(
        "CREATE EXTERNAL TABLE test_table (\n"
            + "f_date DATE, \n"
            + "f_time TIME, \n"
            + "f_ts TIMESTAMP"
            + ") \n"
            + "TYPE 'test'");

    cli.execute(
        "INSERT INTO test_table VALUES ("
            + "DATE '2018-11-01', "
            + "TIME '15:23:59', "
            + "TIMESTAMP '2018-07-01 21:26:07.123' )");

    Table table = metaStore.getTables().get("test_table");
    assertNotNull(table);
    TestTableProvider.TableWithRows tableWithRows = testTableProvider.tables().get(table.getName());
    assertEquals(1, tableWithRows.getRows().size());
    Row row = tableWithRows.getRows().get(0);
    assertEquals(3, row.getFieldCount());

    // test DATE field
    assertEquals("2018-11-01", row.getLogicalTypeValue("f_date", LocalDate.class).toString());
    // test TIME field
    assertEquals("15:23:59", row.getLogicalTypeValue("f_time", LocalTime.class).toString());
    // test TIMESTAMP field
    assertEquals(parseTimestampWithUTCTimeZone("2018-07-01 21:26:07.123"), row.getDateTime("f_ts"));
  }
}

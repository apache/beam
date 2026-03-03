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

import static org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog.DEFAULT;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlAlterCatalog;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.Catalog;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.runtime.CalciteContextException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** UnitTest for {@link BeamSqlCli} using catalogs. */
public class BeamSqlCliCatalogTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;

  @Before
  public void setupCli() {
    catalogManager = new InMemoryCatalogManager();
    cli = new BeamSqlCli().catalogManager(catalogManager);
  }

  @Test
  public void testExecute_createCatalog_invalidTypeError() {
    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage("Could not find type 'abcdef' for catalog 'invalid_catalog'.");
    cli.execute("CREATE CATALOG invalid_catalog TYPE abcdef");
  }

  @Test
  public void testExecute_createCatalog_duplicateCatalogError() {
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
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot use catalog: 'my_catalog' not found.");
    cli.execute("USE CATALOG my_catalog");
  }

  @Test
  public void testExecute_setCatalog() {
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
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage("Cannot drop catalog: 'my_catalog' not found.");
    cli.execute("DROP CATALOG 'my_catalog'");
  }

  @Test
  public void testExecute_dropCatalog_activelyUsedError() {
    thrown.expect(CalciteContextException.class);
    thrown.expectMessage(
        "Unable to drop active catalog 'default'. Please switch to another catalog first.");
    cli.execute("DROP CATALOG 'default'");
  }

  @Test
  public void testExecute_dropCatalog() {
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
  public void testCreateUseDropDatabaseWithSameCatalogScope() {
    // create Catalog catalog_1 and create Database db_1 inside of it
    cli.execute("CREATE CATALOG catalog_1 TYPE 'local'");
    cli.execute("USE CATALOG catalog_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());
    cli.execute("CREATE DATABASE db_1");
    assertTrue(catalogManager.currentCatalog().databaseExists("db_1"));
    cli.execute("USE DATABASE db_1");
    assertEquals("db_1", catalogManager.currentCatalog().currentDatabase());

    // create new Catalog catalog_2 and switch to it
    cli.execute("CREATE CATALOG catalog_2 TYPE 'local'");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    cli.execute("USE CATALOG catalog_2");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());

    // confirm that database 'db_1' from catalog_1 is not leaked to catalog_2
    assertFalse(catalogManager.currentCatalog().databaseExists("db_1"));

    // switch back and drop database
    cli.execute("USE CATALOG catalog_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    cli.execute("DROP DATABASE db_1");
    assertFalse(catalogManager.currentCatalog().databaseExists("db_1"));
  }

  @Test
  public void testCreateWriteDropTableWithSameCatalogScope() {
    // create and use catalog
    cli.execute("CREATE CATALOG catalog_1 TYPE 'local'");
    cli.execute("USE CATALOG catalog_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());

    // create new database
    cli.execute("CREATE DATABASE db_1");
    cli.execute("USE DATABASE db_1");
    assertTrue(catalogManager.currentCatalog().databaseExists("db_1"));
    MetaStore metastoreDb1 =
        checkStateNotNull(catalogManager.getCatalog("catalog_1")).metaStore("db_1");

    // create new table in catalog_1, db_1
    TestTableProvider testTableProvider = new TestTableProvider();
    catalogManager.registerTableProvider(testTableProvider);
    cli.execute("CREATE EXTERNAL TABLE person(id int, name varchar, age int) TYPE 'test'");
    Table table = metastoreDb1.getTable("person");
    assertNotNull(table);

    // write to table
    cli.execute("INSERT INTO person VALUES(123, 'John', 34)");
    TestTableProvider.TableWithRows tableWithRows = testTableProvider.tables().get(table.getName());
    assertEquals(1, tableWithRows.getRows().size());
    Row row = tableWithRows.getRows().get(0);
    Row expectedRow =
        Row.withSchema(
                Schema.builder()
                    .addNullableInt32Field("id")
                    .addNullableStringField("name")
                    .addNullableInt32Field("age")
                    .build())
            .addValues(123, "John", 34)
            .build();
    assertEquals(expectedRow, row);

    // drop the table
    cli.execute("DROP TABLE person");
    assertNull(metastoreDb1.getTable("person"));
  }

  @Test
  public void testCreateUseDropDatabaseWithOtherCatalogScope() {
    // create two catalogs
    cli.execute("CREATE CATALOG catalog_1 TYPE 'local'");
    cli.execute("CREATE CATALOG catalog_2 TYPE 'local'");
    // set default catalog_2
    cli.execute("USE CATALOG catalog_2");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());
    // while using catalog_2, create new database in catalog_1
    cli.execute("CREATE DATABASE catalog_1.db_1");
    assertTrue(checkStateNotNull(catalogManager.getCatalog("catalog_1")).databaseExists("db_1"));

    // use database in catalog_2. this will override both current database (to 'deb_1')
    // and current catalog (to 'catalog_1')
    cli.execute("USE DATABASE catalog_1.db_1");
    assertEquals("catalog_1", catalogManager.currentCatalog().name());
    assertEquals("db_1", catalogManager.currentCatalog().currentDatabase());
    assertTrue(catalogManager.currentCatalog().databaseExists("db_1"));

    // switch back to catalog_2 and drop
    cli.execute("USE CATALOG catalog_2");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    // confirm that database 'db_1' created in catalog_1 was not leaked to catalog_2
    assertFalse(catalogManager.currentCatalog().databaseExists("db_1"));
    // drop and validate
    assertTrue(checkStateNotNull(catalogManager.getCatalog("catalog_1")).databaseExists("db_1"));
    cli.execute("DROP DATABASE catalog_1.db_1");
    assertFalse(checkStateNotNull(catalogManager.getCatalog("catalog_1")).databaseExists("db_1"));
  }

  @Test
  public void testCreateWriteDropTableWithOtherCatalogScope() {
    // create two catalogs
    cli.execute("CREATE CATALOG catalog_1 TYPE 'local'");
    cli.execute("CREATE CATALOG catalog_2 TYPE 'local'");
    // set default catalog_2
    cli.execute("USE CATALOG catalog_2");
    assertEquals("catalog_2", catalogManager.currentCatalog().name());
    assertEquals(DEFAULT, catalogManager.currentCatalog().currentDatabase());

    // while using catalog_2, create new database in catalog_1
    cli.execute("CREATE DATABASE catalog_1.db_1");
    assertTrue(checkStateNotNull(catalogManager.getCatalog("catalog_1")).databaseExists("db_1"));
    MetaStore metastoreDb1 =
        checkStateNotNull(catalogManager.getCatalog("catalog_1")).metaStore("db_1");

    // while using catalog_2, create new table in catalog_1, db_1
    TestTableProvider testTableProvider = new TestTableProvider();
    catalogManager.registerTableProvider(testTableProvider);
    cli.execute(
        "CREATE EXTERNAL TABLE catalog_1.db_1.person(id int, name varchar, age int) TYPE 'test'");
    Table table = metastoreDb1.getTable("person");
    assertNotNull(table);
    // confirm we are still using catalog_2
    assertEquals("catalog_2", catalogManager.currentCatalog().name());

    // write to table while using catalog_2
    cli.execute("INSERT INTO catalog_1.db_1.person VALUES(123, 'John', 34)");
    TestTableProvider.TableWithRows tableWithRows = testTableProvider.tables().get(table.getName());
    assertEquals(1, tableWithRows.getRows().size());
    Row row = tableWithRows.getRows().get(0);
    Row expectedRow =
        Row.withSchema(
                Schema.builder()
                    .addNullableInt32Field("id")
                    .addNullableStringField("name")
                    .addNullableInt32Field("age")
                    .build())
            .addValues(123, "John", 34)
            .build();
    assertEquals(expectedRow, row);
    // confirm we are still using catalog_2
    assertEquals("catalog_2", catalogManager.currentCatalog().name());

    // drop the table while using catalog_2
    cli.execute("DROP TABLE catalog_1.db_1.person");
    assertNull(metastoreDb1.getTable("person"));
  }

  @Test
  public void testAlterCatalog() {
    cli.execute("CREATE CATALOG my_catalog TYPE 'local' PROPERTIES('foo'='abc', 'bar'='xyz')");
    cli.execute("USE CATALOG my_catalog");
    assertEquals(
        ImmutableMap.of("foo", "abc", "bar", "xyz"), catalogManager.currentCatalog().properties());
    cli.execute("ALTER CATALOG my_catalog SET ('foo'='123', 'new'='val')");
    assertEquals(
        ImmutableMap.of("foo", "123", "bar", "xyz", "new", "val"),
        catalogManager.currentCatalog().properties());
    cli.execute("ALTER CATALOG my_catalog RESET ('foo', 'bar')");
    assertEquals(ImmutableMap.of("new", "val"), catalogManager.currentCatalog().properties());
  }

  @Test
  public void testUnparse_SetProperties() {
    SqlNode catalogName = new SqlIdentifier("test_catalog", POS);

    SqlNodeList setProps = new SqlNodeList(POS);
    setProps.add(createPropertyPair("k1", "v1"));
    setProps.add(createPropertyPair("k2", "v2"));

    SqlAlterCatalog alterCatalog = new SqlAlterCatalog(POS, null, catalogName, setProps, null);

    String expectedSql = "ALTER CATALOG `test_catalog` SET ('k1' = 'v1', 'k2' = 'v2')";
    assertEquals(expectedSql, toSql(alterCatalog));
  }

  @Test
  public void testUnparse_ResetProperties() {
    SqlNode catalogName = new SqlIdentifier("test_catalog", POS);

    SqlNodeList resetProps = new SqlNodeList(POS);
    resetProps.add(SqlLiteral.createCharString("k1", POS));
    resetProps.add(SqlLiteral.createCharString("k2", POS));

    SqlAlterCatalog alterCatalog = new SqlAlterCatalog(POS, null, catalogName, null, resetProps);

    String expectedSql = "ALTER CATALOG `test_catalog` RESET ('k1', 'k2')";
    assertEquals(expectedSql, toSql(alterCatalog));
  }

  @Test
  public void testUnparse_SetAndResetProperties() {
    SqlNode catalogName = new SqlIdentifier("my_cat", POS);

    SqlNodeList setProps = new SqlNodeList(POS);
    setProps.add(createPropertyPair("k1", "v1"));

    SqlNodeList resetProps = new SqlNodeList(POS);
    resetProps.add(SqlLiteral.createCharString("k2", POS));

    SqlAlterCatalog alterCatalog =
        new SqlAlterCatalog(POS, null, catalogName, setProps, resetProps);

    String expectedSql = "ALTER CATALOG `my_cat` SET ('k1' = 'v1') RESET ('k2')";
    assertEquals(expectedSql, toSql(alterCatalog));
  }

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  /**
   * Helper to execute the unparse mechanism using a PrettyWriter. This triggers SqlAlter.unparse ->
   * SqlAlterCatalog.unparseAlterOperation.
   */
  private String toSql(SqlAlterCatalog node) {
    SqlPrettyWriter writer = new SqlPrettyWriter(AnsiSqlDialect.DEFAULT);
    writer.setAlwaysUseParentheses(false);
    writer.setSelectListItemsOnSeparateLines(false);
    writer.setIndentation(0);
    node.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }

  /**
   * Helper to create the structure expected by SqlAlterCatalog for K=V pairs. Expects a SqlNodeList
   * containing two literals [Key, Value].
   */
  private SqlNodeList createPropertyPair(String key, String value) {
    SqlNodeList pair = new SqlNodeList(POS);
    pair.add(SqlLiteral.createCharString(key, POS));
    pair.add(SqlLiteral.createCharString(value, POS));
    return pair;
  }
}

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
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.extensions.sql.impl.ParseException;
import org.apache.beam.sdk.extensions.sql.impl.parser.SqlAlterTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlLiteral;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNodeList;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.pretty.SqlPrettyWriter;
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
            + "TYPE 'teXt' \n"
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
            + "TYPE 'TExt' \n"
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
            + "TYPE 'TEXT' \n"
            + "COMMENT '' LOCATION '/home/admin/orders'");
    cli.execute("drop table person");
    cli.explainQuery("select * from person");
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

  @Test
  public void testAlterTableSchema() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    TestTableProvider provider = new TestTableProvider();
    catalogManager.registerTableProvider(provider);
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    cli.execute(
        "CREATE EXTERNAL TABLE test_table(id integer not null, str varchar not null, fl float) type 'test'");
    cli.execute("INSERT INTO test_table VALUES (1, 'a', 0.1), (2, 'b', 0.2), (3, 'c', 0.3)");
    TestTableProvider.TableWithRows tableWithRows = provider.tables().get("test_table");
    assertNotNull(tableWithRows);
    Schema initialSchema =
        Schema.builder()
            .addInt32Field("id")
            .addStringField("str")
            .addNullableFloatField("fl")
            .build();
    assertEquals(initialSchema, tableWithRows.getTable().getSchema());
    List<Row> initialRows =
        Arrays.asList(
            Row.withSchema(initialSchema).addValues(1, "a", 0.1f).build(),
            Row.withSchema(initialSchema).addValues(2, "b", 0.2f).build(),
            Row.withSchema(initialSchema).addValues(3, "c", 0.3f).build());
    assertThat(initialRows, everyItem(is(oneOf(tableWithRows.getRows().toArray(new Row[0])))));

    cli.execute(
        "ALTER TABLE test_table DROP COLUMNS (str, fl) ADD COLUMNS (newBool boolean, newLong bigint)");
    cli.execute("INSERT INTO test_table VALUES (4, true, 4), (5, false, 5), (6, false, 6)");
    Schema newSchema =
        Schema.builder()
            .addInt32Field("id")
            .addNullableBooleanField("newBool")
            .addNullableInt64Field("newLong")
            .build();
    assertEquals(newSchema, tableWithRows.getTable().getSchema());

    // existing rows should have the corresponding values dropped
    List<Row> newRows =
        Arrays.asList(
            Row.withSchema(newSchema).addValues(1, null, null).build(),
            Row.withSchema(newSchema).addValues(2, null, null).build(),
            Row.withSchema(newSchema).addValues(3, null, null).build(),
            Row.withSchema(newSchema).addValues(4, true, 4L).build(),
            Row.withSchema(newSchema).addValues(5, false, 5L).build(),
            Row.withSchema(newSchema).addValues(6, false, 6L).build());
    assertThat(newRows, everyItem(is(oneOf(tableWithRows.getRows().toArray(new Row[0])))));
  }

  @Test
  public void testAlterTableProperties() {
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    TestTableProvider provider = new TestTableProvider();
    catalogManager.registerTableProvider(provider);
    BeamSqlCli cli = new BeamSqlCli().catalogManager(catalogManager);

    cli.execute(
        "CREATE EXTERNAL TABLE test_table(id integer, str varchar) type 'test' "
            + "TBLPROPERTIES '{ \"foo\" : \"123\", \"bar\" : \"abc\"}'");
    TestTableProvider.TableWithRows tableWithRows = provider.tables().get("test_table");
    assertNotNull(tableWithRows);
    assertEquals("123", tableWithRows.getTable().getProperties().get("foo").asText());
    assertEquals("abc", tableWithRows.getTable().getProperties().get("bar").asText());

    cli.execute("ALTER TABLE test_table RESET('bar') SET('foo'='456', 'baz'='xyz')");
    assertEquals("456", tableWithRows.getTable().getProperties().get("foo").asText());
    assertEquals("xyz", tableWithRows.getTable().getProperties().get("baz").asText());
    assertFalse(tableWithRows.getTable().getProperties().has("bar"));
  }

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  @Test
  public void testUnparseAlter_AddColumns() {
    SqlNode tableName = new SqlIdentifier("test_table", POS);

    Field col1 = Field.of("new_col_1", Schema.FieldType.STRING).withNullable(true);
    Field col2 =
        Field.of("new_col_2", Schema.FieldType.INT64)
            .withNullable(false)
            .withDescription("description for col2");

    List<Field> columnsToAdd = Arrays.asList(col1, col2);

    SqlAlterTable alterTable =
        new SqlAlterTable(POS, null, tableName, columnsToAdd, null, null, null, null, null);

    String expectedSql =
        "ALTER TABLE `test_table` "
            + "ADD COLUMNS (`new_col_1` VARCHAR, "
            + "`new_col_2` BIGINT NOT NULL COMMENT `description for col2`)";

    assertEquals(expectedSql, toSql(alterTable));
  }

  @Test
  public void testUnparseAlter_DropColumns() {
    SqlNode tableName = new SqlIdentifier("test_table", POS);

    SqlNodeList columnsToDrop = createStringList("col_to_drop_1", "col_to_drop_2");

    SqlAlterTable alterTable =
        new SqlAlterTable(POS, null, tableName, null, columnsToDrop, null, null, null, null);

    String expectedSql = "ALTER TABLE `test_table` DROP COLUMNS (`col_to_drop_1`, `col_to_drop_2`)";
    assertEquals(expectedSql, toSql(alterTable));
  }

  @Test
  public void testUnparseAlter_AddColumnsAndDropColumns() {
    SqlNode tableName = new SqlIdentifier("test_table", POS);

    // Setup Add
    Field col1 = Field.of("new_col", Schema.FieldType.BOOLEAN).withNullable(true);
    List<Field> columnsToAdd = Arrays.asList(col1);

    // Setup Drop
    SqlNodeList columnsToDrop = createStringList("col_to_drop");

    SqlAlterTable alterTable =
        new SqlAlterTable(
            POS, null, tableName, columnsToAdd, columnsToDrop, null, null, null, null);

    // unparses DROP before ADD
    String expectedSql =
        "ALTER TABLE `test_table` "
            + "DROP COLUMNS (`col_to_drop`) "
            + "ADD COLUMNS (`new_col` BOOLEAN)";

    assertEquals(expectedSql, toSql(alterTable));
  }

  @Test
  public void testUnparseAlter_AddAndDropPartitions() {
    SqlNode tableName = new SqlIdentifier("test_table", POS);

    SqlNodeList partsToAdd = createStringList("p1", "p2");
    SqlNodeList partsToDrop = createStringList("p3");

    SqlAlterTable alterTable =
        new SqlAlterTable(POS, null, tableName, null, null, partsToAdd, partsToDrop, null, null);

    // unparses DROP before ADD
    String expectedSql =
        "ALTER TABLE `test_table` DROP PARTITIONS (`p3`) ADD PARTITIONS (`p1`, `p2`)";

    assertEquals(expectedSql, toSql(alterTable));
  }

  @Test
  public void testUnparseAlter_TableProperties() {
    SqlNode tableName = new SqlIdentifier("test_table", POS);

    SqlNodeList setProps = new SqlNodeList(POS);
    setProps.add(createPropertyPair("prop1", "val1"));

    SqlNodeList resetProps = createStringList("prop2");

    SqlAlterTable alterTable =
        new SqlAlterTable(POS, null, tableName, null, null, null, null, setProps, resetProps);

    // unparses RESET before SET
    String expectedSql = "ALTER TABLE `test_table` RESET ('prop2') SET ('prop1' = 'val1')";

    assertEquals(expectedSql, toSql(alterTable));
  }

  @Test
  public void testUnparseAlter_AllOperations() {
    // A comprehensive test combining all clauses to verify strict ordering
    SqlNode tableName = new SqlIdentifier("full_table", POS);

    List<Field> addCols = Collections.singletonList(Field.of("c1", Schema.FieldType.BOOLEAN));
    SqlNodeList dropCols = createStringList("c_old");
    SqlNodeList addParts = createStringList("p_new");
    SqlNodeList dropParts = createStringList("p_old");
    SqlNodeList setProps = new SqlNodeList(POS);
    setProps.add(createPropertyPair("k", "v"));
    SqlNodeList resetProps = createStringList("k_reset");

    SqlAlterTable alterTable =
        new SqlAlterTable(
            POS, null, tableName, addCols, dropCols, addParts, dropParts, setProps, resetProps);

    // Expected Order based on source code:
    // 1. DROP COLUMNS
    // 2. ADD COLUMNS
    // 3. DROP PARTITIONS
    // 4. ADD PARTITIONS
    // 5. RESET
    // 6. SET
    String expectedSql =
        "ALTER TABLE `full_table` "
            + "DROP COLUMNS (`c_old`) "
            + "ADD COLUMNS (`c1` BOOLEAN NOT NULL) "
            + "DROP PARTITIONS (`p_old`) "
            + "ADD PARTITIONS (`p_new`) "
            + "RESET ('k_reset') "
            + "SET ('k' = 'v')";

    assertEquals(expectedSql, toSql(alterTable));
  }

  /** Helper to execute the unparse mechanism using a PrettyWriter. */
  private String toSql(SqlAlterTable node) {
    SqlPrettyWriter writer = new SqlPrettyWriter(AnsiSqlDialect.DEFAULT);
    writer.setAlwaysUseParentheses(false);
    writer.setSelectListItemsOnSeparateLines(false);
    writer.setIndentation(0);
    node.unparse(writer, 0, 0);
    return writer.toSqlString().getSql();
  }

  /**
   * Helper to create a list of string literals. Useful for Drop Columns, Add/Drop Partitions, Reset
   * Props.
   */
  private SqlNodeList createStringList(String... values) {
    SqlNodeList list = new SqlNodeList(POS);
    for (String val : values) {
      list.add(SqlLiteral.createCharString(val, POS));
    }
    return list;
  }

  /** Helper to create Key=Value pair for Set Properties. */
  private SqlNodeList createPropertyPair(String key, String value) {
    SqlNodeList pair = new SqlNodeList(POS);
    pair.add(SqlLiteral.createCharString(key, POS));
    pair.add(SqlLiteral.createCharString(value, POS));
    return pair;
  }
}

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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static java.lang.String.format;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

/** Unit tests specifically for ALTERing Iceberg catalogs and tables. */
public class BeamSqlCliIcebergAlterTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;
  private String warehouse;
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    catalogManager = new InMemoryCatalogManager();
    cli = new BeamSqlCli().catalogManager(catalogManager);
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile.delete());
    warehouse = "file:" + warehouseFile + "/" + UUID.randomUUID();
  }

  private String createCatalog(String name) {
    return format("CREATE CATALOG %s \n", name)
        + "TYPE iceberg \n"
        + "PROPERTIES (\n"
        + "  'type' = 'hadoop', \n"
        + format("  'warehouse' = '%s')", warehouse);
  }

  @Test
  public void testAlterCatalog() {
    cli.execute(createCatalog("my_catalog"));
    IcebergCatalog catalog =
        (IcebergCatalog) checkStateNotNull(catalogManager.getCatalog("my_catalog"));
    Map<String, String> expected = ImmutableMap.of("type", "hadoop", "warehouse", warehouse);
    assertEquals(expected, catalog.properties());
    assertEquals(expected, catalog.catalogConfig.getCatalogProperties());

    cli.execute("ALTER CATALOG my_catalog SET ('abc'='123', 'foo'='bar') RESET ('type')");
    expected = ImmutableMap.of("warehouse", warehouse, "abc", "123", "foo", "bar");
    assertEquals(expected, catalog.properties());
    assertEquals(expected, catalog.catalogConfig.getCatalogProperties());
  }

  @Test
  public void testAlterTableProps() {
    cli.execute(createCatalog("my_catalog"));
    cli.execute("CREATE DATABASE my_catalog.my_db");
    cli.execute("USE DATABASE my_catalog.my_db");
    cli.execute("CREATE EXTERNAL TABLE my_table(col1 VARCHAR, col2 INTEGER) TYPE 'iceberg'");
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    Table table =
        catalog.catalogConfig.catalog().loadTable(TableIdentifier.parse("my_db.my_table"));
    Map<String, String> baseProps = table.properties();

    cli.execute("ALTER TABLE my_table SET('prop1'='123', 'prop2'='abc', 'prop3'='foo')");
    table.refresh();
    Map<String, String> expectedProps = new HashMap<>(baseProps);
    expectedProps.putAll(ImmutableMap.of("prop1", "123", "prop2", "abc", "prop3", "foo"));

    assertEquals(expectedProps, table.properties());

    cli.execute("ALTER TABLE my_table RESET ('prop1') SET ('prop2'='xyz')");
    expectedProps.put("prop2", "xyz");
    expectedProps.remove("prop1");
    table.refresh();
    assertEquals(expectedProps, table.properties());
  }

  @Test
  public void testAlterTableSchema() {
    cli.execute(createCatalog("my_catalog"));
    cli.execute("CREATE DATABASE my_catalog.my_db");
    cli.execute("USE DATABASE my_catalog.my_db");
    cli.execute("CREATE EXTERNAL TABLE my_table(col1 VARCHAR, col2 INTEGER) TYPE 'iceberg'");
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    Table table =
        catalog.catalogConfig.catalog().loadTable(TableIdentifier.parse("my_db.my_table"));
    Schema actualSchema = table.schema();
    Schema expectedSchema =
        new Schema(
            optional(1, "col1", Types.StringType.get()),
            optional(2, "col2", Types.IntegerType.get()));
    assertTrue(
        String.format("Unequal schemas.\nExpected: %s\nActual: %s", expectedSchema, actualSchema),
        expectedSchema.sameSchema(actualSchema));

    // add some columns
    cli.execute(
        "ALTER TABLE my_table ADD COLUMNS (col3 BOOLEAN COMMENT 'col3-comment', col4 FLOAT COMMENT 'col4-comment')");
    table.refresh();
    actualSchema = table.schema();
    expectedSchema =
        new Schema(
            optional(1, "col1", Types.StringType.get()),
            optional(2, "col2", Types.IntegerType.get()),
            optional(3, "col3", Types.BooleanType.get(), "col3-comment"),
            optional(4, "col4", Types.FloatType.get(), "col4-comment"));
    assertTrue(
        String.format("Unequal schemas.\nExpected: %s\nActual: %s", expectedSchema, actualSchema),
        expectedSchema.sameSchema(actualSchema));

    // remove some columns and add other columns
    cli.execute(
        "ALTER TABLE my_table DROP COLUMNS (col1, col2, col3) ADD COLUMNS (colA VARCHAR, colB INTEGER)");
    table.refresh();
    actualSchema = table.schema();
    expectedSchema =
        new Schema(
            optional(4, "col4", Types.FloatType.get(), "col4-comment"),
            optional(5, "colA", Types.StringType.get()),
            optional(6, "colB", Types.IntegerType.get()));
    assertTrue(
        String.format("Unequal schemas.\nExpected: %s\nActual: %s", expectedSchema, actualSchema),
        expectedSchema.sameSchema(actualSchema));
  }

  @Test
  public void testAlterTableSchemaFailsHelpfullyWhenAddingRequiredColumns() {
    // adding required columns is not yet supported because Beam Schemas do not
    // allow specifying a 'default value' for fields. This concept is required when adding a new
    // Iceberg columns because it allows previously written rows to default to this value.
    cli.execute(createCatalog("my_catalog"));
    cli.execute("CREATE DATABASE my_catalog.my_db");
    cli.execute("USE DATABASE my_catalog.my_db");
    cli.execute("CREATE EXTERNAL TABLE my_table(col1 VARCHAR, col2 INTEGER) TYPE 'iceberg'");

    thrown.expect(UnsupportedOperationException.class);
    thrown.expectMessage(
        "Adding required columns is not yet supported. Encountered required columns: [col3]");
    cli.execute("ALTER TABLE my_table ADD COLUMNS (col3 BOOLEAN NOT NULL)");
  }

  @Test
  public void testAlterTablePartitionSpec() {
    cli.execute(createCatalog("my_catalog"));
    cli.execute("CREATE DATABASE my_catalog.my_db");
    cli.execute("USE DATABASE my_catalog.my_db");
    cli.execute(
        "CREATE EXTERNAL TABLE my_table(col1 VARCHAR, col2 INTEGER, col3 FLOAT, col4 BOOLEAN, col5 TIMESTAMP) "
            + "TYPE 'iceberg' PARTITIONED BY ('col3', 'bucket(col2, 3)')");
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    Table table =
        catalog.catalogConfig.catalog().loadTable(TableIdentifier.parse("my_db.my_table"));
    PartitionSpec actualSpec = table.spec();
    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(table.schema()).identity("col3").bucket("col2", 3).build();
    assertTrue(
        String.format(
            "Partition specs are not compatible.\nExpected: %s\nActual: %s",
            expectedSpec, actualSpec),
        expectedSpec.compatibleWith(actualSpec));

    // add some partitions
    cli.execute("ALTER TABLE my_table ADD PARTITIONS ('col4', 'month(col5)')");
    table.refresh();
    actualSpec = table.spec();
    expectedSpec =
        PartitionSpec.builderFor(table.schema())
            .identity("col3")
            .bucket("col2", 3)
            .identity("col4")
            .month("col5")
            .withSpecId(table.spec().specId())
            .build();
    assertTrue(
        String.format(
            "Partition specs are not compatible.\nExpected: %s\nActual: %s",
            expectedSpec, actualSpec),
        expectedSpec.compatibleWith(actualSpec));

    // remove some partitions and add other partitions
    cli.execute(
        "ALTER TABLE my_table DROP PARTITIONS ('month(col5)', 'bucket(col2, 3)') ADD PARTITIONS ('hour(col5)')");
    table.refresh();
    actualSpec = table.spec();
    expectedSpec =
        PartitionSpec.builderFor(table.schema())
            .identity("col3")
            .identity("col4")
            .hour("col5")
            .withSpecId(table.spec().specId())
            .build();
    assertTrue(
        String.format(
            "Partition specs are not compatible.\nExpected: %s\nActual: %s",
            expectedSpec, actualSpec),
        expectedSpec.compatibleWith(actualSpec));
  }
}

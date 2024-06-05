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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.OUTPUT_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

public class IcebergReadSchemaTransformProviderTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testBuildTransformWithRow() {
    Row catalogConfigRow =
        Row.withSchema(IcebergSchemaTransformCatalogConfig.SCHEMA)
            .withFieldValue("catalog_name", "test_name")
            .withFieldValue("catalog_type", "test_type")
            .withFieldValue("catalog_implementation", "testImplementation")
            .withFieldValue("warehouse_location", "test_location")
            .build();
    Row transformConfigRow =
        Row.withSchema(new IcebergReadSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "test_table_identifier")
            .withFieldValue("catalog_config", catalogConfigRow)
            .build();

    new IcebergReadSchemaTransformProvider().from(transformConfigRow);
  }

  @Test
  public void testSimpleScan() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file2s1.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file3s1.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT1))
        .commit();

    final List<Row> expectedRows =
        Stream.of(
                TestFixtures.FILE1SNAPSHOT1,
                TestFixtures.FILE2SNAPSHOT1,
                TestFixtures.FILE3SNAPSHOT1)
            .flatMap(List::stream)
            .map(record -> SchemaAndRowConversions.recordToRow(schema, record))
            .collect(Collectors.toList());

    IcebergSchemaTransformCatalogConfig catalogConfig =
        IcebergSchemaTransformCatalogConfig.builder()
            .setCatalogName("hadoop")
            .setCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();

    IcebergReadSchemaTransformProvider.Config readConfig =
        IcebergReadSchemaTransformProvider.Config.builder()
            .setTable(identifier)
            .setCatalogConfig(catalogConfig)
            .build();

    PCollection<Row> output =
        PCollectionRowTuple.empty(testPipeline)
            .apply(new IcebergReadSchemaTransformProvider().from(readConfig))
            .get(OUTPUT_TAG);

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  @Test
  public void testReadUsingManagedTransform() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = SchemaAndRowConversions.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s1.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file2s1.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT1))
        .appendFile(
            warehouse.writeRecords(
                "file3s1.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT1))
        .commit();

    final List<Row> expectedRows =
        Stream.of(
                TestFixtures.FILE1SNAPSHOT1,
                TestFixtures.FILE2SNAPSHOT1,
                TestFixtures.FILE3SNAPSHOT1)
            .flatMap(List::stream)
            .map(record -> SchemaAndRowConversions.recordToRow(schema, record))
            .collect(Collectors.toList());

    String yamlConfig =
        String.format(
            "table: %s\n"
                + "catalog_config: \n"
                + "  catalog_name: hadoop\n"
                + "  catalog_type: %s\n"
                + "  warehouse_location: %s",
            identifier, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, warehouse.location);
    Map<String, Object> configMap = new Yaml().load(yamlConfig);

    PCollection<Row> output =
        PCollectionRowTuple.empty(testPipeline)
            .apply(Managed.read(Managed.ICEBERG).withConfig(configMap))
            .get(OUTPUT_TAG);

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }
}

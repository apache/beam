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

import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.OUTPUT_TAG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.yaml.snakeyaml.Yaml;

@RunWith(Parameterized.class)
public class IcebergReadSchemaTransformProviderTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  @Parameter(0)
  public boolean useIncrementalScan;

  @Test
  public void testBuildTransformWithRow() {
    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", "test_location");

    Row.FieldValueBuilder configBuilder =
        Row.withSchema(new IcebergReadSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "test_table_identifier")
            .withFieldValue("catalog_name", "test-name")
            .withFieldValue("catalog_properties", properties);

    if (useIncrementalScan) {
      configBuilder =
          configBuilder
              .withFieldValue("from_snapshot_exclusive", 123L)
              .withFieldValue("to_snapshot", 456L)
              .withFieldValue("triggering_frequency_seconds", 789);
    }

    new IcebergReadSchemaTransformProvider().from(configBuilder.build());
  }

  @Test
  public void testSimpleScan() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    commitData(simpleTable);

    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", warehouse.location);

    Configuration.Builder readConfigBuilder =
        Configuration.builder()
            .setTable(identifier)
            .setCatalogName("name")
            .setCatalogProperties(properties);

    List<List<Record>> expectedRecords =
        Arrays.asList(
            TestFixtures.FILE1SNAPSHOT1,
            TestFixtures.FILE2SNAPSHOT1,
            TestFixtures.FILE3SNAPSHOT1,
            TestFixtures.FILE1SNAPSHOT2,
            TestFixtures.FILE2SNAPSHOT2,
            TestFixtures.FILE3SNAPSHOT2,
            TestFixtures.FILE1SNAPSHOT3,
            TestFixtures.FILE2SNAPSHOT3,
            TestFixtures.FILE3SNAPSHOT3);
    if (useIncrementalScan) {
      // only read files that were added in the second snapshot,
      // ignoring the first and third snapshots.
      expectedRecords = expectedRecords.subList(3, 6);

      Iterator<Snapshot> snapshots = simpleTable.snapshots().iterator();
      long first = snapshots.next().snapshotId();
      long second = snapshots.next().snapshotId();
      readConfigBuilder = readConfigBuilder.setFromSnapshotExclusive(first).setToSnapshot(second);
    }
    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    PCollection<Row> output =
        PCollectionRowTuple.empty(testPipeline)
            .apply(new IcebergReadSchemaTransformProvider().from(readConfigBuilder.build()))
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
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    commitData(simpleTable);

    String yamlConfig =
        String.format(
            "table: %s\n"
                + "catalog_name: test-name\n"
                + "catalog_properties: \n"
                + "  type: %s\n"
                + "  warehouse: %s",
            identifier, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, warehouse.location);

    List<List<Record>> expectedRecords =
        Arrays.asList(
            TestFixtures.FILE1SNAPSHOT1,
            TestFixtures.FILE2SNAPSHOT1,
            TestFixtures.FILE3SNAPSHOT1,
            TestFixtures.FILE1SNAPSHOT2,
            TestFixtures.FILE2SNAPSHOT2,
            TestFixtures.FILE3SNAPSHOT2,
            TestFixtures.FILE1SNAPSHOT3,
            TestFixtures.FILE2SNAPSHOT3,
            TestFixtures.FILE3SNAPSHOT3);
    if (useIncrementalScan) {
      // only read files that were added in the second snapshot,
      // ignoring the first and third snapshots.
      expectedRecords = expectedRecords.subList(3, 6);

      Iterator<Snapshot> snapshots = simpleTable.snapshots().iterator();
      long first = snapshots.next().snapshotId();
      long second = snapshots.next().snapshotId();
      yamlConfig +=
          String.format("\n" + "from_snapshot_exclusive: %s\n" + "to_snapshot: %s", first, second);
    }
    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, Object> configMap = new Yaml().load(yamlConfig);
    PCollection<Row> output =
        testPipeline
            .apply(Managed.read(Managed.ICEBERG).withConfig(configMap))
            .getSinglePCollection();

    PAssert.that(output)
        .satisfies(
            (Iterable<Row> rows) -> {
              assertThat(rows, containsInAnyOrder(expectedRows.toArray()));
              return null;
            });

    testPipeline.run();
  }

  private void commitData(Table simpleTable) throws IOException {
    // first snapshot
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

    // second snapshot
    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s2.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT2))
        .appendFile(
            warehouse.writeRecords(
                "file2s2.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT2))
        .appendFile(
            warehouse.writeRecords(
                "file3s2.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT2))
        .commit();

    // third snapshot
    simpleTable
        .newFastAppend()
        .appendFile(
            warehouse.writeRecords(
                "file1s3.parquet", simpleTable.schema(), TestFixtures.FILE1SNAPSHOT3))
        .appendFile(
            warehouse.writeRecords(
                "file2s3.parquet", simpleTable.schema(), TestFixtures.FILE2SNAPSHOT3))
        .appendFile(
            warehouse.writeRecords(
                "file3s3.parquet", simpleTable.schema(), TestFixtures.FILE3SNAPSHOT3))
        .commit();
  }
}

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

import static org.apache.beam.sdk.io.iceberg.IcebergCdcReadSchemaTransformProvider.Configuration;
import static org.apache.beam.sdk.values.PCollection.IsBounded.BOUNDED;
import static org.apache.beam.sdk.values.PCollection.IsBounded.UNBOUNDED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.HashMap;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

/** Tests for {@link IcebergCdcReadSchemaTransformProvider}. */
public class IcebergCdcReadSchemaTransformProviderTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testBuildTransformWithRow() {
    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", "test_location");

    Row config =
        Row.withSchema(new IcebergCdcReadSchemaTransformProvider().configurationSchema())
            .withFieldValue("table", "test_table_identifier")
            .withFieldValue("catalog_name", "test-name")
            .withFieldValue("catalog_properties", properties)
            .withFieldValue("from_snapshot", 123L)
            .withFieldValue("to_snapshot", 456L)
            .withFieldValue("from_timestamp", 123L)
            .withFieldValue("to_timestamp", 456L)
            .withFieldValue("starting_strategy", "earliest")
            .withFieldValue("poll_interval_seconds", 789)
            .build();

    new IcebergCdcReadSchemaTransformProvider().from(config);
  }

  @Test
  public void testSimpleScan() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    List<List<Record>> expectedRecords = warehouse.commitData(simpleTable);

    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", warehouse.location);

    Configuration.Builder readConfigBuilder =
        Configuration.builder()
            .setTable(identifier)
            .setCatalogName("name")
            .setCatalogProperties(properties)
            .setStartingStrategy("earliest")
            .setToSnapshot(simpleTable.currentSnapshot().snapshotId());

    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    PCollection<Row> output =
        PCollectionRowTuple.empty(testPipeline)
            .apply(new IcebergCdcReadSchemaTransformProvider().from(readConfigBuilder.build()))
            .getSinglePCollection();

    assertThat(output.isBounded(), equalTo(BOUNDED));
    PAssert.that(output).containsInAnyOrder(expectedRows);

    testPipeline.run();
  }

  @Test
  public void testStreamingReadUsingManagedTransform() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, TestFixtures.SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);

    List<List<Record>> expectedRecords = warehouse.commitData(simpleTable).subList(3, 9);
    List<Snapshot> snapshots = Lists.newArrayList(simpleTable.snapshots());
    long second = snapshots.get(1).snapshotId();
    long third = snapshots.get(2).snapshotId();

    String yamlConfig =
        String.format(
            "table: %s\n"
                + "catalog_name: test-name\n"
                + "catalog_properties: \n"
                + "  type: %s\n"
                + "  warehouse: %s\n"
                + "from_snapshot: %s\n"
                + "to_snapshot: %s\n"
                + "streaming: true",
            identifier, CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP, warehouse.location, second, third);

    final List<Row> expectedRows =
        expectedRecords.stream()
            .flatMap(List::stream)
            .map(record -> IcebergUtils.icebergRecordToBeamRow(schema, record))
            .collect(Collectors.toList());

    Map<String, Object> configMap = new Yaml().load(yamlConfig);
    PCollection<Row> output =
        testPipeline
            .apply(Managed.read(Managed.ICEBERG_CDC).withConfig(configMap))
            .getSinglePCollection();

    assertThat(output.isBounded(), equalTo(UNBOUNDED));
    PAssert.that(output).containsInAnyOrder(expectedRows);

    testPipeline.run();
  }
}

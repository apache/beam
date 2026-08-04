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
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.iceberg.cdc.IcebergCdcMetadataColumns;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.yaml.snakeyaml.Yaml;

/** Tests for {@link IcebergCdcReadSchemaTransformProvider}. */
public class IcebergCdcReadSchemaTransformProviderTest {

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final org.apache.iceberg.Schema CDC_SCHEMA =
      new org.apache.iceberg.Schema(TestFixtures.SCHEMA.columns(), ImmutableSet.of(1));

  private static final org.apache.iceberg.Schema CDC_CONFIG_SCHEMA =
      new org.apache.iceberg.Schema(
          ImmutableList.of(
              Types.NestedField.required(1, "id", Types.LongType.get()),
              Types.NestedField.optional(2, "data", Types.StringType.get()),
              Types.NestedField.optional(3, "category", Types.StringType.get()),
              Types.NestedField.required(4, "event_micros", Types.LongType.get())),
          ImmutableSet.of(1));

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
            .withFieldValue("keep", ImmutableList.of("id", "data", "event_micros"))
            .withFieldValue("filter", "\"category\" = 'include'")
            .withFieldValue("watermark_column", "event_micros")
            .withFieldValue("max_snapshot_discovery_delay", 321L)
            .withFieldValue(
                "include_metadata_columns",
                ImmutableList.of(
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID,
                    IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER))
            .build();

    new IcebergCdcReadSchemaTransformProvider().from(config);
  }

  @Test
  public void testSimpleScan() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table simpleTable = warehouse.createTable(tableId, CDC_SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema());

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

    Table simpleTable = warehouse.createTable(tableId, CDC_SCHEMA);
    final Schema schema = IcebergUtils.icebergSchemaToBeamSchema(simpleTable.schema());

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

  @Test
  public void testManagedReadWithProjectionFilterWatermarkAndSnapshotRange() throws Exception {
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    Table table =
        warehouse.createTable(
            tableId, CDC_CONFIG_SCHEMA, null, ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"));
    long eventMicros = (System.currentTimeMillis() - 1_000L) * 1_000L;
    List<Record> records =
        ImmutableList.of(
            record(1L, "keep-a", "include", eventMicros),
            record(2L, "drop", "exclude", eventMicros + 1_000L),
            record(3L, "keep-b", "include", eventMicros + 2_000L));
    table
        .newFastAppend()
        .appendFile(warehouse.writeRecords("cdc-managed-config.parquet", table.schema(), records))
        .commit();

    Map<String, String> properties = new HashMap<>();
    properties.put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP);
    properties.put("warehouse", warehouse.location);

    Map<String, Object> configMap = new HashMap<>();
    configMap.put("table", identifier);
    configMap.put("catalog_name", "test-name");
    configMap.put("catalog_properties", properties);
    configMap.put("from_snapshot", table.currentSnapshot().snapshotId());
    configMap.put("to_snapshot", table.currentSnapshot().snapshotId());
    configMap.put("keep", ImmutableList.of("id", "data", "event_micros"));
    configMap.put("filter", "\"category\" = 'include'");
    configMap.put("watermark_column", "event_micros");
    configMap.put("max_snapshot_discovery_delay", 30L);
    configMap.put(
        "include_metadata_columns",
        ImmutableList.of(
            IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID,
            IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER,
            IcebergCdcMetadataColumns.ROW_ID,
            IcebergCdcMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER));

    org.apache.iceberg.Schema projectedSchema = table.schema().select("id", "data", "event_micros");
    Schema recordSchema = IcebergUtils.icebergSchemaToBeamSchema(projectedSchema);
    Schema outputSchema =
        Schema.builder()
            .addFields(recordSchema.getFields())
            .addInt64Field(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_ID)
            .addInt64Field(IcebergCdcMetadataColumns.COMMIT_SNAPSHOT_SEQUENCE_NUMBER)
            .addNullableField(IcebergCdcMetadataColumns.ROW_ID, Schema.FieldType.INT64)
            .addNullableField(
                IcebergCdcMetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER, Schema.FieldType.INT64)
            .build();
    long snapshotId = table.currentSnapshot().snapshotId();
    long sequenceNumber = table.currentSnapshot().sequenceNumber();
    long firstRowId = table.currentSnapshot().firstRowId();
    List<Row> expectedRows =
        IntStream.range(0, records.size())
            .filter(i -> "include".equals(records.get(i).getField("category")))
            .mapToObj(
                i -> {
                  Row record = IcebergUtils.icebergRecordToBeamRow(recordSchema, records.get(i));
                  return Row.withSchema(outputSchema)
                      .addValues(
                          record.getInt64("id"),
                          record.getString("data"),
                          record.getInt64("event_micros"),
                          snapshotId,
                          sequenceNumber,
                          firstRowId + i,
                          sequenceNumber)
                      .build();
                })
            .collect(Collectors.toList());

    PCollection<Row> output =
        testPipeline
            .apply(Managed.read(Managed.ICEBERG_CDC).withConfig(configMap))
            .getSinglePCollection();

    assertThat(output.isBounded(), equalTo(BOUNDED));
    assertThat(output.getSchema(), equalTo(outputSchema));
    PAssert.that(output).containsInAnyOrder(expectedRows);

    testPipeline.run();
  }

  private static Record record(long id, String data, String category, long eventMicros) {
    return TestFixtures.createRecord(
        CDC_CONFIG_SCHEMA,
        ImmutableMap.of("id", id, "data", data, "category", category, "event_micros", eventMicros));
  }
}

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

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotChanges;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Tests for {@link IcebergIO.WriteRows} with side-input table caching enabled. */
@RunWith(Parameterized.class)
public class IcebergIOSideInputTableCacheTest implements Serializable {

  private static final String NONE = "none";
  private static final String HASH = "hash";
  private static final String HASH_WITH_AUTOSHARDING = "hashWithAutoSharding";

  @Parameterized.Parameters(name = "distributionMode={0}")
  public static Iterable<Object[]> data() {
    return asList(new Object[][] {{NONE}, {HASH}, {HASH_WITH_AUTOSHARDING}});
  }

  @Parameterized.Parameter(0)
  public String distributionMode;

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse =
      new TestDataWarehouse(TEMPORARY_FOLDER, "default_side_input");

  @Rule public transient TestPipeline testPipeline = TestPipeline.create();

  private IcebergCatalogConfig catalogConfig;

  @Before
  public void setUp() {
    Map<String, String> catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .put("warehouse", warehouse.location)
            .build();

    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("hadoop")
            .setCatalogProperties(catalogProps)
            .build();
    TableCache.invalidateAll();
  }

  private IcebergIO.WriteRows applyDistribution(IcebergIO.WriteRows write) {
    if (distributionMode.contains(HASH)) {
      write = write.withDistributionMode(DistributionMode.HASH);
    }
    if (distributionMode.equals(HASH_WITH_AUTOSHARDING)) {
      write = write.withAutosharding();
    }
    return write;
  }

  private long getTablesPolledCount(PipelineResult result) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(TableMetadataDriver.class, "tablesPolled"))
                    .build());
    long total = 0;
    for (MetricResult<Long> counter : metrics.getCounters()) {
      Long val = counter.getCommitted() != null ? counter.getCommitted() : counter.getAttempted();
      if (val != null) {
        total += val;
      }
    }
    return total;
  }

  @Test
  public void testBatchSingleTableWithSideInputCache() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of(
            "default_side_input",
            "single_table_" + Long.toString(UUID.randomUUID().hashCode(), 16));

    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    PCollection<Row> input =
        testPipeline
            .apply("CreateRecords", Create.of(TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1)))
            .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA));

    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withSideInputTableCache()
            .withTableCachePollingBuckets(1);

    input.apply("WriteToTable", applyDistribution(write));
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    assertEquals(1L, getTablesPolledCount(result));

    Table table = warehouse.loadTable(tableId);
    List<Record> writtenRecords = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    assertThat(writtenRecords, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
    assertNotNull(table.currentSnapshot());
  }

  @Test
  public void testBatchDynamicDestinationsWithSideInputCache() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);
    final TableIdentifier table1Id = TableIdentifier.of("default_side_input", "dyn_table1_" + salt);
    final TableIdentifier table2Id = TableIdentifier.of("default_side_input", "dyn_table2_" + salt);
    final TableIdentifier table3Id = TableIdentifier.of("default_side_input", "dyn_table3_" + salt);

    warehouse.createTable(table1Id, TestFixtures.SCHEMA);
    warehouse.createTable(table2Id, TestFixtures.SCHEMA);
    warehouse.createTable(table3Id, TestFixtures.SCHEMA);

    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);
    Schema inputSchema =
        Schema.builder().addStringField("dest").addFields(beamSchema.getFields()).build();

    List<Row> rows = new ArrayList<>();
    for (Record record : TestFixtures.FILE1SNAPSHOT1) {
      Row beamRow = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table1Id))
              .addValues(beamRow.getValues())
              .build());
    }
    for (Record record : TestFixtures.FILE2SNAPSHOT1) {
      Row beamRow = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table2Id))
              .addValues(beamRow.getValues())
              .build());
    }
    for (Record record : TestFixtures.FILE3SNAPSHOT1) {
      Row beamRow = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table3Id))
              .addValues(beamRow.getValues())
              .build());
    }

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          @Override
          public Schema getDataSchema() {
            return beamSchema;
          }

          @Override
          public Row getData(Row element) {
            Row.Builder builder = Row.withSchema(beamSchema);
            for (Schema.Field field : beamSchema.getFields()) {
              builder.addValue(element.getValue(field.getName()));
            }
            return builder.build();
          }

          @Override
          public IcebergDestination instantiateDestination(String destination) {
            return IcebergDestination.builder()
                .setTableIdentifier(IcebergUtils.parseTableIdentifier(destination))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }

          @Override
          public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
            return element.getValue().getString("dest");
          }
        };

    PCollection<Row> input =
        testPipeline.apply("CreateRows", Create.of(rows)).setRowSchema(inputSchema);

    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(dynamicDestinations)
            .withSideInputTableCache()
            .withTableCachePollingBuckets(1);

    input.apply("WriteDynamic", applyDistribution(write));
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    assertEquals(3L, getTablesPolledCount(result));

    Table table1 = warehouse.loadTable(table1Id);
    Table table2 = warehouse.loadTable(table2Id);
    Table table3 = warehouse.loadTable(table3Id);

    List<Record> records1 = ImmutableList.copyOf(IcebergGenerics.read(table1).build());
    List<Record> records2 = ImmutableList.copyOf(IcebergGenerics.read(table2).build());
    List<Record> records3 = ImmutableList.copyOf(IcebergGenerics.read(table3).build());

    assertThat(records1, Matchers.containsInAnyOrder(TestFixtures.FILE1SNAPSHOT1.toArray()));
    assertThat(records2, Matchers.containsInAnyOrder(TestFixtures.FILE2SNAPSHOT1.toArray()));
    assertThat(records3, Matchers.containsInAnyOrder(TestFixtures.FILE3SNAPSHOT1.toArray()));
  }

  @Test
  public void testBatchMaximumCacheSizeSamplingAndFallback() throws Exception {
    final String salt = Long.toString(UUID.randomUUID().hashCode(), 16);
    final TableIdentifier table1Id = TableIdentifier.of("default_side_input", "sample_t1_" + salt);
    final TableIdentifier table2Id = TableIdentifier.of("default_side_input", "sample_t2_" + salt);
    final TableIdentifier table3Id = TableIdentifier.of("default_side_input", "sample_t3_" + salt);
    final TableIdentifier table4Id = TableIdentifier.of("default_side_input", "sample_t4_" + salt);

    warehouse.createTable(table1Id, TestFixtures.SCHEMA);
    warehouse.createTable(table2Id, TestFixtures.SCHEMA);
    warehouse.createTable(table3Id, TestFixtures.SCHEMA);
    warehouse.createTable(table4Id, TestFixtures.SCHEMA);

    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);
    Schema inputSchema =
        Schema.builder().addStringField("dest").addFields(beamSchema.getFields()).build();

    List<Row> rows = new ArrayList<>();
    for (Record record : TestFixtures.FILE1SNAPSHOT1) {
      Row beamRow = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table1Id))
              .addValues(beamRow.getValues())
              .build());
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table2Id))
              .addValues(beamRow.getValues())
              .build());
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table3Id))
              .addValues(beamRow.getValues())
              .build());
      rows.add(
          Row.withSchema(inputSchema)
              .addValue(IcebergUtils.tableIdentifierToString(table4Id))
              .addValues(beamRow.getValues())
              .build());
    }

    DynamicDestinations dynamicDestinations =
        new DynamicDestinations() {
          @Override
          public Schema getDataSchema() {
            return beamSchema;
          }

          @Override
          public Row getData(Row element) {
            Row.Builder builder = Row.withSchema(beamSchema);
            for (Schema.Field field : beamSchema.getFields()) {
              builder.addValue(element.getValue(field.getName()));
            }
            return builder.build();
          }

          @Override
          public IcebergDestination instantiateDestination(String destination) {
            return IcebergDestination.builder()
                .setTableIdentifier(IcebergUtils.parseTableIdentifier(destination))
                .setFileFormat(FileFormat.PARQUET)
                .build();
          }

          @Override
          public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
            return element.getValue().getString("dest");
          }
        };

    PCollection<Row> input =
        testPipeline.apply("CreateSampleRows", Create.of(rows)).setRowSchema(inputSchema);

    // Maximum cache size of 2 forces 2 tables to be cached and 2 to fall back to TableCache
    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(dynamicDestinations)
            .withSideInputTableCache()
            .withMaximumTableCacheSize(2)
            .withTableCachePollingBuckets(1);

    input.apply("WriteWithSampleCap", applyDistribution(write));
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    assertEquals(2L, getTablesPolledCount(result));

    // Verify all 4 tables received data successfully
    for (TableIdentifier tId : asList(table1Id, table2Id, table3Id, table4Id)) {
      Table table = warehouse.loadTable(tId);
      List<Record> written = ImmutableList.copyOf(IcebergGenerics.read(table).build());
      assertEquals(TestFixtures.FILE1SNAPSHOT1.size(), written.size());
    }
  }

  @Test
  public void testStreamingWithSideInputCache() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of(
            "default_side_input",
            "streaming_table_" + Long.toString(UUID.randomUUID().hashCode(), 16));

    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(TestFixtures.SCHEMA);
    List<Row> rows1 = TestFixtures.asRows(TestFixtures.FILE1SNAPSHOT1);
    List<Row> rows2 = TestFixtures.asRows(TestFixtures.FILE2SNAPSHOT1);

    TestStream<Row> testStream =
        TestStream.create(beamSchema)
            .addElements(rows1.get(0), rows1.subList(1, rows1.size()).toArray(new Row[0]))
            .advanceProcessingTime(Duration.standardSeconds(2))
            .addElements(rows2.get(0), rows2.subList(1, rows2.size()).toArray(new Row[0]))
            .advanceProcessingTime(Duration.standardSeconds(2))
            .advanceWatermarkToInfinity();

    PCollection<Row> input = testPipeline.apply("StreamingInput", testStream);

    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withSideInputTableCache()
            .withTriggeringFrequency(Duration.standardSeconds(1))
            .withTableCacheRefreshInterval(Duration.standardSeconds(2))
            .withTableCachePollingBuckets(1);

    input.apply("StreamingWrite", applyDistribution(write));
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    assertThat(getTablesPolledCount(result), Matchers.greaterThanOrEqualTo(1L));

    Table table = warehouse.loadTable(tableId);
    List<Record> written = ImmutableList.copyOf(IcebergGenerics.read(table).build());
    List<Record> expected = new ArrayList<>();
    expected.addAll(TestFixtures.FILE1SNAPSHOT1);
    expected.addAll(TestFixtures.FILE2SNAPSHOT1);
    assertThat(written, Matchers.containsInAnyOrder(expected.toArray()));
    assertNotNull(table.currentSnapshot());
  }

  @Test
  public void testStreamingSpecEvolutionWithoutPipelineRestart() throws Exception {
    TableIdentifier tableId =
        TableIdentifier.of(
            "default_side_input", "spec_evolve_" + Long.toString(UUID.randomUUID().hashCode(), 16));

    // Initial table schema: id (long), name (string), city (string)
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "city", Types.StringType.get()));

    // Create table unpartitioned with format-version 2 to support spec evolution
    Table realTable =
        warehouse.createTable(
            tableId,
            icebergSchema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "2"));

    Schema beamSchema =
        Schema.builder()
            .addInt64Field("id")
            .addNullableStringField("name")
            .addNullableStringField("city")
            .build();

    Row row1 = Row.withSchema(beamSchema).addValues(1L, "alice", "New York").build();
    Row row2 = Row.withSchema(beamSchema).addValues(2L, "bob", "San Francisco").build();

    TestStream<Row> testStream =
        TestStream.create(beamSchema)
            .addElements(row1)
            .advanceProcessingTime(Duration.standardSeconds(2))
            .addElements(row2)
            .advanceProcessingTime(Duration.standardSeconds(2))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        testPipeline
            .apply("StreamingEvolvedInput", testStream)
            .apply(
                "EvolveSpecMidExecution",
                ParDo.of(new EvolveSpecMidExecutionDoFn(catalogConfig, tableId.toString())))
            .setRowSchema(beamSchema);

    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withSideInputTableCache()
            .withTriggeringFrequency(Duration.standardSeconds(1))
            .withTableCacheRefreshInterval(Duration.standardSeconds(1))
            .withTableCachePollingBuckets(1);

    input.apply("StreamingWriteEvolved", write);
    PipelineResult result = testPipeline.run();
    result.waitUntilFinish();

    assertThat(getTablesPolledCount(result), Matchers.greaterThanOrEqualTo(1L));

    realTable.refresh();
    List<DataFile> addedFiles =
        ImmutableList.copyOf(SnapshotChanges.builderFor(realTable).build().addedDataFiles());
    if (addedFiles.size() < 2) {
      List<DataFile> allAddedFiles = new ArrayList<>();
      for (Snapshot s : realTable.snapshots()) {
        for (DataFile df :
            SnapshotChanges.builderFor(realTable).snapshot(s).build().addedDataFiles()) {
          allAddedFiles.add(df);
        }
      }
      addedFiles = allAddedFiles;
    }

    assertEquals(2, addedFiles.size());

    DataFile firstFile = addedFiles.get(0);
    DataFile secondFile = addedFiles.get(1);
    DataFile unpartitionedFile;
    DataFile partitionedFile;
    if (realTable.specs().get(firstFile.specId()).isUnpartitioned()) {
      unpartitionedFile = firstFile;
      partitionedFile = secondFile;
    } else {
      unpartitionedFile = secondFile;
      partitionedFile = firstFile;
    }

    PartitionSpec spec1 = realTable.specs().get(unpartitionedFile.specId());
    assertNotNull(spec1);
    assertTrue("First DataFile must have unpartitioned spec", spec1.isUnpartitioned());

    PartitionSpec spec2 = realTable.specs().get(partitionedFile.specId());
    assertNotNull(spec2);
    assertEquals(1, spec2.fields().size());
    assertEquals("city", spec2.fields().get(0).name());

    List<Record> records = ImmutableList.copyOf(IcebergGenerics.read(realTable).build());
    assertEquals(2, records.size());
  }

  @Test
  public void testPreconditionsAndValidation() {
    TableIdentifier tableId = TableIdentifier.of("default_side_input", "validation_table");
    IcebergIO.WriteRows write = IcebergIO.writeRows(catalogConfig).to(tableId);

    assertThrows(IllegalArgumentException.class, () -> write.withMaximumTableCacheSize(0));
    assertThrows(IllegalArgumentException.class, () -> write.withMaximumTableCacheSize(-1));

    assertThrows(
        IllegalArgumentException.class, () -> write.withTableCacheRefreshInterval(Duration.ZERO));

    assertThrows(IllegalArgumentException.class, () -> write.withTableCachePollingBuckets(0));
    assertThrows(IllegalArgumentException.class, () -> write.withTableCachePollingBuckets(-1));

    // Unbounded streaming pipeline with maximumTableCacheSize must fail at expand
    Pipeline p = Pipeline.create();
    Schema schema = Schema.builder().addInt64Field("id").build();
    TestStream<Row> testStream =
        TestStream.create(schema)
            .addElements(Row.withSchema(schema).addValues(1L).build())
            .advanceWatermarkToInfinity();

    PCollection<Row> streamInput = p.apply("StreamForValidation", testStream);

    // Sub-options specified without withSideInputTableCache() must fail at expand
    IcebergIO.WriteRows writeWithMaxCacheOnly =
        IcebergIO.writeRows(catalogConfig).to(tableId).withMaximumTableCacheSize(5);
    assertThrows(IllegalArgumentException.class, () -> streamInput.apply(writeWithMaxCacheOnly));

    IcebergIO.WriteRows writeWithRefreshIntervalOnly =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withTableCacheRefreshInterval(Duration.standardMinutes(1));
    assertThrows(
        IllegalArgumentException.class, () -> streamInput.apply(writeWithRefreshIntervalOnly));

    IcebergIO.WriteRows writeWithPollingBucketsOnly =
        IcebergIO.writeRows(catalogConfig).to(tableId).withTableCachePollingBuckets(2);
    assertThrows(
        IllegalArgumentException.class, () -> streamInput.apply(writeWithPollingBucketsOnly));

    IcebergIO.WriteRows streamWrite =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withSideInputTableCache()
            .withMaximumTableCacheSize(5);

    assertThrows(IllegalArgumentException.class, () -> streamInput.apply(streamWrite));
  }

  @Test
  public void testDisplayData() {
    TableIdentifier tableId = TableIdentifier.of("default_side_input", "display_data_table");
    IcebergIO.WriteRows write =
        IcebergIO.writeRows(catalogConfig)
            .to(tableId)
            .withSideInputTableCache()
            .withMaximumTableCacheSize(100)
            .withTableCacheRefreshInterval(Duration.standardMinutes(10))
            .withTableCachePollingBuckets(3);

    DisplayData displayData = DisplayData.from(write);
    Map<String, String> items = new HashMap<>();
    for (DisplayData.Item item : displayData.items()) {
      items.put(item.getKey(), item.getValue() != null ? item.getValue().toString() : "");
    }

    assertEquals("true", items.get("useSideInputTableCache"));
    assertEquals("100", items.get("maximumTableCacheSize"));
    assertEquals("600000", items.get("tableCacheRefreshInterval"));
    assertEquals("3", items.get("tableCachePollingBuckets"));
  }

  private static class EvolveSpecMidExecutionDoFn extends DoFn<Row, Row> {
    private final IcebergCatalogConfig catalogConfig;
    private final String tableIdString;

    EvolveSpecMidExecutionDoFn(IcebergCatalogConfig catalogConfig, String tableIdString) {
      this.catalogConfig = catalogConfig;
      this.tableIdString = tableIdString;
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Row> out) {
      Long id = row.getInt64("id");
      if (id != null && id == 2L) {
        Table table =
            catalogConfig.catalog().loadTable(IcebergUtils.parseTableIdentifier(tableIdString));
        if (table.spec().isUnpartitioned()) {
          table.updateSpec().addField("city").commit();
          // Ensure worker-local table ID cache TTL (interval / 2 = 500ms) has elapsed
          try {
            Thread.sleep(700);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      out.output(row);
    }
  }
}

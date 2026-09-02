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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableMetadataDriverTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  private String warehouseLocation;
  private IcebergCatalogConfig catalogConfig;

  private static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addInt64Field("id")
          .addStringField("data")
          .addNullableStringField("dest")
          .build();

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      IcebergUtils.beamSchemaToIcebergSchema(
          Schema.builder().addInt64Field("id").addStringField("data").build());

  private static final TableIdentifier TABLE_ID = TableIdentifier.of("default", "table");

  private static final DynamicDestinations SINGLE_TABLE_DYNAMIC_DESTINATIONS =
      DynamicDestinations.singleTable(TABLE_ID, BEAM_SCHEMA);

  private static final DynamicDestinations DYNAMIC_DESTINATIONS =
      new DynamicDestinations() {
        @Override
        public Schema getDataSchema() {
          return BEAM_SCHEMA;
        }

        @Override
        public Row getData(Row element) {
          return element;
        }

        @Override
        public IcebergDestination instantiateDestination(String destination) {
          return IcebergDestination.builder()
              .setTableIdentifier(IcebergUtils.parseTableIdentifier(destination))
              .build();
        }

        @Override
        public String getTableStringIdentifier(ValueInSingleWindow<Row> element) {
          return element.getValue().getString("dest");
        }
      };

  @Before
  public void setUp() throws Exception {
    warehouseLocation = "file:" + tempFolder.newFolder().getAbsolutePath();
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("hadoop")
            .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouseLocation))
            .build();
  }

  private Catalog getCatalog() {
    return CatalogUtil.loadCatalog(
        CatalogUtil.ICEBERG_CATALOG_HADOOP,
        "hadoop",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation),
        new Configuration());
  }

  @Test
  public void testSingleTableExtractionAndSpecOutput() {
    Table realTable = getCatalog().createTable(TABLE_ID, ICEBERG_SCHEMA);

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(
          Row.withSchema(BEAM_SCHEMA)
              .withFieldValue("id", (long) i)
              .withFieldValue("data", "val_" + i)
              .withFieldValue("dest", null)
              .build());
    }

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .build());

    String expectedTableIdString = IcebergUtils.tableIdentifierToString(TABLE_ID);

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(1, list.size());
              KV<String, SerializableTableSpec> kv = list.get(0);
              assertEquals(expectedTableIdString, kv.getKey());
              SerializableTableSpec spec = kv.getValue();
              assertNotNull(spec);
              assertEquals(realTable.name(), spec.getName());
              assertEquals(realTable.location(), spec.getLocation());
              assertEquals(realTable.schema().asStruct(), spec.getSchema().asStruct());
              assertEquals(realTable.spec(), spec.getPartitionSpec());
              assertNotNull(spec.getFileIO());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testMultipleDynamicDestinationsExtraction() {
    Catalog catalog = getCatalog();
    TableIdentifier tableA = TableIdentifier.of("default", "table_a");
    TableIdentifier tableB = TableIdentifier.of("default", "table_b");
    TableIdentifier tableC = TableIdentifier.of("default", "table_c");

    catalog.createTable(tableA, ICEBERG_SCHEMA);
    catalog.createTable(tableB, ICEBERG_SCHEMA);
    catalog.createTable(tableC, ICEBERG_SCHEMA);

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.table_a").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.table_b").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(3L, "v3", "default.table_c").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(4L, "v4", "default.table_a").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(5L, "v5", "default.table_b").build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(3, list.size());
              Map<String, SerializableTableSpec> map =
                  list.stream().collect(ImmutableMap.toImmutableMap(KV::getKey, KV::getValue));
              assertTrue(map.containsKey("default.table_a"));
              assertTrue(map.containsKey("default.table_b"));
              assertTrue(map.containsKey("default.table_c"));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testWindowedDeduplication() {
    Catalog catalog = getCatalog();
    TableIdentifier table1 = TableIdentifier.of("default", "t1");
    TableIdentifier table2 = TableIdentifier.of("default", "t2");

    catalog.createTable(table1, ICEBERG_SCHEMA);
    catalog.createTable(table2, ICEBERG_SCHEMA);

    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String dest = (i % 2 == 0) ? "default.t1" : "default.t2";
      rows.add(Row.withSchema(BEAM_SCHEMA).addValues((long) i, "val_" + i, dest).build());
    }

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(2, list.size());
              Map<String, SerializableTableSpec> map =
                  list.stream().collect(ImmutableMap.toImmutableMap(KV::getKey, KV::getValue));
              assertTrue(map.containsKey("default.t1"));
              assertTrue(map.containsKey("default.t2"));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testUnboundedGlobalWindowStreamingDeduplication() {
    Catalog catalog = getCatalog();
    TableIdentifier table1 = TableIdentifier.of("default", "stream_t1");
    TableIdentifier table2 = TableIdentifier.of("default", "stream_t2");

    catalog.createTable(table1, ICEBERG_SCHEMA);
    catalog.createTable(table2, ICEBERG_SCHEMA);

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.stream_t1").build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.stream_t2").build();
    Row row3 = Row.withSchema(BEAM_SCHEMA).addValues(3L, "v3", "default.stream_t1").build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row1)
            .addElements(row2)
            .addElements(row3)
            .advanceProcessingTime(Duration.standardSeconds(5))
            .advanceWatermarkToInfinity();

    PCollection<Row> input = pipeline.apply("StreamInput", stream);

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setRefreshInterval(Duration.standardSeconds(2))
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(2, list.size());
              Map<String, SerializableTableSpec> map =
                  list.stream().collect(ImmutableMap.toImmutableMap(KV::getKey, KV::getValue));
              assertTrue(map.containsKey("default.stream_t1"));
              assertTrue(map.containsKey("default.stream_t2"));
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testMaximumCacheSizeCap() {
    Catalog catalog = getCatalog();
    for (int i = 1; i <= 6; i++) {
      catalog.createTable(TableIdentifier.of("default", "cap_table_" + i), ICEBERG_SCHEMA);
    }

    List<Row> rows = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      rows.add(
          Row.withSchema(BEAM_SCHEMA)
              .addValues((long) i, "v_" + i, "default.cap_table_" + i)
              .build());
    }

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    int maxCacheSize = 3;
    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setMaximumCacheSize(maxCacheSize)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(maxCacheSize, list.size());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testUncappedByDefault() {
    Catalog catalog = getCatalog();
    for (int i = 1; i <= 10; i++) {
      catalog.createTable(TableIdentifier.of("default", "uncapped_table_" + i), ICEBERG_SCHEMA);
    }

    List<Row> rows = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      rows.add(
          Row.withSchema(BEAM_SCHEMA)
              .addValues((long) i, "v_" + i, "default.uncapped_table_" + i)
              .build());
    }

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    // Without setting maximumCacheSize, all 10 distinct tables are emitted
    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(10, list.size());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testNonExistentTableIsSkippedWithoutFailingBundle() {
    Catalog catalog = getCatalog();
    TableIdentifier validTable = TableIdentifier.of("default", "existing_table");
    catalog.createTable(validTable, ICEBERG_SCHEMA);

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.existing_table").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.non_existent_table").build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    // Only the existing table is emitted; the non-existent table is skipped without failing bundle
    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(1, list.size());
              assertEquals("default.existing_table", list.get(0).getKey());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testFiltersNullAndBlankTableIdentifiers() {
    TableIdentifier validTableId = TableIdentifier.of("default", "valid_dest_table");
    getCatalog().createTable(validTableId, ICEBERG_SCHEMA);

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", null).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(3L, "v3", "   ").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(4L, "v4", "default.valid_dest_table").build(),
            Row.withSchema(BEAM_SCHEMA)
                .addValues(5L, "v5", "  default.valid_dest_table  ")
                .build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(1, list.size());
              assertEquals("default.valid_dest_table", list.get(0).getKey());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testInvalidMaximumCacheSizeThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setMaximumCacheSize(0)
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setMaximumCacheSize(-5)
                .build());
  }

  @Test
  public void testInvalidRefreshIntervalThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setRefreshInterval(Duration.ZERO)
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setRefreshInterval(Duration.standardSeconds(-5))
                .build());
  }

  @Test
  public void testWindowPreservation() {
    Catalog catalog = getCatalog();
    TableIdentifier tableW1 = TableIdentifier.of("default", "table_w1");
    TableIdentifier tableW2 = TableIdentifier.of("default", "table_w2");

    catalog.createTable(tableW1, ICEBERG_SCHEMA);
    catalog.createTable(tableW2, ICEBERG_SCHEMA);

    Instant t1 = new Instant(1000);
    Instant t2 = new Instant(70000);

    PCollection<Row> input =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(
                        Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.table_w1").build(),
                        t1),
                    TimestampedValue.of(
                        Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.table_w2").build(),
                        t2)))
            .setCoder(RowCoder.of(BEAM_SCHEMA))
            .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, SerializableTableSpec>> list = ImmutableList.copyOf(elements);
              assertEquals(2, list.size());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testEmptyInputProducesEmptyOutput() {
    getCatalog().createTable(TABLE_ID, ICEBERG_SCHEMA);

    PCollection<Row> input = pipeline.apply(Create.empty(RowCoder.of(BEAM_SCHEMA)));

    PCollection<KV<String, SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs).empty();

    pipeline.run();
  }

  @Test
  public void testDisplayData() {
    TableMetadataDriver driver =
        TableMetadataDriver.builder()
            .setCatalogConfig(catalogConfig)
            .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
            .setMaximumCacheSize(42)
            .setRefreshInterval(Duration.standardMinutes(10))
            .build();

    DisplayData displayData = DisplayData.from(driver);
    Map<DisplayData.Identifier, DisplayData.Item> items = displayData.asMap();

    assertNotNull(displayData);
    boolean hasCacheSize = false;
    boolean hasRefreshInterval = false;
    for (DisplayData.Item item : items.values()) {
      if ("maximumCacheSize".equals(item.getKey())) {
        assertEquals(42L, item.getValue());
        hasCacheSize = true;
      }
      if ("refreshInterval".equals(item.getKey())) {
        hasRefreshInterval = true;
      }
    }
    assertTrue(hasCacheSize);
    assertTrue(hasRefreshInterval);
  }

  @Test
  public void testViewAsMapIntegration() {
    PartitionSpec partitionSpec = PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("data").build();
    getCatalog().createTable(TABLE_ID, ICEBERG_SCHEMA, partitionSpec);

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(10L, "partition_val_a", null).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(20L, "partition_val_b", null).build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(catalogConfig, SINGLE_TABLE_DYNAMIC_DESTINATIONS));

    String expectedTableIdString = IcebergUtils.tableIdentifierToString(TABLE_ID);

    PCollection<String> writtenFiles =
        input.apply(
            "WriteWithSideInputTable",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c)
                          throws Exception {
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        SerializableTableSpec spec = viewMap.get(expectedTableIdString);
                        assertNotNull(spec);

                        SideInputTable sideInputTable = new SideInputTable(spec);
                        PartitionKey partitionKey =
                            new PartitionKey(sideInputTable.spec(), sideInputTable.schema());
                        Record record = GenericRecord.create(sideInputTable.schema());
                        record.setField("id", row.getInt64("id"));
                        record.setField("data", row.getString("data"));
                        partitionKey.partition(record);

                        RecordWriter writer =
                            new RecordWriter(
                                sideInputTable,
                                FileFormat.PARQUET,
                                "side_input_test_file_" + row.getInt64("id"),
                                partitionKey,
                                ImmutableMap.of());
                        writer.write(record);
                        writer.close();

                        out.output(writer.getDataFile().path().toString());
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(writtenFiles)
        .satisfies(
            files -> {
              List<String> paths = ImmutableList.copyOf(files);
              assertEquals(2, paths.size());
              return null;
            });

    pipeline.run();
  }
}

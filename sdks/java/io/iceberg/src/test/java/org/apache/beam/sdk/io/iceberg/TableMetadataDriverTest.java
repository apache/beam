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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.iceberg.types.Types;
import org.checkerframework.checker.nullness.qual.Nullable;
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .build());

    String expectedTableIdString = IcebergUtils.tableIdentifierToString(TABLE_ID);

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
              assertEquals(1, list.size());
              KV<String, @Nullable SerializableTableSpec> kv = list.get(0);
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setRefreshInterval(Duration.standardSeconds(2))
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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
  public void testMetadataRefreshedAcrossIntervals() {
    Catalog catalog = getCatalog();
    TableIdentifier tableId = TableIdentifier.of("default", "evolving_table");
    catalog.createTable(tableId, ICEBERG_SCHEMA);

    Row row1 =
        Row.withSchema(BEAM_SCHEMA).addValues(1L, "initial_data", "default.evolving_table").build();
    Row row2 =
        Row.withSchema(BEAM_SCHEMA)
            .addValues(2L, "trigger_update", "default.evolving_table")
            .build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row1)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(row2)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        pipeline
            .apply("StreamInput", stream)
            .apply(
                "EvolveSchemaOnTriggerRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        if ("trigger_update".equals(row.getString("data"))) {
                          Table table =
                              catalogConfig
                                  .catalog()
                                  .loadTable(
                                      IcebergUtils.parseTableIdentifier("default.evolving_table"));
                          table
                              .updateSchema()
                              .addColumn("new_col", Types.StringType.get())
                              .commit();
                        }
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setRefreshInterval(Duration.standardSeconds(2))
                .build());

    // Downstream consumer transform verifying that updated metadata is received
    PCollection<String> consumerReceivedSchemas =
        specs.apply(
            "ConsumerTransform",
            ParDo.of(
                new DoFn<KV<String, @Nullable SerializableTableSpec>, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, @Nullable SerializableTableSpec> element,
                      OutputReceiver<String> out) {
                    SerializableTableSpec spec = checkNotNull(element.getValue());
                    boolean hasNewCol = spec.getSchema().findField("new_col") != null;
                    out.output(hasNewCol ? "UPDATED_SCHEMA" : "INITIAL_SCHEMA");
                  }
                }));

    PAssert.that(consumerReceivedSchemas).containsInAnyOrder("INITIAL_SCHEMA", "UPDATED_SCHEMA");

    pipeline.run();
  }

  @Test
  public void testMetadataRefreshedAcrossIntervalsAsSideInput() {
    Catalog catalog = getCatalog();
    TableIdentifier tableId = TableIdentifier.of("default", "evolving_side_input_table");
    catalog.createTable(tableId, ICEBERG_SCHEMA);

    String tableIdStr = "default.evolving_side_input_table";
    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1L, "initial_data", tableIdStr).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2L, "trigger_update", tableIdStr).build();
    Row row3 = Row.withSchema(BEAM_SCHEMA).addValues(3L, "post_update_data", tableIdStr).build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row1)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(row2)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(row3)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        pipeline
            .apply("StreamInput", stream)
            .apply(
                "EvolveSchemaOnTriggerRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        if ("trigger_update".equals(row.getString("data"))) {
                          Table table =
                              catalogConfig
                                  .catalog()
                                  .loadTable(
                                      IcebergUtils.parseTableIdentifier(
                                          "default.evolving_side_input_table"));
                          table
                              .updateSchema()
                              .addColumn("new_col", Types.StringType.get())
                              .commit();
                        }
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(
                catalogConfig, DYNAMIC_DESTINATIONS, null, Duration.standardSeconds(2)));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        if ("trigger_update".equals(row.getString("data"))) {
                          return;
                        }
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        SerializableTableSpec spec = viewMap.get(row.getString("dest"));
                        assertNotNull("Expected spec in side input view", spec);

                        SideInputTable sideInputTable = new SideInputTable(spec);
                        boolean hasNewCol = sideInputTable.schema().findField("new_col") != null;
                        out.output(
                            row.getString("data")
                                + ":"
                                + (hasNewCol ? "UPDATED_SCHEMA" : "INITIAL_SCHEMA"));
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved)
        .containsInAnyOrder("initial_data:INITIAL_SCHEMA", "post_update_data:UPDATED_SCHEMA");

    pipeline.run();
  }

  @Test
  public void testMetadataRefreshedAcrossIntervalsAsSideInputWithMultipleTables() {
    Catalog catalog = getCatalog();
    TableIdentifier tableA = TableIdentifier.of("default", "multi_table_a");
    TableIdentifier tableB = TableIdentifier.of("default", "multi_table_b");
    catalog.createTable(tableA, ICEBERG_SCHEMA);
    catalog.createTable(tableB, ICEBERG_SCHEMA);

    String tableAStr = "default.multi_table_a";
    String tableBStr = "default.multi_table_b";

    Row rowSeedA = Row.withSchema(BEAM_SCHEMA).addValues(0L, "seed_a", tableAStr).build();
    Row rowSeedB = Row.withSchema(BEAM_SCHEMA).addValues(0L, "seed_b", tableBStr).build();
    Row rowA1 = Row.withSchema(BEAM_SCHEMA).addValues(1L, "a1", tableAStr).build();
    Row rowB1 = Row.withSchema(BEAM_SCHEMA).addValues(2L, "b1", tableBStr).build();
    Row rowTriggerUpdateA =
        Row.withSchema(BEAM_SCHEMA).addValues(3L, "trigger_update_a", tableAStr).build();
    Row rowA2 = Row.withSchema(BEAM_SCHEMA).addValues(4L, "a2", tableAStr).build();
    Row rowB2 = Row.withSchema(BEAM_SCHEMA).addValues(5L, "b2", tableBStr).build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(rowSeedA, rowSeedB)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowA1, rowB1)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowTriggerUpdateA)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowA2, rowB2)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        pipeline
            .apply("StreamInput", stream)
            .apply(
                "EvolveSchemaOnTriggerRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        if ("trigger_update_a".equals(row.getString("data"))) {
                          Table table =
                              catalogConfig
                                  .catalog()
                                  .loadTable(
                                      IcebergUtils.parseTableIdentifier("default.multi_table_a"));
                          table
                              .updateSchema()
                              .addColumn("new_col_a", Types.StringType.get())
                              .commit();
                        }
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(
                catalogConfig, DYNAMIC_DESTINATIONS, null, Duration.standardSeconds(2)));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        String data = row.getString("data");
                        if ("seed_a".equals(data)
                            || "seed_b".equals(data)
                            || "trigger_update_a".equals(data)) {
                          return;
                        }
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        SerializableTableSpec spec = viewMap.get(row.getString("dest"));
                        assertNotNull(
                            "Expected table " + row.getString("dest") + " in side input view",
                            spec);

                        SideInputTable sideInputTable = new SideInputTable(spec);
                        boolean hasNewColA = sideInputTable.schema().findField("new_col_a") != null;
                        out.output(
                            row.getString("data")
                                + ":"
                                + (hasNewColA ? "UPDATED_SCHEMA" : "INITIAL_SCHEMA"));
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved)
        .containsInAnyOrder(
            "a1:INITIAL_SCHEMA", "b1:INITIAL_SCHEMA", "a2:UPDATED_SCHEMA", "b2:INITIAL_SCHEMA");

    pipeline.run();
  }

  @Test
  public void testStreamingNonExistentTableEmitsEmptyMapWithoutBlockingConsumer() {
    Row row =
        Row.withSchema(BEAM_SCHEMA)
            .addValues(1L, "v1", "default.non_existent_streaming_table")
            .build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row)
            .advanceWatermarkToInfinity();

    PCollection<Row> input = pipeline.apply("StreamInput", stream);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(
                catalogConfig, DYNAMIC_DESTINATIONS, null, Duration.standardSeconds(2)));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        assertNotNull("View map should not be null", viewMap);
                        assertTrue(
                            "View map should be empty when all polled tables do not exist",
                            viewMap.isEmpty());
                        out.output("CONSUMER_UNBLOCKED_EMPTY_MAP");
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved).containsInAnyOrder("CONSUMER_UNBLOCKED_EMPTY_MAP");

    pipeline.run();
  }

  @Test
  public void testStreamingMixedExistingAndNonExistentTables() {
    Catalog catalog = getCatalog();
    TableIdentifier validTable = TableIdentifier.of("default", "mixed_valid_table");
    catalog.createTable(validTable, ICEBERG_SCHEMA);

    Row seedValidRow =
        Row.withSchema(BEAM_SCHEMA)
            .addValues(0L, "seed_valid", "default.mixed_valid_table")
            .build();
    Row seedMissingRow =
        Row.withSchema(BEAM_SCHEMA)
            .addValues(0L, "seed_missing", "default.mixed_missing_table")
            .build();
    Row validRow =
        Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.mixed_valid_table").build();
    Row missingRow =
        Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.mixed_missing_table").build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(seedValidRow, seedMissingRow)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(validRow, missingRow)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input = pipeline.apply("StreamInput", stream);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(
                catalogConfig, DYNAMIC_DESTINATIONS, null, Duration.standardSeconds(2)));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        String data = row.getString("data");
                        if ("seed_valid".equals(data) || "seed_missing".equals(data)) {
                          return;
                        }
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        assertNotNull(viewMap);
                        String dest = row.getString("dest");
                        if ("default.mixed_valid_table".equals(dest)) {
                          assertNotNull(viewMap.get(dest));
                          out.output("VALID_TABLE_FOUND");
                        } else {
                          assertTrue(!viewMap.containsKey(dest));
                          out.output("MISSING_TABLE_NOT_FOUND");
                        }
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved)
        .containsInAnyOrder("VALID_TABLE_FOUND", "MISSING_TABLE_NOT_FOUND");

    pipeline.run();
  }

  @Test
  public void testMaximumCacheSizeInStreamingThrowsUnsupportedOperationException() {
    pipeline.enableAbandonedNodeEnforcement(false);
    Row row = Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.test_table").build();
    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row)
            .advanceWatermarkToInfinity();

    PCollection<Row> input = pipeline.apply("StreamInput", stream);

    assertThrows(
        UnsupportedOperationException.class,
        () ->
            input.apply(
                TableMetadataDriver.builder()
                    .setCatalogConfig(catalogConfig)
                    .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                    .setMaximumCacheSize(5)
                    .build()));
  }

  @Test
  public void testMalformedTableIdentifierSkippedWithoutFailingBundle() {
    Row validRow = Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.valid_table").build();
    Row malformedRow =
        Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.invalid..name///").build();

    getCatalog().createTable(TableIdentifier.of("default", "valid_table"), ICEBERG_SCHEMA);

    PCollection<Row> input =
        pipeline.apply(Create.of(validRow, malformedRow)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              Map<String, @Nullable SerializableTableSpec> map = new HashMap<>();
              for (KV<String, @Nullable SerializableTableSpec> elem : elements) {
                map.put(elem.getKey(), elem.getValue());
              }
              assertEquals(2, map.size());
              assertNotNull(map.get("default.valid_table"));
              assertNull(map.get("default.invalid..name///"));
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
    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setMaximumCacheSize(maxCacheSize)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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
    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    // Both existing and missing table entries are emitted; missing table has null spec
    PAssert.that(specs)
        .satisfies(
            elements -> {
              Map<String, @Nullable SerializableTableSpec> map = new HashMap<>();
              for (KV<String, @Nullable SerializableTableSpec> elem : elements) {
                map.put(elem.getKey(), elem.getValue());
              }
              assertEquals(2, map.size());
              assertNotNull(map.get("default.existing_table"));
              assertNull(map.get("default.non_existent_table"));
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
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
  public void testInvalidPollingBucketsThrowsException() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setPollingBuckets(0)
                .build());

    assertThrows(
        IllegalArgumentException.class,
        () ->
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(SINGLE_TABLE_DYNAMIC_DESTINATIONS)
                .setPollingBuckets(-2)
                .build());
  }

  @Test
  public void testConfigurablePollingBuckets() {
    Catalog catalog = getCatalog();
    TableIdentifier table1 = TableIdentifier.of("default", "bucket_t1");
    TableIdentifier table2 = TableIdentifier.of("default", "bucket_t2");
    catalog.createTable(table1, ICEBERG_SCHEMA);
    catalog.createTable(table2, ICEBERG_SCHEMA);

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.bucket_t1").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.bucket_t2").build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .setPollingBuckets(2)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
              assertEquals(2, list.size());
              Map<String, SerializableTableSpec> map =
                  list.stream().collect(ImmutableMap.toImmutableMap(KV::getKey, KV::getValue));
              assertTrue(map.containsKey("default.bucket_t1"));
              assertTrue(map.containsKey("default.bucket_t2"));
              return null;
            });

    pipeline.run();
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

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
        input.apply(
            TableMetadataDriver.builder()
                .setCatalogConfig(catalogConfig)
                .setDynamicDestinations(DYNAMIC_DESTINATIONS)
                .build());

    PAssert.that(specs)
        .satisfies(
            elements -> {
              List<KV<String, @Nullable SerializableTableSpec>> list =
                  ImmutableList.copyOf(elements);
              assertEquals(2, list.size());
              return null;
            });

    pipeline.run();
  }

  @Test
  public void testEmptyInputProducesEmptyOutput() {
    getCatalog().createTable(TABLE_ID, ICEBERG_SCHEMA);

    PCollection<Row> input = pipeline.apply(Create.empty(RowCoder.of(BEAM_SCHEMA)));

    PCollection<KV<String, @Nullable SerializableTableSpec>> specs =
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
            .setPollingBuckets(3)
            .build();

    DisplayData displayData = DisplayData.from(driver);
    Map<DisplayData.Identifier, DisplayData.Item> items = displayData.asMap();

    assertNotNull(displayData);
    boolean hasCacheSize = false;
    boolean hasRefreshInterval = false;
    boolean hasPollingBuckets = false;
    for (DisplayData.Item item : items.values()) {
      if ("maximumCacheSize".equals(item.getKey())) {
        assertEquals(42L, item.getValue());
        hasCacheSize = true;
      }
      if ("refreshInterval".equals(item.getKey())) {
        hasRefreshInterval = true;
      }
      if ("pollingBuckets".equals(item.getKey())) {
        assertEquals(3L, item.getValue());
        hasPollingBuckets = true;
      }
    }
    assertTrue(hasCacheSize);
    assertTrue(hasRefreshInterval);
    assertTrue(hasPollingBuckets);
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

  @Test
  public void testMapMergerFnCommutativeAndTimestampAware() {
    TableIdentifier tableIdA = TableIdentifier.of("default", "merge_table_a");
    Table realTableA = getCatalog().createTable(tableIdA, ICEBERG_SCHEMA);
    SerializableTableSpec specAOld =
        SerializableTableSpec.fromTable(tableIdA, realTableA)
            .toBuilder()
            .setLastUpdatedMillis(1000L)
            .build();
    SerializableTableSpec specANew =
        SerializableTableSpec.fromTable(tableIdA, realTableA)
            .toBuilder()
            .setLastUpdatedMillis(2000L)
            .build();

    TableIdentifier tableIdB = TableIdentifier.of("default", "merge_table_b");
    Table realTableB = getCatalog().createTable(tableIdB, ICEBERG_SCHEMA);
    SerializableTableSpec specB =
        SerializableTableSpec.fromTable(tableIdB, realTableB)
            .toBuilder()
            .setLastUpdatedMillis(1500L)
            .build();

    TableMetadataDriver.MapMergerFn fn = new TableMetadataDriver.MapMergerFn();

    Map<String, SerializableTableSpec> map1 = ImmutableMap.of("tableA", specAOld, "tableB", specB);
    Map<String, SerializableTableSpec> map2 = ImmutableMap.of("tableA", specANew);

    // Left has old, right has new: right wins for tableA
    Map<String, SerializableTableSpec> merged1 = fn.apply(map1, map2);
    assertEquals(2, merged1.size());
    assertEquals(2000L, merged1.get("tableA").getLastUpdatedMillis());
    assertEquals(1500L, merged1.get("tableB").getLastUpdatedMillis());

    // Commutativity: left has new, right has old: left wins for tableA
    Map<String, SerializableTableSpec> merged2 = fn.apply(map2, map1);
    assertEquals(2, merged2.size());
    assertEquals(2000L, merged2.get("tableA").getLastUpdatedMillis());
    assertEquals(1500L, merged2.get("tableB").getLastUpdatedMillis());

    // Identical results in both merge directions
    assertEquals(merged1, merged2);
  }

  @Test
  public void testMapMergerFnTieBreaksBySchemaIdCommutatively() {
    TableIdentifier tableId = TableIdentifier.of("default", "tie_break_table");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA);
    SerializableTableSpec specSchema0 =
        SerializableTableSpec.fromTable(tableId, realTable)
            .toBuilder()
            .setLastUpdatedMillis(1000L)
            .setSchemaId(0)
            .build();
    SerializableTableSpec specSchema1 =
        SerializableTableSpec.fromTable(tableId, realTable)
            .toBuilder()
            .setLastUpdatedMillis(1000L)
            .setSchemaId(1)
            .build();

    TableMetadataDriver.MapMergerFn fn = new TableMetadataDriver.MapMergerFn();

    Map<String, SerializableTableSpec> mapA = ImmutableMap.of("table", specSchema0);
    Map<String, SerializableTableSpec> mapB = ImmutableMap.of("table", specSchema1);

    Map<String, SerializableTableSpec> mergedAB = fn.apply(mapA, mapB);
    Map<String, SerializableTableSpec> mergedBA = fn.apply(mapB, mapA);

    assertEquals(1, mergedAB.get("table").getSchemaId());
    assertEquals(1, mergedBA.get("table").getSchemaId());
    assertEquals(mergedAB, mergedBA);
  }

  static class ControllableTestClock implements TableMetadataDriver.Clock {
    private static final AtomicLong CURRENT_TIME = new AtomicLong(0L);

    public static void setTime(long millis) {
      CURRENT_TIME.set(millis);
    }

    @Override
    public long currentTimeMillis() {
      return CURRENT_TIME.get();
    }
  }

  @Test
  public void testUnusedTablesEvictedFromStreamingCache() {
    TableIdentifier tableIdA = TableIdentifier.of("default", "evict_table_a");
    TableIdentifier tableIdB = TableIdentifier.of("default", "evict_table_b");
    getCatalog().createTable(tableIdA, ICEBERG_SCHEMA);
    getCatalog().createTable(tableIdB, ICEBERG_SCHEMA);

    String tableAStr = IcebergUtils.tableIdentifierToString(tableIdA);
    String tableBStr = IcebergUtils.tableIdentifierToString(tableIdB);

    Duration refreshInterval = Duration.standardSeconds(5);
    ControllableTestClock.setTime(1000L);
    ControllableTestClock testClock = new ControllableTestClock();

    Row rowSeedA = Row.withSchema(BEAM_SCHEMA).addValues(0L, "seed_a", tableAStr).build();
    Row rowSeedB = Row.withSchema(BEAM_SCHEMA).addValues(0L, "seed_b", tableBStr).build();
    Row rowA1 = Row.withSchema(BEAM_SCHEMA).addValues(1L, "a1", tableAStr).build();
    Row rowB1 = Row.withSchema(BEAM_SCHEMA).addValues(2L, "b1", tableBStr).build();
    Row rowTriggerEvictA =
        Row.withSchema(BEAM_SCHEMA).addValues(3L, "trigger_evict_a", tableAStr).build();
    Row rowA2 = Row.withSchema(BEAM_SCHEMA).addValues(4L, "a2", tableAStr).build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(rowSeedA, rowSeedB)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowA1, rowB1)
            .advanceProcessingTime(Duration.standardSeconds(6))
            .addElements(rowTriggerEvictA)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowA2)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        pipeline
            .apply("StreamInput", stream)
            .apply(
                "AdvanceClockOnTriggerRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        if ("trigger_evict_a".equals(row.getString("data"))) {
                          ControllableTestClock.setTime(20000L);
                        }
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(
                catalogConfig, DYNAMIC_DESTINATIONS, null, refreshInterval, null, testClock));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        String data = row.getString("data");
                        if ("seed_a".equals(data)
                            || "seed_b".equals(data)
                            || "trigger_evict_a".equals(data)) {
                          return;
                        }
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        boolean hasA = viewMap.containsKey(tableAStr);
                        boolean hasB = viewMap.containsKey(tableBStr);
                        out.output(data + ":hasA=" + hasA + ",hasB=" + hasB);
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved)
        .containsInAnyOrder(
            "a1:hasA=true,hasB=true", "b1:hasA=true,hasB=true", "a2:hasA=true,hasB=false");

    pipeline.run();
  }

  @Test
  public void testBatchAllNonExistentTablesEmitsEmptyMapWithoutBlockingConsumer() {
    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(BEAM_SCHEMA).addValues(1L, "v1", "default.missing_1").build(),
            Row.withSchema(BEAM_SCHEMA).addValues(2L, "v2", "default.missing_2").build());

    PCollection<Row> input = pipeline.apply(Create.of(rows)).setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView", TableMetadataDriver.asView(catalogConfig, DYNAMIC_DESTINATIONS));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        out.output("size=" + viewMap.size());
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved).containsInAnyOrder("size=0", "size=0");

    pipeline.run();
  }

  @Test
  public void testStreamingDroppedTableImmediatelyInvalidatedInCache() {
    TableIdentifier tableId = TableIdentifier.of("default", "dropped_table");
    getCatalog().createTable(tableId, ICEBERG_SCHEMA);
    String tableStr = IcebergUtils.tableIdentifierToString(tableId);

    Duration refreshInterval = Duration.standardSeconds(2);
    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1L, "initial", tableStr).build();
    Row rowDrop = Row.withSchema(BEAM_SCHEMA).addValues(2L, "trigger_drop", tableStr).build();
    Row rowPostDrop = Row.withSchema(BEAM_SCHEMA).addValues(3L, "post_drop", tableStr).build();

    TestStream<Row> stream =
        TestStream.create(RowCoder.of(BEAM_SCHEMA))
            .advanceWatermarkTo(new Instant(0))
            .addElements(row1)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowDrop)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceProcessingTime(Duration.standardSeconds(3))
            .addElements(rowPostDrop)
            .advanceProcessingTime(Duration.standardSeconds(3))
            .advanceWatermarkToInfinity();

    PCollection<Row> input =
        pipeline
            .apply("StreamInput", stream)
            .apply(
                "DropTableOnTriggerRow",
                ParDo.of(
                    new DoFn<Row, Row>() {
                      @ProcessElement
                      public void processElement(@Element Row row, OutputReceiver<Row> out) {
                        if ("trigger_drop".equals(row.getString("data"))) {
                          catalogConfig
                              .catalog()
                              .dropTable(
                                  IcebergUtils.parseTableIdentifier("default.dropped_table"));
                        }
                        out.output(row);
                      }
                    }))
            .setCoder(RowCoder.of(BEAM_SCHEMA));

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        input.apply(
            "CreateMetadataView",
            TableMetadataDriver.asView(catalogConfig, DYNAMIC_DESTINATIONS, null, refreshInterval));

    PCollection<String> consumerObserved =
        input.apply(
            "ConsumeSideInput",
            ParDo.of(
                    new DoFn<Row, String>() {
                      @ProcessElement
                      public void processElement(
                          @Element Row row, OutputReceiver<String> out, ProcessContext c) {
                        String data = row.getString("data");
                        if ("trigger_drop".equals(data)) {
                          return;
                        }
                        Map<String, SerializableTableSpec> viewMap = c.sideInput(metadataView);
                        out.output(data + ":hasTable=" + viewMap.containsKey(tableStr));
                      }
                    })
                .withSideInputs(metadataView));

    PAssert.that(consumerObserved)
        .containsInAnyOrder("initial:hasTable=true", "post_drop:hasTable=false");

    pipeline.run();
  }
}

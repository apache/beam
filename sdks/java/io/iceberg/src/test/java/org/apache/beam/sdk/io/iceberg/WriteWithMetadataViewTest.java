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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests verifying that file writers and orchestrators correctly resolve table metadata from {@link
 * PCollectionView} of {@link SerializableTableSpec}.
 */
@RunWith(JUnit4.class)
public class WriteWithMetadataViewTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TemporaryFolder tempFolder = new TemporaryFolder();

  private static final org.apache.beam.sdk.schemas.Schema BEAM_SCHEMA =
      org.apache.beam.sdk.schemas.Schema.builder()
          .addInt32Field("id")
          .addStringField("name")
          .addBooleanField("bool")
          .build();

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA);

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(ICEBERG_SCHEMA).identity("bool").build();

  private String warehouseLocation;
  private IcebergCatalogConfig catalogConfig;

  @Before
  public void setUp() throws Exception {
    warehouseLocation = "file:" + tempFolder.newFolder().getAbsolutePath();
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName("hadoop")
            .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", warehouseLocation))
            .build();
    TableCache.invalidateAll();
  }

  private Catalog getCatalog() {
    return CatalogUtil.loadCatalog(
        CatalogUtil.ICEBERG_CATALOG_HADOOP,
        "hadoop",
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation),
        new Configuration());
  }

  @Test
  public void testWriteUngroupedRowsToFilesWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_ungrouped");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "bob", false).build();

    PCollection<KV<String, Row>> input =
        pipeline.apply(
            "CreateInput",
            Create.of(KV.of(tableIdString, row1), KV.of(tableIdString, row2))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(BEAM_SCHEMA))));

    WriteUngroupedRowsToFiles.Result result =
        input.apply(
            new WriteUngroupedRowsToFiles(
                catalogConfig, dynamicDestinations, "prefix", 1024L * 1024L, null, metadataView));

    PCollection<String> tables =
        result
            .getWrittenFiles()
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(f -> IcebergUtils.tableIdentifierToString(f.getTableIdentifier())));

    PAssert.that(tables).containsInAnyOrder(tableIdString, tableIdString);

    PAssert.that(result.getWrittenRows()).containsInAnyOrder(row1, row2);
    pipeline.run();
  }

  @Test
  public void testWriteGroupedRowsToFilesWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_grouped");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "bob", false).build();

    ShardedKey<String> shardedKey = ShardedKey.of(tableIdString, new byte[] {0});
    PCollection<KV<ShardedKey<String>, Iterable<Row>>> input =
        pipeline.apply(
            "CreateGroupedInput",
            Create.of(KV.of(shardedKey, (Iterable<Row>) ImmutableList.of(row1, row2)))
                .withCoder(
                    KvCoder.of(
                        ShardedKey.Coder.of(StringUtf8Coder.of()),
                        IterableCoder.of(RowCoder.of(BEAM_SCHEMA)))));

    PCollection<FileWriteResult> writtenFiles =
        input.apply(
            new WriteGroupedRowsToFiles(
                catalogConfig, dynamicDestinations, "prefix", 1024L * 1024L, null, metadataView));

    PCollection<String> tables =
        writtenFiles.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(f -> IcebergUtils.tableIdentifierToString(f.getTableIdentifier())));

    PAssert.that(tables).containsInAnyOrder(tableIdString, tableIdString);
    pipeline.run();
  }

  @Test
  public void testWriteDirectRowsToFilesWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_direct");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();

    PCollection<KV<String, Row>> input =
        pipeline.apply(
            "CreateDirectInput",
            Create.of(KV.of(tableIdString, row1))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(BEAM_SCHEMA))));

    PCollection<FileWriteResult> writtenFiles =
        input.apply(
            new WriteDirectRowsToFiles(
                catalogConfig, dynamicDestinations, "prefix", 1024L * 1024L, null, metadataView));

    PCollection<String> tables =
        writtenFiles.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(f -> IcebergUtils.tableIdentifierToString(f.getTableIdentifier())));

    PAssert.that(tables).containsInAnyOrder(tableIdString);
    pipeline.run();
  }

  @Test
  public void testWritePartitionedRowsToFilesWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_partitioned");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row partitionRow =
        Row.withSchema(AssignDestinationsAndPartitions.OUTPUT_SCHEMA)
            .addValues(tableIdString, "bool=true")
            .build();
    Row dataRow = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();

    PCollection<KV<Row, Iterable<Row>>> input =
        pipeline.apply(
            "CreatePartitionedInput",
            Create.of(KV.of(partitionRow, (Iterable<Row>) ImmutableList.of(dataRow)))
                .withCoder(
                    KvCoder.of(
                        RowCoder.of(AssignDestinationsAndPartitions.OUTPUT_SCHEMA),
                        IterableCoder.of(RowCoder.of(BEAM_SCHEMA)))));

    PCollection<FileWriteResult> writtenFiles =
        input.apply(
            new WritePartitionedRowsToFiles(
                catalogConfig, dynamicDestinations, "prefix", null, metadataView));

    PCollection<String> tables =
        writtenFiles.apply(
            MapElements.into(TypeDescriptors.strings())
                .via(f -> IcebergUtils.tableIdentifierToString(f.getTableIdentifier())));

    PAssert.that(tables).containsInAnyOrder(tableIdString);
    pipeline.run();
  }

  @Test
  public void testWriteToDestinationsUntriggeredWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_destinations_end_to_end");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "bob", false).build();

    PCollection<KV<String, Row>> input =
        pipeline.apply(
            "CreateInput",
            Create.of(KV.of(tableIdString, row1), KV.of(tableIdString, row2))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(BEAM_SCHEMA))));

    input.apply(
        new WriteToDestinations(
            catalogConfig, dynamicDestinations, null, null, null, metadataView));

    pipeline.run();

    // Verify records committed to table
    realTable.refresh();
    List<Record> committed = ImmutableList.copyOf(IcebergGenerics.read(realTable).build());
    assertEquals(2, committed.size());
  }

  @Test
  public void testWriteToPartitionsWithMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_partitions_end_to_end");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "bob", false).build();

    PCollection<Row> input =
        pipeline.apply("CreateRows", Create.of(row1, row2).withRowSchema(BEAM_SCHEMA));

    PCollection<KV<Row, Row>> assigned =
        input.apply(
            new AssignDestinationsAndPartitions(dynamicDestinations, catalogConfig, metadataView));

    assigned.apply(
        new WriteToPartitions(catalogConfig, dynamicDestinations, null, false, null, metadataView));

    pipeline.run();

    // Verify records committed to table
    realTable.refresh();
    List<Record> committed = ImmutableList.copyOf(IcebergGenerics.read(realTable).build());
    assertEquals(2, committed.size());
  }

  @Test
  public void testWriteUngroupedRowsBypassesCatalogWhenUsingMetadataView() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_bypasses_catalog");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    // Drop table from catalog and invalidate cache so catalog.loadTable() would fail if invoked
    getCatalog().dropTable(tableId, false);
    TableCache.invalidateAll();

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();

    PCollection<KV<String, Row>> input =
        pipeline.apply(
            "CreateInput",
            Create.of(KV.of(tableIdString, row1))
                .withCoder(KvCoder.of(StringUtf8Coder.of(), RowCoder.of(BEAM_SCHEMA))));

    WriteUngroupedRowsToFiles.Result result =
        input.apply(
            new WriteUngroupedRowsToFiles(
                catalogConfig, dynamicDestinations, "prefix", 1024L * 1024L, null, metadataView));

    PCollection<String> tables =
        result
            .getWrittenFiles()
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(f -> IcebergUtils.tableIdentifierToString(f.getTableIdentifier())));

    PAssert.that(tables).containsInAnyOrder(tableIdString);
    PAssert.that(result.getWrittenRows()).containsInAnyOrder(row1);
    pipeline.run();
  }
}

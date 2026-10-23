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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link AssignDestinationsAndPartitions}. */
@RunWith(JUnit4.class)
public class AssignDestinationsAndPartitionsTest implements Serializable {

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
      PartitionSpec.builderFor(ICEBERG_SCHEMA).truncate("name", 3).identity("bool").build();

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
  public void testAssignDestinationsWithoutMetadataViewFallsBackToTableCache() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_table_no_view");
    getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();
    Row row2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "bob", false).build();

    PCollection<Row> input =
        pipeline.apply("CreateInput", Create.of(row1, row2).withRowSchema(BEAM_SCHEMA));

    PCollection<KV<Row, Row>> assigned =
        input.apply(new AssignDestinationsAndPartitions(dynamicDestinations, catalogConfig));

    PCollection<String> partitionPaths =
        assigned.apply(
            "ExtractPartitionPaths",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey().getString(AssignDestinationsAndPartitions.PARTITION)));

    PAssert.that(partitionPaths)
        .containsInAnyOrder("name_trunc=ali/bool=true", "name_trunc=bob/bool=false");

    pipeline.run();
  }

  @Test
  public void testAssignDestinationsWithMetadataViewHit() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_table_view_hit");
    Table realTable = getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);
    SerializableTableSpec spec = SerializableTableSpec.fromTable(tableId, realTable);
    String tableIdString = IcebergUtils.tableIdentifierToString(tableId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    // Drop table from catalog and clear cache so that any catalog fallback would fail to find the
    // spec
    getCatalog().dropTable(tableId);
    TableCache.invalidateAll();

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(tableIdString, spec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();

    PCollection<Row> input =
        pipeline.apply("CreateInput", Create.of(row1).withRowSchema(BEAM_SCHEMA));

    PCollection<KV<Row, Row>> assigned =
        input.apply(
            new AssignDestinationsAndPartitions(dynamicDestinations, catalogConfig, metadataView));

    PCollection<String> partitionPaths =
        assigned.apply(
            "ExtractPartitionPaths",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey().getString(AssignDestinationsAndPartitions.PARTITION)));

    PAssert.that(partitionPaths).containsInAnyOrder("name_trunc=ali/bool=true");

    pipeline.run();
  }

  @Test
  public void testAssignDestinationsWithMetadataViewMissFallsBack() {
    TableIdentifier tableId = TableIdentifier.of("default", "test_table_view_miss");
    getCatalog().createTable(tableId, ICEBERG_SCHEMA, PARTITION_SPEC);

    TableIdentifier otherId = TableIdentifier.of("default", "test_other_table");
    SerializableTableSpec otherSpec =
        SerializableTableSpec.fromTable(tableId, getCatalog().loadTable(tableId));
    String otherIdString = IcebergUtils.tableIdentifierToString(otherId);

    DynamicDestinations dynamicDestinations = DynamicDestinations.singleTable(tableId, BEAM_SCHEMA);

    PCollectionView<Map<String, SerializableTableSpec>> metadataView =
        pipeline
            .apply(
                "CreateMetadata",
                Create.of(KV.of(otherIdString, otherSpec))
                    .withCoder(KvCoder.of(StringUtf8Coder.of(), SerializableTableSpec.getCoder())))
            .apply("AsView", View.asMap());

    Row row1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "alice", true).build();

    PCollection<Row> input =
        pipeline.apply("CreateInput", Create.of(row1).withRowSchema(BEAM_SCHEMA));

    PCollection<KV<Row, Row>> assigned =
        input.apply(
            new AssignDestinationsAndPartitions(dynamicDestinations, catalogConfig, metadataView));

    PCollection<String> partitionPaths =
        assigned.apply(
            "ExtractPartitionPaths",
            MapElements.into(TypeDescriptors.strings())
                .via(kv -> kv.getKey().getString(AssignDestinationsAndPartitions.PARTITION)));

    PAssert.that(partitionPaths).containsInAnyOrder("name_trunc=ali/bool=true");

    pipeline.run();
  }

  @Test
  public void testWeightedReservoirSamplingMergesSkewedBundlesCorrectly() {
    AssignDestinationsAndPartitions.PartitionQuantilesCombineFn combineFn =
        new AssignDestinationsAndPartitions.PartitionQuantilesCombineFn(1000L);
    AssignDestinationsAndPartitions.PartitionSample accum = combineFn.createAccumulator();

    // First, fill the 2048-slot reservoir using 11 small bundles of 200 rows each (2,200 rows total
    // with keys "small_0000" .. "small_0199").
    for (int b = 0; b < 11; b++) {
      List<byte[]> smallSamples = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        smallSamples.add(String.format("small_%04d", i).getBytes(StandardCharsets.UTF_8));
      }
      accum =
          combineFn.addInput(
              accum, new AssignDestinationsAndPartitions.PartitionSample(200L, 200L, smallSamples));
    }

    // Next, merge a single huge bundle representing 1,000,000 rows (with 200 reservoir samples
    // "z_huge_0000" .. "z_huge_0199"). Because the huge bundle represents >99.7% of all rows,
    // weighted reservoir sampling must ensure that the resulting quantile splits come from the
    // huge bundle's key domain rather than being drowned out by the earlier small bundles.
    List<byte[]> hugeSamples = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      hugeSamples.add(String.format("z_huge_%04d", i).getBytes(StandardCharsets.UTF_8));
    }
    accum =
        combineFn.addInput(
            accum,
            new AssignDestinationsAndPartitions.PartitionSample(10_000L, 1_000_000L, hugeSamples));

    List<byte[]> splits = combineFn.extractOutput(accum);
    assertFalse("Expected non-empty split points", splits.isEmpty());
    for (byte[] split : splits) {
      String splitKey = new String(split, StandardCharsets.UTF_8);
      assertTrue(
          "Expected split point to reflect the 1M-row bundle, but got: " + splitKey,
          splitKey.startsWith("z_huge_"));
    }
  }
}

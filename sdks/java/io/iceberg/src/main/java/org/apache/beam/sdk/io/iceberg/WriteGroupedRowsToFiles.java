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

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class WriteGroupedRowsToFiles
    extends PTransform<
        PCollection<KV<ShardedKey<String>, Iterable<Row>>>, PCollection<FileWriteResult>> {

  private static final long DEFAULT_MAX_BYTES_PER_FILE = (1L << 29); // 512mb

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final @Nullable Map<String, String> partitionSpec; // Add this field

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> partitionSpec) { // Add partitionSpec to the constructor
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.partitionSpec = partitionSpec; // Initialize the partition spec
  }

  @Override
  public PCollection<FileWriteResult> expand(
      PCollection<KV<ShardedKey<String>, Iterable<Row>>> input) {
    return input.apply(
        ParDo.of(
            new WriteGroupedRowsToFilesDoFn(
                catalogConfig,
                dynamicDestinations,
                DEFAULT_MAX_BYTES_PER_FILE,
                filePrefix,
                partitionSpec))); // Pass partitionSpec to the DoFn
  }

  private static class WriteGroupedRowsToFilesDoFn
      extends DoFn<KV<ShardedKey<String>, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private transient @MonotonicNonNull Catalog catalog;
    private final String filePrefix;
    private final long maxFileSize;
    private final @Nullable Map<String, String> partitionSpec; // Add this field

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix,
        @Nullable Map<String, String> partitionSpec) { // Add partitionSpec to the constructor
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.maxFileSize = maxFileSize;
      this.partitionSpec = partitionSpec; // Initialize the partition spec
    }

    private org.apache.iceberg.catalog.Catalog getCatalog() {
      if (catalog == null) {
        this.catalog = catalogConfig.catalog();
      }
      return catalog;
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<ShardedKey<String>, Iterable<Row>> element,
        BoundedWindow window,
        PaneInfo pane)
        throws Exception {

      String tableIdentifier = element.getKey().getKey();
      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      WindowedValue<IcebergDestination> windowedDestination =
          WindowedValue.of(destination, window.maxTimestamp(), window, pane);

      // Create or load the table with the specified partition spec
      TableIdentifier tableId = TableIdentifier.parse(tableIdentifier);
      Catalog catalog = getCatalog();
      Table table;
      // If the table doesn't exist, create it with the specified partition spec
      if (!catalog.tableExists(tableId)) {
        // Convert Beam Schema to Iceberg Schema
        org.apache.beam.sdk.schemas.Schema beamSchema = dynamicDestinations.getDataSchema();
        org.apache.iceberg.Schema icebergSchema =
            IcebergUtils.beamSchemaToIcebergSchema(beamSchema);
        table = createTableWithPartitionSpec(catalog, tableId, icebergSchema, partitionSpec);
      } else {
        table = catalog.loadTable(tableId);
      }

      // Write rows to files
      RecordWriterManager writer;
      try (RecordWriterManager openWriter =
          new RecordWriterManager(getCatalog(), filePrefix, maxFileSize, Integer.MAX_VALUE)) {
        writer = openWriter;
        for (Row e : element.getValue()) {
          writer.write(windowedDestination, e);
        }
      }

      // Emit file write results
      List<SerializableDataFile> serializableDataFiles =
          Preconditions.checkNotNull(writer.getSerializableDataFiles().get(windowedDestination));
      for (SerializableDataFile dataFile : serializableDataFiles) {
        c.output(
            FileWriteResult.builder()
                .setTableIdentifier(destination.getTableIdentifier())
                .setSerializableDataFile(dataFile)
                .build());
      }
    }

    // Helper method to create a table with the specified partition spec
    private Table createTableWithPartitionSpec(
        Catalog catalog,
        TableIdentifier tableId,
        Schema schema,
        Map<String, String> partitionSpecMap) {
      if (partitionSpecMap != null) {
        PartitionSpec partitionSpec = createPartitionSpec(schema, partitionSpecMap);
        return catalog.createTable(tableId, schema, partitionSpec);
      } else {
        return catalog.createTable(tableId, schema);
      }
    }

    // Helper method to create a partition spec from a map of column names to transforms
    private PartitionSpec createPartitionSpec(Schema schema, Map<String, String> partitionSpecMap) {
      PartitionSpec.Builder partitionSpecBuilder = PartitionSpec.builderFor(schema);
      for (Map.Entry<String, String> entry : partitionSpecMap.entrySet()) {
        String column = entry.getKey();
        String transform = entry.getValue();
        switch (transform) {
          case "identity":
            partitionSpecBuilder.identity(column);
            break;
          case "year":
            partitionSpecBuilder.year(column);
            break;
          case "month":
            partitionSpecBuilder.month(column);
            break;
          case "day":
            partitionSpecBuilder.day(column);
            break;
          case "bucket16": // Predefined bucket transforms
            partitionSpecBuilder.bucket(column, 16);
            break;
          case "bucket32":
            partitionSpecBuilder.bucket(column, 32);
            break;
          case "truncate10": // Predefined truncate transforms
            partitionSpecBuilder.truncate(column, 10);
            break;
          default:
            throw new IllegalArgumentException("Unsupported partition transform: " + transform);
        }
      }
      return partitionSpecBuilder.build();
    }
  }
}

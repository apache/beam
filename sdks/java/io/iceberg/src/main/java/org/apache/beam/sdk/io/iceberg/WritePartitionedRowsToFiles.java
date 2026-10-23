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

import static org.apache.beam.sdk.io.iceberg.AssignDestinationsAndPartitions.DESTINATION;
import static org.apache.beam.sdk.io.iceberg.AssignDestinationsAndPartitions.PARTITION;
import static org.apache.beam.sdk.io.iceberg.RecordWriterManager.getPartitionDataPath;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class WritePartitionedRowsToFiles
    extends PTransform<PCollection<KV<Row, Iterable<Row>>>, PCollection<FileWriteResult>> {
  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final long maxFileSize;
  private final @Nullable Map<String, String> writeProperties;
  private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
  private final boolean sortedWrites;

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> writeProperties) {
    this(
        catalogConfig,
        dynamicDestinations,
        filePrefix,
        WriteToPartitions.DEFAULT_BYTES_PER_FILE,
        writeProperties,
        null,
        false);
  }

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
    this(
        catalogConfig,
        dynamicDestinations,
        filePrefix,
        WriteToPartitions.DEFAULT_BYTES_PER_FILE,
        writeProperties,
        metadataView,
        false);
  }

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      boolean sortedWrites) {
    this(
        catalogConfig,
        dynamicDestinations,
        filePrefix,
        WriteToPartitions.DEFAULT_BYTES_PER_FILE,
        writeProperties,
        metadataView,
        sortedWrites);
  }

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxFileSize,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      boolean sortedWrites) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.maxFileSize = maxFileSize;
    this.writeProperties = writeProperties;
    this.metadataView = metadataView;
    this.sortedWrites = sortedWrites;
  }

  @Override
  public PCollection<FileWriteResult> expand(PCollection<KV<Row, Iterable<Row>>> input) {
    Schema dataSchema = dynamicDestinations.getDataSchema();
    ParDo.SingleOutput<KV<Row, Iterable<Row>>, FileWriteResult> parDo =
        ParDo.of(
            new WriteDoFn(
                catalogConfig,
                dynamicDestinations,
                filePrefix,
                maxFileSize,
                dataSchema,
                writeProperties,
                metadataView,
                sortedWrites));
    if (metadataView != null) {
      parDo = parDo.withSideInputs(metadataView);
    }
    return input.apply(parDo);
  }

  private static class WriteDoFn extends DoFn<KV<Row, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final String filePrefix;
    private final long maxFileSize;
    private final Schema dataSchema;
    private final @Nullable Map<String, String> writeProperties;
    private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
    private final boolean sortedWrites;
    private transient @MonotonicNonNull Map<TableIdentifier, Integer> specIds;
    private transient @MonotonicNonNull Map<TableIdentifier, Map<String, PartitionField>>
        partitionFieldMaps;

    WriteDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        String filePrefix,
        long maxFileSize,
        Schema dataSchema,
        @Nullable Map<String, String> writeProperties,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
        boolean sortedWrites) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.maxFileSize = maxFileSize;
      this.dataSchema = dataSchema;
      this.writeProperties = writeProperties;
      this.metadataView = metadataView;
      this.sortedWrites = sortedWrites;
    }

    @Setup
    public void setup() {
      partitionFieldMaps = Maps.newHashMap();
      specIds = Maps.newHashMap();
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<Row, Iterable<Row>> element,
        OutputReceiver<FileWriteResult> out)
        throws Exception {
      String tableIdentifier = checkStateNotNull(element.getKey().getString(DESTINATION));
      String partitionPath = checkStateNotNull(element.getKey().getString(PARTITION));

      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      Map<String, SerializableTableSpec> sideInputs =
          metadataView != null ? c.sideInput(metadataView) : null;
      Table table = getOrCreateTable(destination, dataSchema, sideInputs);
      partitionPath =
          getPartitionDataPath(
              partitionPath, getPartitionFieldMap(destination.getTableIdentifier(), table));

      StructLike partitionData =
          table.spec().isPartitioned()
              ? DataFiles.data(table.spec(), partitionPath)
              : new PartitionKey(table.spec(), table.schema());

      List<SerializableDataFile> writtenFiles = new ArrayList<>();
      RecordWriter writer = null;
      try (CloseableIterable<Row> sortedOrUnsortedRows =
          sortedWrites && table.sortOrder().isSorted()
              ? IcebergRowSorter.sortRows(
                  element.getValue(), table.sortOrder(), table.schema(), dataSchema)
              : CloseableIterable.withNoopClose(element.getValue())) {
        for (Row row : sortedOrUnsortedRows) {
          if (writer != null && writer.bytesWritten() > maxFileSize) {
            RecordWriter finishedWriter = writer;
            writer = null;
            finishedWriter.close();
            writtenFiles.add(
                SerializableDataFile.from(finishedWriter.getDataFile(), table.specs()));
          }
          if (writer == null) {
            String fileName =
                destination
                    .getFileFormat()
                    .addExtension(String.format("%s-%s", filePrefix, UUID.randomUUID()));
            writer =
                new RecordWriter(
                    table, destination.getFileFormat(), fileName, partitionData, writeProperties);
          }
          Record record = IcebergUtils.beamRowToIcebergRecord(table.schema(), row);
          writer.write(record);
        }
        if (writer != null) {
          RecordWriter finishedWriter = writer;
          writer = null;
          finishedWriter.close();
          writtenFiles.add(SerializableDataFile.from(finishedWriter.getDataFile(), table.specs()));
        }
      } finally {
        if (writer != null) {
          writer.close();
        }
      }

      for (SerializableDataFile sdf : writtenFiles) {
        out.output(
            FileWriteResult.builder()
                .setTableIdentifier(destination.getTableIdentifier())
                .setSerializableDataFile(sdf)
                .build());
      }
    }

    private Map<String, PartitionField> getPartitionFieldMap(
        TableIdentifier identifier, Table table) {
      @Nullable Integer specId = checkStateNotNull(specIds).get(identifier);
      if (specId != null && specId == table.spec().specId()) {
        return checkStateNotNull(checkStateNotNull(partitionFieldMaps).get(identifier));
      }
      Map<String, PartitionField> partitionFieldMap = Maps.newHashMap();
      for (PartitionField partitionField : table.spec().fields()) {
        partitionFieldMap.put(partitionField.name(), partitionField);
      }
      checkStateNotNull(specIds).put(identifier, table.spec().specId());
      checkStateNotNull(partitionFieldMaps).put(identifier, partitionFieldMap);
      return partitionFieldMap;
    }

    Table getOrCreateTable(
        IcebergDestination destination,
        Schema dataSchema,
        @Nullable Map<String, SerializableTableSpec> sideInputTableSpecs) {
      return RecordWriterManager.getOrCreateTable(
          catalogConfig, destination, dataSchema, sideInputTableSpecs);
    }

    Table getOrCreateTable(IcebergDestination destination, Schema dataSchema) {
      return getOrCreateTable(destination, dataSchema, null);
    }
  }
}

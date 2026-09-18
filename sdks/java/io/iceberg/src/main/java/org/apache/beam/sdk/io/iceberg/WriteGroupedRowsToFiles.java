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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.checkerframework.checker.nullness.qual.Nullable;

class WriteGroupedRowsToFiles
    extends PTransform<
        PCollection<KV<ShardedKey<String>, Iterable<Row>>>, PCollection<FileWriteResult>> {
  private final long maxBytesPerFile;

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final @Nullable Map<String, String> writeProperties;
  private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
  private final boolean sortedWrites;

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxBytesPerFile,
      @Nullable Map<String, String> writeProperties) {
    this(
        catalogConfig,
        dynamicDestinations,
        filePrefix,
        maxBytesPerFile,
        writeProperties,
        null,
        false);
  }

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxBytesPerFile,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
    this(
        catalogConfig,
        dynamicDestinations,
        filePrefix,
        maxBytesPerFile,
        writeProperties,
        metadataView,
        false);
  }

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxBytesPerFile,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView,
      boolean sortedWrites) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.maxBytesPerFile = maxBytesPerFile;
    this.writeProperties = writeProperties;
    this.metadataView = metadataView;
    this.sortedWrites = sortedWrites;
  }

  @Override
  public PCollection<FileWriteResult> expand(
      PCollection<KV<ShardedKey<String>, Iterable<Row>>> input) {
    Schema dataSchema = dynamicDestinations.getDataSchema();
    ParDo.SingleOutput<KV<ShardedKey<String>, Iterable<Row>>, FileWriteResult> parDo =
        ParDo.of(
            new WriteGroupedRowsToFilesDoFn(
                catalogConfig,
                dynamicDestinations,
                maxBytesPerFile,
                filePrefix,
                dataSchema,
                writeProperties,
                metadataView,
                sortedWrites));
    if (metadataView != null) {
      parDo = parDo.withSideInputs(metadataView);
    }
    return input.apply(parDo);
  }

  private static class WriteGroupedRowsToFilesDoFn
      extends DoFn<KV<ShardedKey<String>, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final String filePrefix;
    private final long maxFileSize;
    private final Schema dataSchema;
    private final @Nullable Map<String, String> writeProperties;
    private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;
    private final boolean sortedWrites;

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix,
        Schema dataSchema,
        @Nullable Map<String, String> writeProperties) {
      this(
          catalogConfig,
          dynamicDestinations,
          maxFileSize,
          filePrefix,
          dataSchema,
          writeProperties,
          null,
          false);
    }

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix,
        Schema dataSchema,
        @Nullable Map<String, String> writeProperties,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
      this(
          catalogConfig,
          dynamicDestinations,
          maxFileSize,
          filePrefix,
          dataSchema,
          writeProperties,
          metadataView,
          false);
    }

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix,
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

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<ShardedKey<String>, Iterable<Row>> element,
        BoundedWindow window,
        PaneInfo paneInfo)
        throws Exception {

      String tableIdentifier = element.getKey().getKey();
      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      WindowedValue<IcebergDestination> windowedDestination =
          WindowedValues.of(destination, window.maxTimestamp(), window, paneInfo);
      Map<String, SerializableTableSpec> sideInputs =
          metadataView != null ? c.sideInput(metadataView) : null;
      RecordWriterManager writer;
      try (RecordWriterManager openWriter =
          new RecordWriterManager(
              catalogConfig,
              filePrefix,
              maxFileSize,
              Integer.MAX_VALUE,
              writeProperties,
              sideInputs)) {
        writer = openWriter;
        Table table = sortedWrites ? openWriter.getOrCreateTable(destination, dataSchema) : null;
        try (CloseableIterable<Row> sortedOrUnsortedRows =
            table != null && table.sortOrder().isSorted()
                ? IcebergRowSorter.sortRows(
                    element.getValue(), table.sortOrder(), table.schema(), dataSchema)
                : CloseableIterable.withNoopClose(element.getValue())) {
          for (Row e : sortedOrUnsortedRows) {
            writer.write(windowedDestination, e);
          }
        }
      }

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
  }
}

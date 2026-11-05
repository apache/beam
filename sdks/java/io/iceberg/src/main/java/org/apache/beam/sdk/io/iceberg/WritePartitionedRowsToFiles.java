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
import static org.apache.beam.sdk.io.iceberg.WriteToPartitions.DEFAULT_BYTES_PER_FILE;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.checkerframework.checker.nullness.qual.Nullable;

class WritePartitionedRowsToFiles
    extends PTransform<PCollection<KV<Row, Iterable<Row>>>, PCollection<FileWriteResult>> {
  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final @Nullable Map<String, String> writeProperties;
  private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> writeProperties) {
    this(catalogConfig, dynamicDestinations, filePrefix, writeProperties, null);
  }

  WritePartitionedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      @Nullable Map<String, String> writeProperties,
      @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.writeProperties = writeProperties;
    this.metadataView = metadataView;
  }

  @Override
  public PCollection<FileWriteResult> expand(PCollection<KV<Row, Iterable<Row>>> input) {
    ParDo.SingleOutput<KV<Row, Iterable<Row>>, FileWriteResult> parDo =
        ParDo.of(
            new WriteDoFn(
                catalogConfig, dynamicDestinations, filePrefix, writeProperties, metadataView));
    if (metadataView != null) {
      parDo = parDo.withSideInputs(metadataView);
    }
    return input.apply(parDo);
  }

  private static class WriteDoFn extends DoFn<KV<Row, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final String filePrefix;
    private final @Nullable Map<String, String> writeProperties;
    private final @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView;

    WriteDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        String filePrefix,
        @Nullable Map<String, String> writeProperties,
        @Nullable PCollectionView<Map<String, SerializableTableSpec>> metadataView) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.writeProperties = writeProperties;
      this.metadataView = metadataView;
    }

    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Element KV<Row, Iterable<Row>> element,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputReceiver<FileWriteResult> out)
        throws Exception {
      String tableIdentifier = checkStateNotNull(element.getKey().getString(DESTINATION));

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
              DEFAULT_BYTES_PER_FILE,
              Integer.MAX_VALUE,
              writeProperties,
              sideInputs)) {
        writer = openWriter;
        for (Row row : element.getValue()) {
          writer.write(windowedDestination, row);
        }
      }

      for (SerializableDataFile dataFile :
          checkStateNotNull(writer.getSerializableDataFiles().get(windowedDestination))) {
        out.output(
            FileWriteResult.builder()
                .setTableIdentifier(destination.getTableIdentifier())
                .setSerializableDataFile(dataFile)
                .build());
      }
    }
  }
}

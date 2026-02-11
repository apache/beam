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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.catalog.Catalog;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

class WriteDirectRowsToFiles
    extends PTransform<PCollection<KV<String, Row>>, PCollection<FileWriteResult>> {

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final long maxBytesPerFile;

  WriteDirectRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxBytesPerFile) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.maxBytesPerFile = maxBytesPerFile;
  }

  @Override
  public PCollection<FileWriteResult> expand(PCollection<KV<String, Row>> input) {
    return input.apply(
        ParDo.of(
            new WriteDirectRowsToFilesDoFn(
                catalogConfig, dynamicDestinations, maxBytesPerFile, filePrefix)));
  }

  private static class WriteDirectRowsToFilesDoFn extends DoFn<KV<String, Row>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private transient @MonotonicNonNull Catalog catalog;
    private final String filePrefix;
    private final long maxFileSize;
    private transient @Nullable RecordWriterManager recordWriterManager;

    WriteDirectRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.maxFileSize = maxFileSize;
      this.recordWriterManager = null;
    }

    private org.apache.iceberg.catalog.Catalog getCatalog() {
      if (catalog == null) {
        this.catalog = catalogConfig.catalog();
      }
      return catalog;
    }

    @StartBundle
    public void startBundle() {
      recordWriterManager =
          new RecordWriterManager(getCatalog(), filePrefix, maxFileSize, Integer.MAX_VALUE);
    }

    @ProcessElement
    public void processElement(
        ProcessContext context,
        @Element KV<String, Row> element,
        BoundedWindow window,
        PaneInfo paneInfo)
        throws Exception {
      String tableIdentifier = element.getKey();
      IcebergDestination destination = dynamicDestinations.instantiateDestination(tableIdentifier);
      WindowedValue<IcebergDestination> windowedDestination =
          WindowedValues.of(destination, window.maxTimestamp(), window, paneInfo);
      Preconditions.checkNotNull(recordWriterManager)
          .write(windowedDestination, element.getValue());
    }

    @FinishBundle
    public void finishBundle(FinishBundleContext context) throws Exception {
      if (recordWriterManager == null) {
        return;
      }
      recordWriterManager.close();

      for (Map.Entry<WindowedValue<IcebergDestination>, List<SerializableDataFile>>
          destinationAndFiles :
              Preconditions.checkNotNull(recordWriterManager)
                  .getSerializableDataFiles()
                  .entrySet()) {
        WindowedValue<IcebergDestination> windowedDestination = destinationAndFiles.getKey();

        for (SerializableDataFile dataFile : destinationAndFiles.getValue()) {
          context.output(
              FileWriteResult.builder()
                  .setSerializableDataFile(dataFile)
                  .setTableIdentifier(windowedDestination.getValue().getTableIdentifier())
                  .build(),
              windowedDestination.getTimestamp(),
              Iterables.getFirst(windowedDestination.getWindows(), null));
        }
      }
      recordWriterManager = null;
    }
  }
}

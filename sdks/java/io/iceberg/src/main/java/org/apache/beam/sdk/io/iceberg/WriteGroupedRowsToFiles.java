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
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

class WriteGroupedRowsToFiles
    extends PTransform<
        PCollection<KV<ShardedKey<String>, Iterable<Row>>>, PCollection<FileWriteResult>> {
  private final long maxBytesPerFile;

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;
  private final String filePrefix;
  private final @Nullable Map<String, String> writeProperties;

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig,
      DynamicDestinations dynamicDestinations,
      String filePrefix,
      long maxBytesPerFile,
      @Nullable Map<String, String> writeProperties) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
    this.filePrefix = filePrefix;
    this.maxBytesPerFile = maxBytesPerFile;
    this.writeProperties = writeProperties;
  }

  @Override
  public PCollection<FileWriteResult> expand(
      PCollection<KV<ShardedKey<String>, Iterable<Row>>> input) {
    return input.apply(
        ParDo.of(
            new WriteGroupedRowsToFilesDoFn(
                catalogConfig, dynamicDestinations, maxBytesPerFile, filePrefix, writeProperties)));
  }

  private static class WriteGroupedRowsToFilesDoFn
      extends DoFn<KV<ShardedKey<String>, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private final String filePrefix;
    private final long maxFileSize;
    private final @Nullable Map<String, String> writeProperties;

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize,
        String filePrefix,
        @Nullable Map<String, String> writeProperties) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = filePrefix;
      this.maxFileSize = maxFileSize;
      this.writeProperties = writeProperties;
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
      RecordWriterManager writer;
      try (RecordWriterManager openWriter =
          new RecordWriterManager(
              catalogConfig, filePrefix, maxFileSize, Integer.MAX_VALUE, writeProperties)) {
        writer = openWriter;
        for (Row e : element.getValue()) {
          writer.write(windowedDestination, e);
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

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
import java.util.UUID;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.catalog.Catalog;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

class WriteGroupedRowsToFiles
    extends PTransform<
        PCollection<KV<ShardedKey<Row>, Iterable<Row>>>, PCollection<FileWriteResult>> {

  static final long DEFAULT_MAX_BYTES_PER_FILE = (1L << 40); // 1TB

  private final DynamicDestinations dynamicDestinations;
  private final IcebergCatalogConfig catalogConfig;

  WriteGroupedRowsToFiles(
      IcebergCatalogConfig catalogConfig, DynamicDestinations dynamicDestinations) {
    this.catalogConfig = catalogConfig;
    this.dynamicDestinations = dynamicDestinations;
  }

  @Override
  public PCollection<FileWriteResult> expand(
      PCollection<KV<ShardedKey<Row>, Iterable<Row>>> input) {
    return input.apply(
        ParDo.of(
            new WriteGroupedRowsToFilesDoFn(
                catalogConfig, dynamicDestinations, DEFAULT_MAX_BYTES_PER_FILE)));
  }

  private static class WriteGroupedRowsToFilesDoFn
      extends DoFn<KV<ShardedKey<Row>, Iterable<Row>>, FileWriteResult> {

    private final DynamicDestinations dynamicDestinations;
    private final IcebergCatalogConfig catalogConfig;
    private transient @MonotonicNonNull Catalog catalog;
    private final String filePrefix;
    private final long maxFileSize;

    WriteGroupedRowsToFilesDoFn(
        IcebergCatalogConfig catalogConfig,
        DynamicDestinations dynamicDestinations,
        long maxFileSize) {
      this.catalogConfig = catalogConfig;
      this.dynamicDestinations = dynamicDestinations;
      this.filePrefix = UUID.randomUUID().toString();
      this.maxFileSize = maxFileSize;
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
        @Element KV<ShardedKey<Row>, Iterable<Row>> element,
        BoundedWindow window,
        PaneInfo pane)
        throws Exception {

      Row destMetadata = element.getKey().getKey();
      IcebergDestination destination = dynamicDestinations.instantiateDestination(destMetadata);
      WindowedValue<IcebergDestination> windowedDestination =
          WindowedValue.of(destination, window.maxTimestamp(), window, pane);
      RecordWriterManager writer;
      try (RecordWriterManager openWriter =
          new RecordWriterManager(getCatalog(), filePrefix, maxFileSize, Integer.MAX_VALUE)) {
        writer = openWriter;
        for (Row e : element.getValue()) {
          writer.write(windowedDestination, e);
        }
      }

      List<ManifestFile> manifestFiles =
          Preconditions.checkNotNull(writer.getManifestFiles().get(windowedDestination));
      for (ManifestFile manifestFile : manifestFiles) {
        c.output(
            FileWriteResult.builder()
                .setTableIdentifier(destination.getTableIdentifier())
                .setManifestFile(manifestFile)
                .build());
      }
    }
  }
}

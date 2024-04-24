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

import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

class AppendFilesToTables
    extends PTransform<PCollection<FileWriteResult>, PCollection<KV<String, SnapshotInfo>>> {

  private final IcebergCatalogConfig catalogConfig;

  AppendFilesToTables(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @Override
  public PCollection<KV<String, SnapshotInfo>> expand(PCollection<FileWriteResult> writtenFiles) {

    // Apply any sharded writes and flatten everything for catalog updates
    return writtenFiles
        .apply(
            "Key metadata updates by table",
            WithKeys.of(
                new SerializableFunction<FileWriteResult, String>() {
                  @Override
                  public String apply(FileWriteResult input) {
                    return input.getTableIdentifier().toString();
                  }
                }))
        .apply("Group metadata updates by table", GroupByKey.create())
        .apply(
            "Append metadata updates to tables",
            ParDo.of(new AppendFilesToTablesDoFn(catalogConfig)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SnapshotInfo.CODER));
  }

  private static class AppendFilesToTablesDoFn
      extends DoFn<KV<String, Iterable<FileWriteResult>>, KV<String, SnapshotInfo>> {

    private final IcebergCatalogConfig catalogConfig;

    private transient @MonotonicNonNull Catalog catalog;

    private AppendFilesToTablesDoFn(IcebergCatalogConfig catalogConfig) {
      this.catalogConfig = catalogConfig;
    }

    private Catalog getCatalog() {
      if (catalog == null) {
        catalog = catalogConfig.catalog();
      }
      return catalog;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<FileWriteResult>> element,
        OutputReceiver<KV<String, SnapshotInfo>> out,
        BoundedWindow window) {
      Table table = getCatalog().loadTable(TableIdentifier.parse(element.getKey()));
      AppendFiles update = table.newAppend();
      for (FileWriteResult writtenFile : element.getValue()) {
        update.appendManifest(writtenFile.getManifestFile());
      }
      update.commit();
      out.outputWithTimestamp(
          KV.of(element.getKey(), SnapshotInfo.fromSnapshot(table.currentSnapshot())),
          window.maxTimestamp());
    }
  }
}

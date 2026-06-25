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
package org.apache.beam.sdk.io.iceberg.maintenance;

import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CommitFiles extends DoFn<KV<Void, CoGbkResult>, SnapshotInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(CommitFiles.class);
  private static final Counter numFilesCommitted =
      Metrics.counter(CommitFiles.class, "numFilesCommitted");
  private static final Counter numFilesRemoved =
      Metrics.counter(CommitFiles.class, "numFilesRemoved");
  private final String tableIdentifier;
  private final IcebergCatalogConfig catalogConfig;
  private final TupleTag<SerializableDataFile> newFiles;
  private final TupleTag<SerializableDataFile> oldFiles;

  CommitFiles(
      String tableIdentifier,
      IcebergCatalogConfig catalogConfig,
      TupleTag<SerializableDataFile> newFiles,
      TupleTag<SerializableDataFile> oldFiles) {
    this.tableIdentifier = tableIdentifier;
    this.catalogConfig = catalogConfig;
    this.newFiles = newFiles;
    this.oldFiles = oldFiles;
  }

  @ProcessElement
  public void process(@Element KV<Void, CoGbkResult> element, OutputReceiver<SnapshotInfo> output) {
    Table table = catalogConfig.catalog().loadTable(TableIdentifier.parse(tableIdentifier));

    Iterable<SerializableDataFile> newSerializedFiles = element.getValue().getAll(newFiles);
    Iterable<SerializableDataFile> oldSerializedFiles = element.getValue().getAll(oldFiles);

    if (!newSerializedFiles.iterator().hasNext()) {
      Preconditions.checkState(
          !oldSerializedFiles.iterator().hasNext(),
          "No new files were added, so expected no files planned for deletion, "
              + "but received %s files planned for deletion.",
          Iterables.size(oldSerializedFiles));
      LOG.info(RewriteDataFiles.REWRITE_PREFIX + "Received no rewritten files. Skipping commit.");
      return;
    }

    RewriteFiles rewriteFiles = table.newRewrite();

    int numAddFiles = 0;
    for (SerializableDataFile newFile : newSerializedFiles) {
      DataFile f = newFile.createDataFile(table.specs());
      rewriteFiles.addFile(f);
      numAddFiles++;
    }

    int numRemoveFiles = 0;
    for (SerializableDataFile oldFile : oldSerializedFiles) {
      DataFile f = oldFile.createDataFile(table.specs());
      rewriteFiles.deleteFile(f);
      numRemoveFiles++;
    }

    rewriteFiles.commit();
    Snapshot snapshot = table.currentSnapshot();
    numFilesCommitted.inc(numAddFiles);
    numFilesRemoved.inc(numRemoveFiles);
    LOG.info(
        "Committed rewrite and created new snapshot for table '{}': {}", tableIdentifier, snapshot);
    output.output(SnapshotInfo.fromSnapshot(snapshot));
  }
}

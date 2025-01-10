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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AppendFilesToTables
    extends PTransform<PCollection<FileWriteResult>, PCollection<KV<String, SnapshotInfo>>> {
  private static final Logger LOG = LoggerFactory.getLogger(AppendFilesToTables.class);
  private final IcebergCatalogConfig catalogConfig;
  private final String manifestFilePrefix;

  AppendFilesToTables(IcebergCatalogConfig catalogConfig, String manifestFilePrefix) {
    this.catalogConfig = catalogConfig;
    this.manifestFilePrefix = manifestFilePrefix;
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
            ParDo.of(new AppendFilesToTablesDoFn(catalogConfig, manifestFilePrefix)))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), SnapshotInfo.CODER));
  }

  private static class AppendFilesToTablesDoFn
      extends DoFn<KV<String, Iterable<FileWriteResult>>, KV<String, SnapshotInfo>> {
    private final Counter snapshotsCreated =
        Metrics.counter(AppendFilesToTables.class, "snapshotsCreated");
    private final Distribution committedDataFileByteSize =
        Metrics.distribution(RecordWriter.class, "committedDataFileByteSize");
    private final Distribution committedDataFileRecordCount =
        Metrics.distribution(RecordWriter.class, "committedDataFileRecordCount");

    private final IcebergCatalogConfig catalogConfig;
    private final String manifestFilePrefix;

    private transient @MonotonicNonNull Catalog catalog;

    private AppendFilesToTablesDoFn(IcebergCatalogConfig catalogConfig, String manifestFilePrefix) {
      this.catalogConfig = catalogConfig;
      this.manifestFilePrefix = manifestFilePrefix;
    }

    private Catalog getCatalog() {
      if (catalog == null) {
        catalog = catalogConfig.catalog();
      }
      return catalog;
    }

    private boolean containsMultiplePartitionSpecs(Iterable<FileWriteResult> fileWriteResults) {
      int id = fileWriteResults.iterator().next().getSerializableDataFile().getPartitionSpecId();
      for (FileWriteResult result : fileWriteResults) {
        if (id != result.getSerializableDataFile().getPartitionSpecId()) {
          return true;
        }
      }
      return false;
    }

    @ProcessElement
    public void processElement(
        @Element KV<String, Iterable<FileWriteResult>> element,
        OutputReceiver<KV<String, SnapshotInfo>> out,
        BoundedWindow window)
        throws IOException {
      String tableStringIdentifier = element.getKey();
      Iterable<FileWriteResult> fileWriteResults = element.getValue();
      if (!fileWriteResults.iterator().hasNext()) {
        return;
      }

      Table table = getCatalog().loadTable(TableIdentifier.parse(element.getKey()));

      // vast majority of the time, we will simply append data files.
      // in the rare case we get a batch that contains multiple partition specs, we will group
      // data into manifest files and append.
      // note: either way, we must use a single commit operation for atomicity.
      if (containsMultiplePartitionSpecs(fileWriteResults)) {
        appendManifestFiles(table, fileWriteResults);
      } else {
        appendDataFiles(table, fileWriteResults);
      }

      Snapshot snapshot = table.currentSnapshot();
      LOG.info("Created new snapshot for table '{}': {}", tableStringIdentifier, snapshot);
      snapshotsCreated.inc();
      out.outputWithTimestamp(
          KV.of(element.getKey(), SnapshotInfo.fromSnapshot(snapshot)), window.maxTimestamp());
    }

    // This works only when all files are using the same partition spec.
    private void appendDataFiles(Table table, Iterable<FileWriteResult> fileWriteResults) {
      AppendFiles update = table.newAppend();
      for (FileWriteResult result : fileWriteResults) {
        DataFile dataFile = result.getDataFile(table.specs());
        update.appendFile(dataFile);
        committedDataFileByteSize.update(dataFile.fileSizeInBytes());
        committedDataFileRecordCount.update(dataFile.recordCount());
      }
      update.commit();
    }

    // When a user updates their table partition spec during runtime, we can end up with
    // a batch of files where some are written with the old spec and some are written with the new
    // spec.
    // A table commit is limited to a single partition spec.
    // To handle this, we create a manifest file for each partition spec, and group data files
    // accordingly.
    // Afterward, we append all manifests using a single commit operation.
    private void appendManifestFiles(Table table, Iterable<FileWriteResult> fileWriteResults)
        throws IOException {
      String uuid = UUID.randomUUID().toString();
      Map<Integer, PartitionSpec> specs = table.specs();

      Map<Integer, List<DataFile>> dataFilesBySpec = new HashMap<>();
      for (FileWriteResult result : fileWriteResults) {
        DataFile dataFile = result.getDataFile(specs);
        dataFilesBySpec.computeIfAbsent(dataFile.specId(), i -> new ArrayList<>()).add(dataFile);
      }

      AppendFiles update = table.newAppend();
      for (Map.Entry<Integer, List<DataFile>> entry : dataFilesBySpec.entrySet()) {
        int specId = entry.getKey();
        List<DataFile> files = entry.getValue();
        PartitionSpec spec = Preconditions.checkStateNotNull(specs.get(specId));
        ManifestWriter<DataFile> writer =
            createManifestWriter(table.location(), uuid, spec, table.io());
        for (DataFile file : files) {
          writer.add(file);
          committedDataFileByteSize.update(file.fileSizeInBytes());
          committedDataFileRecordCount.update(file.recordCount());
        }
        writer.close();
        update.appendManifest(writer.toManifestFile());
      }
      update.commit();
    }

    private ManifestWriter<DataFile> createManifestWriter(
        String tableLocation, String uuid, PartitionSpec spec, FileIO io) {
      String location =
          FileFormat.AVRO.addExtension(
              String.format(
                  "%s/metadata/%s-%s-%s.manifest",
                  tableLocation, manifestFilePrefix, uuid, spec.specId()));
      OutputFile outputFile = io.newOutputFile(location);
      return ManifestFiles.write(spec, outputFile);
    }
  }
}

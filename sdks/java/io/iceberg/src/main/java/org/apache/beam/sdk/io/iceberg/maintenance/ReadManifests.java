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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Reads Iceberg manifests in parallel and emits a {@link FileInfo} for every live file they
 * reference.
 *
 * <p>This is an internal building block of the table maintenance operations, and is not intended to
 * be applied directly by users. Its input is produced by the maintenance pipeline's metadata
 * planning step. That step runs once per table: it loads current table metadata, selects the
 * snapshots relevant to the operation (for example, the expired or the retained snapshots), and
 * reads their manifest lists. Each manifest becomes one {@link ManifestDescriptor}, created with
 * {@link ManifestDescriptor#from}, which records whether the files it references are reachable.
 * When a manifest's partition spec may no longer be in table metadata by the time it is read, the
 * planning step attaches the spec with {@link ManifestDescriptor#withPartitionSpec}. The output
 * {@link FileInfo}s are then compared against other file sets to find files that can be deleted.
 *
 * <p>Input manifests are redistributed before they are read. Planning produces all of a table's
 * manifests from a single element, and redistributing spreads their reads across workers.
 *
 * <p>Only live entries ({@code ADDED} and {@code EXISTING}) are emitted; entries recording a file
 * deletion are skipped. Every emitted {@link FileInfo} takes its {@link FileInfo#getReachable()
 * reachable} flag from the manifest's descriptor.
 *
 * <p>The output may contain the same path more than once: a file can be referenced by several
 * manifests, and in format v3 several deletion vectors can be stored in one Puffin file. Consumers
 * that need distinct paths must deduplicate.
 *
 * <p>Tables with format version 4 or later are rejected, because their manifests can reference
 * other manifests, which this transform does not follow. The check uses the worker's cached table
 * metadata, so the planning step must also reject such tables using current metadata.
 */
@Internal
public class ReadManifests
    extends PTransform<PCollection<ManifestDescriptor>, PCollection<FileInfo>> {

  static final String MANIFESTS_READ = "manifestsRead";
  static final String MANIFEST_ENTRIES_READ = "manifestEntriesRead";

  /** The highest table format version whose manifests this transform can read. */
  @VisibleForTesting static final int MAX_SUPPORTED_FORMAT_VERSION = 3;

  /** The only manifest entry columns needed to build a {@link FileInfo}. */
  private static final List<String> COLUMNS =
      ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());

  private final IcebergCatalogConfig catalogConfig;

  private ReadManifests(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  /** Creates a transform that loads tables from {@code catalogConfig}. */
  public static ReadManifests of(IcebergCatalogConfig catalogConfig) {
    return new ReadManifests(catalogConfig);
  }

  @Override
  public PCollection<FileInfo> expand(PCollection<ManifestDescriptor> input) {
    return input
        .apply("Redistribute", Redistribute.arbitrarily())
        .apply("Read", ParDo.of(new ReadManifestDoFn(catalogConfig)));
  }

  static class ReadManifestDoFn extends DoFn<ManifestDescriptor, FileInfo> {
    private final IcebergCatalogConfig catalogConfig;
    private final Counter manifestsRead = Metrics.counter(ReadManifests.class, MANIFESTS_READ);
    private final Counter manifestEntriesRead =
        Metrics.counter(ReadManifests.class, MANIFEST_ENTRIES_READ);

    ReadManifestDoFn(IcebergCatalogConfig catalogConfig) {
      this.catalogConfig = catalogConfig;
    }

    @ProcessElement
    public void processElement(
        @Element ManifestDescriptor descriptor, OutputReceiver<FileInfo> output)
        throws IOException {
      String tableIdentifier = descriptor.getTableIdentifier();
      Table table = TableCache.get(catalogConfig, tableIdentifier);
      checkFormatVersion(table, tableIdentifier);
      Map<Integer, PartitionSpec> specs = resolveSpecs(table, descriptor);

      long entries = 0;
      try (CloseableIterable<? extends ContentFile<?>> files =
          open(new ManifestFileAdapter(descriptor), table.io(), specs)) {
        for (ContentFile<?> file : files) {
          output.output(
              FileInfo.of(
                  file.location(),
                  category(file.content(), descriptor),
                  descriptor.getReachable(),
                  tableIdentifier));
          entries++;
        }
      }
      manifestsRead.inc();
      manifestEntriesRead.inc(entries);
    }

    /**
     * Returns partition specs that include the manifest's spec.
     *
     * <p>A spec carried by the descriptor is preferred: it was captured when the manifest was
     * planned, while a spec ID in current table metadata may have been reassigned to a different
     * spec after the original was removed. Otherwise the cached table's specs are used if they
     * include the spec ID, and failing that, the table is refreshed in case the cached copy
     * predates the spec.
     *
     * @throws IllegalStateException if the spec cannot be found
     */
    private Map<Integer, PartitionSpec> resolveSpecs(Table table, ManifestDescriptor descriptor) {
      int specId = descriptor.getPartitionSpecId();
      PartitionSpec carried = descriptor.partitionSpec();
      if (carried != null) {
        return ImmutableMap.of(specId, carried);
      }

      if (table.specs().containsKey(specId)) {
        return table.specs();
      }

      Map<Integer, PartitionSpec> refreshed =
          TableCache.getRefreshed(catalogConfig, descriptor.getTableIdentifier()).specs();
      if (refreshed.containsKey(specId)) {
        return refreshed;
      }

      throw new IllegalStateException(
          String.format(
              "Cannot read manifest %s: partition spec %s is not in the metadata of table %s and "
                  + "was not attached to its descriptor",
              descriptor.getPath(), specId, descriptor.getTableIdentifier()));
    }
  }

  private static void checkFormatVersion(Table table, String tableIdentifier) {
    int formatVersion = TableUtil.formatVersion(table);
    if (formatVersion > MAX_SUPPORTED_FORMAT_VERSION) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot read manifests of table %s: format version %s is not supported. "
                  + "The highest supported format version is %s.",
              tableIdentifier, formatVersion, MAX_SUPPORTED_FORMAT_VERSION));
    }
  }

  private static CloseableIterable<? extends ContentFile<?>> open(
      ManifestFile manifest, FileIO io, Map<Integer, PartitionSpec> specs) {
    switch (manifest.content()) {
      case DATA:
        return ManifestFiles.read(manifest, io, specs).select(COLUMNS);
      case DELETES:
        return ManifestFiles.readDeleteManifest(manifest, io, specs).select(COLUMNS);
    }
    throw new IllegalArgumentException(
        String.format("Unknown content %s of manifest %s", manifest.content(), manifest.path()));
  }

  /**
   * Returns the category of a manifest entry.
   *
   * @throws UnsupportedOperationException if the entry references another manifest (format v4),
   *     which this transform does not follow
   */
  @VisibleForTesting
  static FileCategory category(FileContent content, ManifestDescriptor descriptor) {
    if (content == FileContent.DATA_MANIFEST || content == FileContent.DELETE_MANIFEST) {
      throw new UnsupportedOperationException(
          String.format(
              "Manifest %s of table %s references another manifest, which is not supported",
              descriptor.getPath(), descriptor.getTableIdentifier()));
    }
    return FileCategory.fromContent(content);
  }
}

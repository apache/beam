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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

/**
 * Worker DoFn that reads entries from a manifest file and emits tagged {@link FileInfo} records.
 *
 * <p>Partition specs are resolved from the spec carried on the {@link ManifestFileBean} when
 * present (for manifests whose spec was removed from table metadata), otherwise from the cached
 * table, refreshing the table if the manifest references a spec newer than the cached metadata.
 */
public class ReadManifestDoFn extends DoFn<KV<String, ManifestFileBean>, KV<String, FileInfo>> {

  private static final List<String> PROJECTION =
      ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());

  private final IcebergCatalogConfig catalogConfig;

  public ReadManifestDoFn(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, ManifestFileBean> element, OutputReceiver<KV<String, FileInfo>> out)
      throws IOException {
    String tableIdString = element.getKey();
    ManifestFileBean manifest = element.getValue();

    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableIdString);
    Table table = TableCache.get(catalogConfig, tableId);
    Map<Integer, PartitionSpec> specs = resolveSpecs(table, tableId, manifest);
    FileIO io = table.io();

    ManifestContent content = manifest.content();
    if (content == ManifestContent.DATA) {
      try (CloseableIterable<DataFile> reader =
          ManifestFiles.read(manifest, io, specs).select(PROJECTION)) {
        for (DataFile file : reader) {
          String path = file.path().toString();
          out.output(
              KV.of(path, FileInfo.of(path, FileCategory.DATA, manifest.isValid(), tableIdString)));
        }
      }
    } else if (content == ManifestContent.DELETES) {
      try (CloseableIterable<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, io, specs).select(PROJECTION)) {
        for (DeleteFile file : reader) {
          String path = file.path().toString();
          FileCategory category =
              file.content() == FileContent.POSITION_DELETES
                  ? FileCategory.POSITION_DELETES
                  : FileCategory.EQUALITY_DELETES;
          out.output(KV.of(path, FileInfo.of(path, category, manifest.isValid(), tableIdString)));
        }
      }
    } else {
      throw new IllegalArgumentException("Unsupported manifest content: " + content);
    }
  }

  private Map<Integer, PartitionSpec> resolveSpecs(
      Table table, TableIdentifier tableId, ManifestFileBean manifest) {
    int specId = manifest.partitionSpecId();
    Map<Integer, PartitionSpec> specs = table.specs();
    if (specs.containsKey(specId)) {
      return specs;
    }

    PartitionSpec carriedSpec = manifest.partitionSpec();
    if (carriedSpec != null) {
      Map<Integer, PartitionSpec> merged = new HashMap<>(specs);
      merged.put(specId, carriedSpec);
      return merged;
    }

    // The cached table may predate the spec referenced by this manifest.
    specs = TableCache.getRefreshed(catalogConfig, tableId).specs();
    if (!specs.containsKey(specId)) {
      throw new IllegalStateException(
          String.format(
              "Cannot read manifest %s: partition spec %s not found in table %s",
              manifest.path(), specId, tableId));
    }
    return specs;
  }
}

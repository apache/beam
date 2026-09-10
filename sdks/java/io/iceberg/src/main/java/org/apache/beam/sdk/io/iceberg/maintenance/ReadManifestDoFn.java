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
import java.util.Objects;
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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Worker DoFn that reads entries from a manifest file and emits tagged {@link FileInfo} records.
 */
public class ReadManifestDoFn extends DoFn<KV<String, ManifestFileBean>, KV<String, FileInfo>> {

  private static final List<String> PROJECTION =
      ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());

  private final IcebergCatalogConfig catalogConfig;

  private transient @Nullable String cachedTableIdString;
  private transient @Nullable Table cachedTable;
  private transient @Nullable FileIO cachedIo;
  private transient @Nullable Map<Integer, PartitionSpec> cachedSpecs;

  public ReadManifestDoFn(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void processElement(
      @Element KV<String, ManifestFileBean> element, OutputReceiver<KV<String, FileInfo>> out)
      throws IOException {
    String tableIdString = element.getKey();
    ManifestFileBean manifest = element.getValue();

    Table table;
    FileIO io;
    Map<Integer, PartitionSpec> specs;
    if (Objects.equals(cachedTableIdString, tableIdString)
        && cachedTable != null
        && cachedIo != null
        && cachedSpecs != null) {
      table = cachedTable;
      io = cachedIo;
      specs = cachedSpecs;
    } else {
      table =
          TableCache.getAndRefreshIfStale(
              catalogConfig, IcebergUtils.parseTableIdentifier(tableIdString));
      io = table.io();
      specs = table.specs();
      cachedTableIdString = tableIdString;
      cachedTable = table;
      cachedIo = io;
      cachedSpecs = specs;
    }

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
}

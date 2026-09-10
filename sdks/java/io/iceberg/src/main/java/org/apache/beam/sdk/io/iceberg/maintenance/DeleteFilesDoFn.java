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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker DoFn that deletes batched unreferenced files from storage and emits partial deletion
 * metrics.
 */
public class DeleteFilesDoFn
    extends DoFn<KV<ShardedKey<String>, Iterable<FileInfo>>, ExpireSnapshotsResult> {

  private static final Logger LOG = LoggerFactory.getLogger(DeleteFilesDoFn.class);

  private final IcebergCatalogConfig catalogConfig;
  private final ExpireSnapshots.Configuration config;

  public DeleteFilesDoFn(IcebergCatalogConfig catalogConfig, ExpireSnapshots.Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  @ProcessElement
  public void processElement(
      @Element KV<ShardedKey<String>, Iterable<FileInfo>> element,
      OutputReceiver<ExpireSnapshotsResult> out) {
    String tableIdString = element.getKey().getKey();
    if (tableIdString == null || tableIdString.isEmpty()) {
      return;
    }

    Table table =
        TableCache.getAndRefreshIfStale(
            catalogConfig, IcebergUtils.parseTableIdentifier(tableIdString));
    FileIO io = table.io();

    List<FileInfo> fileList = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    for (FileInfo file : element.getValue()) {
      fileList.add(file);
      paths.add(file.getPath());
    }

    if (paths.isEmpty()) {
      return;
    }

    Set<String> failedPaths = Collections.emptySet();
    if (config.cleanFiles()) {
      failedPaths = deletePaths(io, paths);
      if (!failedPaths.isEmpty()) {
        Metrics.counter(DeleteFilesDoFn.class, "failed_file_deletions").inc(failedPaths.size());
      }
    } else {
      LOG.info(
          ExpireSnapshots.PREFIX
              + "Dry run enabled (cleanFiles=false); skipping physical deletion of {} file(s).",
          paths.size());
    }

    long dataCount = 0;
    long posDeleteCount = 0;
    long eqDeleteCount = 0;
    long manifestCount = 0;
    long manifestListCount = 0;
    long statsCount = 0;

    for (FileInfo file : fileList) {
      if (failedPaths.contains(file.getPath())) {
        continue;
      }
      switch (file.fileCategory()) {
        case DATA:
          dataCount++;
          break;
        case POSITION_DELETES:
          posDeleteCount++;
          break;
        case EQUALITY_DELETES:
          eqDeleteCount++;
          break;
        case MANIFEST:
          manifestCount++;
          break;
        case MANIFEST_LIST:
          manifestListCount++;
          break;
        case STATISTICS:
          statsCount++;
          break;
      }
    }

    out.output(
        ExpireSnapshotsResult.builder()
            .setDeletedDataFilesCount(dataCount)
            .setDeletedPositionDeleteFilesCount(posDeleteCount)
            .setDeletedEqualityDeleteFilesCount(eqDeleteCount)
            .setDeletedManifestsCount(manifestCount)
            .setDeletedManifestListsCount(manifestListCount)
            .setDeletedStatisticsFilesCount(statsCount)
            .build());
  }

  /**
   * Deletes {@code paths} using {@link SupportsBulkOperations} when supported by {@link FileIO},
   * falling back to per-file deletion. Silently ignores {@link NotFoundException}.
   *
   * @return set of paths that failed to be deleted
   */
  static Set<String> deletePaths(FileIO io, List<String> paths) {
    Set<String> failedPaths = new HashSet<>();
    if (paths.isEmpty()) {
      return failedPaths;
    }
    if (io instanceof SupportsBulkOperations) {
      try {
        ((SupportsBulkOperations) io).deleteFiles(paths);
        return failedPaths;
      } catch (BulkDeletionFailureException e) {
        LOG.warn(
            ExpireSnapshots.PREFIX
                + "Bulk delete failed for {} of {} files. Retrying individually.",
            e.numberFailedObjects(),
            paths.size(),
            e);
      } catch (RuntimeException e) {
        LOG.warn(
            ExpireSnapshots.PREFIX
                + "Bulk delete raised non-bulk exception; falling back to per-file deletion.",
            e);
      }
    }

    for (String path : paths) {
      try {
        io.deleteFile(path);
      } catch (NotFoundException e) {
        LOG.debug(
            ExpireSnapshots.PREFIX + "File {} not found during deletion (already removed).", path);
      } catch (Exception e) {
        LOG.warn(ExpireSnapshots.PREFIX + "Failed to delete file {}.", path, e);
        failedPaths.add(path);
      }
    }
    return failedPaths;
  }
}

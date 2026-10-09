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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Execution-time planning DoFn for {@link ExpireSnapshots}.
 *
 * <p>Validates {@code GC_ENABLED}, invokes {@code
 * table.expireSnapshots().cleanExpiredFiles(false).commit()} to atomically prune snapshots from
 * metadata, and emits candidate and valid file descriptors for distributed content resolution.
 */
public class PlanExpireSnapshotsDoFn extends DoFn<String, ExpireSnapshotsResult> {

  private static final Logger LOG = LoggerFactory.getLogger(PlanExpireSnapshotsDoFn.class);

  public static final TupleTag<ExpireSnapshotsResult> PLAN_SUMMARY = new TupleTag<>() {};
  public static final TupleTag<KV<String, ManifestFileBean>> MANIFESTS = new TupleTag<>() {};
  public static final TupleTag<KV<String, FileInfo>> DIRECT_FILES = new TupleTag<>() {};

  private final IcebergCatalogConfig catalogConfig;
  private final ExpireSnapshots.Configuration config;

  public PlanExpireSnapshotsDoFn(
      IcebergCatalogConfig catalogConfig, ExpireSnapshots.Configuration config) {
    this.catalogConfig = catalogConfig;
    this.config = config;
  }

  @ProcessElement
  public void processElement(@Element String tableIdString, MultiOutputReceiver out) {
    TableIdentifier tableId = IcebergUtils.parseTableIdentifier(tableIdString);
    Table table = TableCache.getAndRefreshIfStale(catalogConfig, tableId);

    boolean gcEnabled =
        PropertyUtil.propertyAsBoolean(
            table.properties(), TableProperties.GC_ENABLED, TableProperties.GC_ENABLED_DEFAULT);
    if (!gcEnabled) {
      throw new ValidationException(
          "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
    }

    List<Snapshot> originalSnapshots = Lists.newArrayList(table.snapshots());
    if (originalSnapshots.isEmpty()) {
      LOG.info(
          ExpireSnapshots.PREFIX + "Table '{}' has no snapshots; expiration is a no-op.", tableId);
      out.get(PLAN_SUMMARY).output(ExpireSnapshotsResult.zeros());
      return;
    }

    org.apache.iceberg.ExpireSnapshots expire = table.expireSnapshots();
    if (config.getExpireOlderThan() != null) {
      expire = expire.expireOlderThan(config.getExpireOlderThan());
    }
    if (config.getRetainLast() != null) {
      expire = expire.retainLast(config.retainLast());
    }
    if (config.getSnapshotIds() != null) {
      for (Long id : config.getSnapshotIds()) {
        expire = expire.expireSnapshotId(id);
      }
    }
    if (config.getCleanExpiredMetadata() != null) {
      expire = expire.cleanExpiredMetadata(config.cleanExpiredMetadata());
    }

    List<StatisticsFile> originalStats = Lists.newArrayList(table.statisticsFiles());
    List<PartitionStatisticsFile> originalPartitionStats =
        Lists.newArrayList(table.partitionStatisticsFiles());
    LOG.info(
        ExpireSnapshots.PREFIX
            + "Committing snapshot expiration with cleanExpiredFiles(false) on table '{}'.",
        tableId);
    expire.cleanExpiredFiles(false).commit();

    table.refresh();
    Set<Long> retainedSnapshotIds = new HashSet<>();
    for (Snapshot s : table.snapshots()) {
      retainedSnapshotIds.add(s.snapshotId());
    }

    Set<Long> deletedSnapshotIds = new HashSet<>();
    for (Snapshot s : originalSnapshots) {
      if (!retainedSnapshotIds.contains(s.snapshotId())) {
        deletedSnapshotIds.add(s.snapshotId());
      }
    }

    if (deletedSnapshotIds.isEmpty()) {
      LOG.info(
          ExpireSnapshots.PREFIX + "No snapshots expired for table '{}'; expiration is a no-op.",
          tableId);
      out.get(PLAN_SUMMARY).output(ExpireSnapshotsResult.zeros());
      return;
    }

    LOG.info(
        ExpireSnapshots.PREFIX + "Expired {} snapshot(s) {} from table '{}'.",
        deletedSnapshotIds.size(),
        deletedSnapshotIds,
        tableId);

    out.get(PLAN_SUMMARY)
        .output(
            ExpireSnapshotsResult.builder()
                .setExpiredSnapshotsCount((long) deletedSnapshotIds.size())
                .build());

    // 1. Emit Manifest Lists
    for (Snapshot s : table.snapshots()) {
      if (s.manifestListLocation() != null) {
        String path = s.manifestListLocation();
        out.get(DIRECT_FILES)
            .output(
                KV.of(path, FileInfo.of(path, FileCategory.MANIFEST_LIST, true, tableIdString)));
      }
    }

    for (Snapshot s : originalSnapshots) {
      if (deletedSnapshotIds.contains(s.snapshotId()) && s.manifestListLocation() != null) {
        String path = s.manifestListLocation();
        out.get(DIRECT_FILES)
            .output(
                KV.of(path, FileInfo.of(path, FileCategory.MANIFEST_LIST, false, tableIdString)));
      }
    }

    // 2. Emit Statistics Files
    for (StatisticsFile sf : originalStats) {
      if (retainedSnapshotIds.contains(sf.snapshotId())) {
        out.get(DIRECT_FILES)
            .output(
                KV.of(
                    sf.path(),
                    FileInfo.of(sf.path(), FileCategory.STATISTICS, true, tableIdString)));
      } else if (deletedSnapshotIds.contains(sf.snapshotId())) {
        out.get(DIRECT_FILES)
            .output(
                KV.of(
                    sf.path(),
                    FileInfo.of(sf.path(), FileCategory.STATISTICS, false, tableIdString)));
      }
    }

    for (PartitionStatisticsFile psf : originalPartitionStats) {
      if (retainedSnapshotIds.contains(psf.snapshotId())) {
        out.get(DIRECT_FILES)
            .output(
                KV.of(
                    psf.path(),
                    FileInfo.of(psf.path(), FileCategory.STATISTICS, true, tableIdString)));
      } else if (deletedSnapshotIds.contains(psf.snapshotId())) {
        out.get(DIRECT_FILES)
            .output(
                KV.of(
                    psf.path(),
                    FileInfo.of(psf.path(), FileCategory.STATISTICS, false, tableIdString)));
      }
    }

    // 3. Emit Manifest Files
    Set<String> validManifestPaths = new HashSet<>();
    for (Snapshot s : table.snapshots()) {
      for (ManifestFile m : s.allManifests(table.io())) {
        if (validManifestPaths.add(m.path())) {
          out.get(DIRECT_FILES)
              .output(
                  KV.of(
                      m.path(), FileInfo.of(m.path(), FileCategory.MANIFEST, true, tableIdString)));
          out.get(MANIFESTS)
              .output(KV.of(tableIdString, ManifestFileBean.fromManifestFile(m, true)));
        }
      }
    }

    Set<String> candidateManifestPaths = new HashSet<>();
    for (Snapshot s : originalSnapshots) {
      if (deletedSnapshotIds.contains(s.snapshotId())) {
        for (ManifestFile m : s.allManifests(table.io())) {
          if (candidateManifestPaths.add(m.path())) {
            out.get(DIRECT_FILES)
                .output(
                    KV.of(
                        m.path(),
                        FileInfo.of(m.path(), FileCategory.MANIFEST, false, tableIdString)));
            // If the manifest is already part of a retained snapshot, its files are all valid
            if (!validManifestPaths.contains(m.path())) {
              out.get(MANIFESTS)
                  .output(KV.of(tableIdString, ManifestFileBean.fromManifestFile(m, false)));
            }
          }
        }
      }
    }
  }
}

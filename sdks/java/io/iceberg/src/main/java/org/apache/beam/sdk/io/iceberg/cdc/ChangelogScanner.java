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
package org.apache.beam.sdk.io.iceberg.cdc;

import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.Type.ADDED_ROWS;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChangelogScanner
    extends DoFn<
        KV<String, List<SnapshotInfo>>, KV<ChangelogDescriptor, List<SerializableChangelogTask>>> {
  private static final Logger LOG = LoggerFactory.getLogger(ChangelogScanner.class);
  private static final Counter totalChangelogScanTasks =
      Metrics.counter(ChangelogScanner.class, "totalChangelogScanTasks");
  private static final Counter numAddedRowsScanTasks =
      Metrics.counter(ChangelogScanner.class, "numAddedRowsScanTasks");
  private static final Counter numDeletedRowsScanTasks =
      Metrics.counter(ChangelogScanner.class, "numDeletedRowsScanTasks");
  private static final Counter numDeletedDataFileScanTasks =
      Metrics.counter(ChangelogScanner.class, "numDeletedDataFileScanTasks");
  private static final Counter numUniDirectionalTasks =
      Metrics.counter(ChangelogScanner.class, "numUniDirectionalTasks");
  private static final Counter numBiDirectionalTasks =
      Metrics.counter(ChangelogScanner.class, "numBiDirectionalTasks");
  public static final TupleTag<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>
      UNIDIRECTIONAL_CHANGES = new TupleTag<>();
  public static final TupleTag<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>
      BIDIRECTIONAL_CHANGES = new TupleTag<>();
  public static final KvCoder<ChangelogDescriptor, List<SerializableChangelogTask>> OUTPUT_CODER =
      KvCoder.of(ChangelogDescriptor.coder(), ListCoder.of(SerializableChangelogTask.coder()));
  private final IcebergScanConfig scanConfig;

  ChangelogScanner(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @ProcessElement
  public void process(@Element KV<String, List<SnapshotInfo>> element, MultiOutputReceiver out)
      throws IOException {
    // TODO: use TableCache here
    Table table = scanConfig.getTable();
    table.refresh();

    List<SnapshotInfo> snapshots = element.getValue();
    SnapshotInfo startSnapshot = snapshots.get(0);
    SnapshotInfo endSnapshot = snapshots.get(snapshots.size() - 1);
    @Nullable Long fromSnapshotId = startSnapshot.getParentId();
    long toSnapshot = endSnapshot.getSnapshotId();

    IncrementalChangelogScan scan =
        table
            .newIncrementalChangelogScan()
            .toSnapshot(toSnapshot)
            .project(scanConfig.getProjectedSchema());
    if (fromSnapshotId != null) {
      scan = scan.fromSnapshotExclusive(fromSnapshotId);
    }
    @Nullable Expression filter = scanConfig.getFilter();
    if (filter != null) {
      scan = scan.filter(filter);
    }
    LOG.info("Planning to scan snapshot range [{}, {}]", fromSnapshotId, toSnapshot);

    createAndOutputReadTasks(
        scan, startSnapshot, endSnapshot, SerializableTable.copyOf(table), out);
  }

  private void createAndOutputReadTasks(
      IncrementalChangelogScan scan,
      SnapshotInfo startSnapshot,
      SnapshotInfo endSnapshot,
      Table table,
      MultiOutputReceiver multiOutputReceiver)
      throws IOException {
    int numAddedRowsTasks = 0;
    int numDeletedRowsTasks = 0;
    int numDeletedFileTasks = 0;

    Map<Long, Long> cachedSnapshotTimestamps = new HashMap<>();
    // Best effort maintain the same scan task groupings produced by Iceberg's binpacking, for
    // better work load distribution among readers.
    // This allows the user to control load per worker by tuning `read.split.target-size`:
    // https://iceberg.apache.org/docs/latest/configuration/#read-properties
    Map<Integer, List<List<SerializableChangelogTask>>> changelogScanTasks = new HashMap<>();

    // keep track of the types of changes in each partition. do this for each ordinal
    Map<Integer, Map<String, Set<SerializableChangelogTask.Type>>> partitionChangeTypesPerOrdinal =
        new HashMap<>();

    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> scanTaskGroups = scan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> scanTaskGroup : scanTaskGroups) {
        Map<Integer, List<SerializableChangelogTask>> ordinalTaskGroup = new HashMap<>();

        for (ChangelogScanTask changelogScanTask : scanTaskGroup.tasks()) {
          long snapshotId = changelogScanTask.commitSnapshotId();
          long timestampMillis =
              cachedSnapshotTimestamps.computeIfAbsent(
                  snapshotId, (snapId) -> table.snapshot(snapId).timestampMillis());
          int ordinal = changelogScanTask.changeOrdinal();

          SerializableChangelogTask task =
              SerializableChangelogTask.from(changelogScanTask, timestampMillis);
          String partition = task.getDataFile().getPartitionPath();

          // gather metrics
          switch (task.getType()) {
            case ADDED_ROWS:
              numAddedRowsTasks++;
              break;
            case DELETED_ROWS:
              numDeletedRowsTasks++;
              break;
            case DELETED_FILE:
              numDeletedFileTasks++;
              break;
          }

          partitionChangeTypesPerOrdinal
              .computeIfAbsent(ordinal, (o) -> new HashMap<>())
              .computeIfAbsent(partition, (p) -> new HashSet<>())
              .add(task.getType());

          ordinalTaskGroup.computeIfAbsent(ordinal, (o) -> new ArrayList<>()).add(task);
        }

        for (Map.Entry<Integer, List<SerializableChangelogTask>> ordinalGroup :
            ordinalTaskGroup.entrySet()) {
          changelogScanTasks
              .computeIfAbsent(ordinalGroup.getKey(), (unused) -> new ArrayList<>())
              .add(ordinalGroup.getValue());
        }
      }
    }

    int numUniDirTasks = 0;
    int numBiDirTasks = 0;

    for (Map.Entry<Integer, List<List<SerializableChangelogTask>>> taskGroups :
        changelogScanTasks.entrySet()) {
      int ordinal = taskGroups.getKey();
      ChangelogDescriptor descriptor =
          ChangelogDescriptor.builder()
              .setTableIdentifierString(checkStateNotNull(startSnapshot.getTableIdentifierString()))
              .setStartSnapshotId(startSnapshot.getSnapshotId())
              .setEndSnapshotId(endSnapshot.getSnapshotId())
              .setChangeOrdinal(ordinal)
              .build();

      for (List<SerializableChangelogTask> subgroup : taskGroups.getValue()) {
        Instant timestamp = Instant.ofEpochMilli(subgroup.get(0).getTimestampMillis());

        // Determine where each ordinal's tasks will go, based on the type of changes:
        // 1. If an ordinal's changes are unidirectional (i.e. only inserts or only deletes), they
        // should be processed directly in the fast path.
        // 2. If an ordinal's changes are bidirectional (i.e. both inserts and deletes) within a
        // partition, they will need more careful processing to determine if any updates have
        // occurred.
        Map<String, Set<SerializableChangelogTask.Type>> changeTypesPerPartition =
            checkStateNotNull(partitionChangeTypesPerOrdinal.get(ordinal));
        List<SerializableChangelogTask> uniDirTasks = new ArrayList<>();
        List<SerializableChangelogTask> biDirTasks = new ArrayList<>();
        for (SerializableChangelogTask task : subgroup) {
          Set<SerializableChangelogTask.Type> partitionChangeTypes =
              checkStateNotNull(changeTypesPerPartition.get(task.getDataFile().getPartitionPath()));
          if (containsBiDirectionalChanges(partitionChangeTypes)) {
            biDirTasks.add(task);
          } else {
            uniDirTasks.add(task);
          }
        }

        if (!uniDirTasks.isEmpty()) {
          KV<ChangelogDescriptor, List<SerializableChangelogTask>> uniDirOutput =
              KV.of(descriptor, uniDirTasks);
          multiOutputReceiver
              .get(UNIDIRECTIONAL_CHANGES)
              .outputWithTimestamp(uniDirOutput, timestamp);
          numUniDirTasks += uniDirTasks.size();
        }
        if (!biDirTasks.isEmpty()) {
          KV<ChangelogDescriptor, List<SerializableChangelogTask>> biDirOutput =
              KV.of(descriptor, biDirTasks);
          multiOutputReceiver
              .get(BIDIRECTIONAL_CHANGES)
              .outputWithTimestamp(biDirOutput, timestamp);
          numBiDirTasks += biDirTasks.size();
        }
      }
    }

    int totalTasks = numAddedRowsTasks + numDeletedRowsTasks + numDeletedFileTasks;
    totalChangelogScanTasks.inc(totalTasks);
    numAddedRowsScanTasks.inc(numAddedRowsTasks);
    numDeletedRowsScanTasks.inc(numDeletedRowsTasks);
    numDeletedDataFileScanTasks.inc(numDeletedFileTasks);
    numUniDirectionalTasks.inc(numUniDirTasks);
    numBiDirectionalTasks.inc(numBiDirTasks);

    LOG.info(
        "Snapshots [{}, {}] produced {} tasks:\n\t{} AddedRowsScanTasks\n\t{} DeletedRowsScanTasks\n\t{} DeletedDataFileScanTasks\n"
            + "Observed {} uni-directional tasks and {} bi-directional tasks.",
        startSnapshot.getSnapshotId(),
        endSnapshot.getSnapshotId(),
        totalTasks,
        numAddedRowsTasks,
        numDeletedRowsTasks,
        numDeletedFileTasks,
        numUniDirTasks,
        numBiDirTasks);
  }

  private static boolean containsBiDirectionalChanges(
      Set<SerializableChangelogTask.Type> changeTypes) {
    return changeTypes.contains(ADDED_ROWS) && changeTypes.size() > 1;
  }
}

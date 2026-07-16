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

import static java.lang.String.format;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.Type.ADDED_ROWS;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.getDataFile;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.getLength;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.getPartition;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.getSpec;
import static org.apache.beam.sdk.io.iceberg.cdc.SerializableChangelogTask.getType;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.BeamBaseIncrementalChangelogScan;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn that takes incoming Iceberg snapshots and scans them for changelogs using Iceberg's {@link
 * IncrementalChangelogScan}. Changelog tasks are organized into batches and routed to different
 * downstream PCollections based on complexity.
 *
 * <p>The Iceberg scan generates batches of changelog scan tasks, each of size {@link
 * TableProperties#SPLIT_SIZE}. This can be configured with the table's <a
 * href="https://iceberg.apache.org/docs/latest/configuration/#read-properties">read.split.target-size
 * property</a>.
 *
 * <p>This DoFn analyzes the nature of changes within the snapshot, partition, and file level, then
 * routes the changes accordingly:
 *
 * <ol>
 *   <li><b>Unidirectional (Fast Path):</b> If an isolated level contains only inserts OR only
 *       deletes, its tasks are emitted to {@link #UNIDIRECTIONAL_TASKS}. These records <b>bypass
 *       the CoGBK shuffle</b> and are output immediately.
 *   <li><b>Small Bidirectional (Medium Path):</b> If an isolated level contains a mix of inserts
 *       and deletes, and is small enough, its tasks are emitted to {@link
 *       #SMALL_BIDIRECTIONAL_TASKS}. These records are resolved in memory to identify potential
 *       updates. Task groups are considered small enough if the estimated overlap region is within
 *       {@link TableProperties#SPLIT_SIZE}.
 *   <li><b>Bidirectional (Slow Path):</b> If an isolated level contains a mix of inserts and
 *       deletes, and is too large, its tasks are emitted to {@link #LARGE_BIDIRECTIONAL_TASKS}.
 *       These records are grouped by Primary Key and processed by {@link ResolveChanges} to
 *       identify potential updates.
 * </ol>
 *
 * <h2>Optimizing by Shuffling Less Data</h2>
 *
 * <p>We take a three-layered approach to identify data that can bypass the expensive downstream
 * CoGroupByKey shuffle:
 *
 * <h3>Snapshots</h3>
 *
 * We start by analyzing the nature of changes at the snapshot level. If a snapshot's operation is
 * not of type {@link DataOperations#OVERWRITE}, then it's a uni-directional change.
 *
 * <h3>Pinned Partitions</h3>
 *
 * <p>If the table's partition fields are derived entirely from Primary Key fields, we know that a
 * record will not migrate between partitions. This narrows down the isolated level and allows us to
 * only check for bi-directional changes <b>within a partition</b>. Doing this will allow partitions
 * with uni-directional changes to bypass the expensive CoGBK shuffle. It also gives partitions with
 * small bi-directional changes a chance to be processed in-memory instead of needing to pass
 * through the CoGBK.
 *
 * <h3>Optimization for Individual Files</h3>
 *
 * When we have narrowed down our group of tasks with bi-directional changes, we start analyzing the
 * metadata of their underlying files. We compare the upper and lower bounds of Partition Keys
 * relevant to each file, and consider any overlaps as potentially containing an update. If a given
 * task's Primary Key bounds has no overlap with any opposing task's Primary Key bounds, then we
 * know it's not possible to create an (insert, delete) pair with it. Such a task can safely bypass
 * the shuffle.
 *
 * <p>Note: "opposing" refers to a change that happens in the opposite direction (e.g. insert is
 * "positive", delete is "negative")
 *
 * <p>For example, say we have a group of tasks:
 *
 * <ol>
 *   <li>Task A (adds rows): bounds [3, 8]
 *   <li>Task B (adds rows): bounds [2, 4]
 *   <li>Task C (deletes rows): bounds [1, 5]
 *   <li>Task D (adds rows): bounds [6, 12]
 * </ol>
 *
 * <p>Tasks A and B add rows, and overlap with Task C which deletes row. We need to resolve the rows
 * in these 3 tasks because they might all contain (insert, delete) pairs that lead to an update.
 *
 * <p>Task D however, does not overlap with any delete rows. It will never produce an (insert,
 * delete) pair, so we can directly emit it without resolving its output rows.
 */
class ChangelogScanner
    extends DoFn<Long, KV<ChangelogDescriptor, List<SerializableChangelogTask>>> {
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
  private static final Counter numLargeBiDirectionalTasks =
      Metrics.counter(ChangelogScanner.class, "numLargeBiDirectionalTasks");
  private static final Counter numSmallBiDirectionalTasks =
      Metrics.counter(ChangelogScanner.class, "numSmallBiDirectionalTasks");
  static final TupleTag<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>
      UNIDIRECTIONAL_TASKS = new TupleTag<>();
  static final TupleTag<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>
      SMALL_BIDIRECTIONAL_TASKS = new TupleTag<>();
  static final TupleTag<KV<ChangelogDescriptor, List<SerializableChangelogTask>>>
      LARGE_BIDIRECTIONAL_TASKS = new TupleTag<>();

  private final IcebergScanConfig scanConfig;
  private boolean canDoPartitionOptimization = false;
  // for metrics
  private int numAddedRowsTasks = 0;
  private int numDeletedRowsTasks = 0;
  private int numDeletedFileTasks = 0;
  private int numUniDirTasks = 0;
  private int numSmallBiDirTasks = 0;
  private int numLargeBiDirTasks = 0;
  private int numUniDirSplits = 0;
  private int numSmallBiDirSplits = 0;
  private int numLargeBiDirSplits = 0;

  ChangelogScanner(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  static KvCoder<ChangelogDescriptor, List<SerializableChangelogTask>> coder(
      org.apache.beam.sdk.schemas.Schema rowIdBeamSchema) {
    return KvCoder.of(
        ChangelogDescriptor.coder(rowIdBeamSchema),
        ListCoder.of(SerializableChangelogTask.coder()));
  }

  @ProcessElement
  public void process(@Element Long snapshotId, MultiOutputReceiver out) throws IOException {
    resetLocalMetrics();
    // upstream Watch should have already refreshed the table
    Table table =
        TableCache.getAndRefreshIfStale(
            scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());
    @Nullable Snapshot snapshot = table.snapshot(snapshotId);

    // refresh anyway on miss
    if (snapshot == null) {
      table =
          TableCache.getRefreshed(scanConfig.getCatalogConfig(), scanConfig.getTableIdentifier());
      snapshot =
          checkStateNotNull(
              table.snapshot(snapshotId), "Could not retrieve table snapshot: %s", snapshotId);
    }

    @Nullable Long fromSnapshotId = snapshot.parentId();
    @Nullable Expression filter = scanConfig.getFilter();

    // TODO(ahmedabu98): replace this with table.newIncrementalChangelogScan() when
    //  https://github.com/apache/iceberg/pull/14264/ gets merged and released.
    IncrementalChangelogScan scan =
        new BeamBaseIncrementalChangelogScan(table)
            .toSnapshot(snapshotId)
            .project(scanConfig.getProjectedSchema());
    if (fromSnapshotId != null) {
      scan = scan.fromSnapshotExclusive(fromSnapshotId);
    }
    if (filter != null) {
      scan = scan.filter(filter);
    }

    // configure the scan to store upper/lower bound metrics only
    // if it's available for primary key fields
    if (metricsAvailableForIdentifierFields(table)) {
      scan = scan.includeColumnStats(table.schema().identifierFieldNames());
    }

    createAndOutputReadTasks(table, snapshot, scan, out);
  }

  private boolean metricsAvailableForIdentifierFields(Table table) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Collection<String> pkFields = table.schema().identifierFieldNames();
    for (String field : pkFields) {
      MetricsModes.MetricsMode mode = metricsConfig.columnMode(field);
      if (!(mode instanceof MetricsModes.Full) && !(mode instanceof MetricsModes.Truncate)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("Slf4jFormatShouldBeConst")
  private void createAndOutputReadTasks(
      Table table,
      Snapshot snapshot,
      IncrementalChangelogScan scan,
      MultiOutputReceiver multiOutputReceiver)
      throws IOException {

    // ******** Partition Optimization ********
    // Determine which partition specs "pin" records to their partition
    // (i.e. partition fields are sourced entirely from a record's PK).
    // If records are pinned, we can optimize by only shuffling bi-directional changes
    // *within* a partition, since no cross-partition changes will occur.
    Set<Integer> pinnedSpecs =
        table.specs().entrySet().stream()
            .filter(e -> doesSpecPinRecordsToPartition(e.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    boolean tableHasPinnedSpecs = !pinnedSpecs.isEmpty();

    // The optimization cannot apply if any file in this snapshot uses an unpinned spec
    boolean snapshotHasUnpinnedSpec = false;
    Set<Integer> specsInSnapshot = new HashSet<>();
    ChangeTypesInPartition changeTypesInPartition = new ChangeTypesInPartition();

    // Buffer tasks from OVERWRITE snapshots, because they are potentially bi-directional
    OverwriteTasks overwriteTasks = new OverwriteTasks();

    // batcher for uni-directional tasks, which can be directly emitted when splitSize is reached
    TaskBatcher uniBatcher =
        new TaskBatcher(
            scanConfig.getTableIdentifier(),
            snapshot.timestampMillis(),
            splitSize(table),
            multiOutputReceiver.get(UNIDIRECTIONAL_TASKS));

    // === collect partition metadata and route/buffer tasks ===
    LOG.info(
        "Planning to scan snapshot {} (seq: {})", snapshot.snapshotId(), snapshot.sequenceNumber());
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> scanTaskGroups = scan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> scanTaskGroup : scanTaskGroups) {
        for (ChangelogScanTask task : scanTaskGroup.tasks()) {
          SerializableChangelogTask.Type type = getType(task);
          StructLike partition = getPartition(task);
          PartitionSpec spec = getSpec(task);
          gatherTaskTypeMetrics(type);

          // Collect partition metadata for pinned-spec optimization
          if (tableHasPinnedSpecs) {
            if (!pinnedSpecs.contains(spec.specId())) {
              snapshotHasUnpinnedSpec = true;
            } else {
              changeTypesInPartition.add(spec, partition, type);
              specsInSnapshot.add(spec.specId());
            }
          }

          // non-overwrite tasks are always unidirectional (the scan planner
          // skips REPLACE ops).
          if (!DataOperations.OVERWRITE.equals(snapshot.operation())) {
            uniBatcher.add(makeTask(task, table), snapshot.sequenceNumber(), getLength(task));
            numUniDirTasks++;
            continue;
          }

          // Overwrite tasks need further analysis — buffer for post-loop processing
          overwriteTasks.add(spec, partition, task);
        }
      }
    }
    // a snapshot using multiple specs is also not safe for the partition optimization,
    // unless we account for the spec ID in the file-to-file comparison, which complicates things
    canDoPartitionOptimization =
        tableHasPinnedSpecs && !snapshotHasUnpinnedSpec && specsInSnapshot.size() <= 1;

    // === analyze buffered overwrite tasks using the partition metadata ===
    processOverwriteTasks(
        table, snapshot, overwriteTasks, changeTypesInPartition, uniBatcher, multiOutputReceiver);
    uniBatcher.flush();

    numUniDirSplits = uniBatcher.totalSplits;
    int totalTasks = numAddedRowsTasks + numDeletedRowsTasks + numDeletedFileTasks;
    updateTaskCounters();

    LOG.info(scanResultMessage(snapshot, totalTasks));
  }

  private void processOverwriteTasks(
      Table table,
      Snapshot snapshot,
      OverwriteTasks overwriteTasks,
      ChangeTypesInPartition changeTypesInPartition,
      TaskBatcher uniBatcher,
      MultiOutputReceiver multiOutputReceiver) {
    if (overwriteTasks.isEmpty()) {
      return;
    }
    boolean metricsAreAvailable = metricsAvailableForIdentifierFields(table);
    TaskBatcher largeBiBatcher =
        new TaskBatcher(
            scanConfig.getTableIdentifier(),
            snapshot.timestampMillis(),
            splitSize(table),
            multiOutputReceiver.get(LARGE_BIDIRECTIONAL_TASKS));

    if (!canDoPartitionOptimization) {
      // Records are not pinned to partition (or no pinned specs at all).
      // We need to compare underlying files across the whole snapshot.
      List<ChangelogScanTask> tasks = overwriteTasks.allTasks();

      AnalysisResult result =
          analyzeFiles(
              metricsAreAvailable,
              tasks,
              scanConfig.recordIdSchema(),
              scanConfig.recordIdComparator());

      uniBatcher.add(result.unidirectional, snapshot.sequenceNumber(), table);
      numUniDirTasks += result.unidirectional.size();

      routeBidirectional(table, snapshot, result, largeBiBatcher, multiOutputReceiver);
    } else {
      // Records are pinned to partition.
      // Narrow down by comparing the files within each partition independently.
      for (Map.Entry<Integer, StructLikeMap<List<ChangelogScanTask>>> tasksPerSpec :
          overwriteTasks.tasks.entrySet()) {
        int specId = tasksPerSpec.getKey();
        for (Map.Entry<StructLike, List<ChangelogScanTask>> tasksInPartition :
            tasksPerSpec.getValue().entrySet()) {
          StructLike partition = tasksInPartition.getKey();
          @Nullable
          Set<SerializableChangelogTask.Type> partitionChangeTypes =
              changeTypesInPartition.typesFor(specId, partition);

          // If this partition has only uni-directional changes, output to UNIDIRECTIONAL and bypass
          // file analysis
          if (partitionChangeTypes != null && !containsBiDirectionalChanges(partitionChangeTypes)) {
            uniBatcher.add(tasksInPartition.getValue(), snapshot.sequenceNumber(), table);
            numUniDirTasks += tasksInPartition.getValue().size();
            continue;
          }

          // Partition has bi-directional changes — analyze file-level overlaps
          AnalysisResult result =
              analyzeFiles(
                  metricsAreAvailable,
                  tasksInPartition.getValue(),
                  scanConfig.recordIdSchema(),
                  scanConfig.recordIdComparator());

          uniBatcher.add(result.unidirectional, snapshot.sequenceNumber(), table);
          routeBidirectional(table, snapshot, result, largeBiBatcher, multiOutputReceiver);

          // metrics
          numUniDirTasks += result.unidirectional.size();
          numLargeBiDirTasks += result.bidirectional.size();
        }
      }
    }
    largeBiBatcher.flush();
    numLargeBiDirSplits = largeBiBatcher.totalSplits;
  }

  /**
   * Helper class for storing + processing {@link ChangelogScanTask}s organized by partition and
   * spec ID.
   */
  static class OverwriteTasks {
    Map<Integer, StructLikeMap<List<ChangelogScanTask>>> tasks = new HashMap<>();

    void add(PartitionSpec spec, StructLike partition, ChangelogScanTask task) {
      tasks
          .computeIfAbsent(spec.specId(), id -> StructLikeMap.create(spec.partitionType()))
          .computeIfAbsent(partition, p -> new ArrayList<>())
          .add(task);
    }

    boolean isEmpty() {
      return tasks.isEmpty();
    }

    List<ChangelogScanTask> allTasks() {
      return tasks.values().stream()
          .flatMap(taskMap -> taskMap.values().stream())
          .flatMap(List::stream)
          .collect(Collectors.toList());
    }
  }

  /**
   * Helper class for identifying types of {@link ChangelogScanTask} per spec ID and partition. This
   * is used to determine whether this snapshot is eligible for partition optimization.
   */
  static class ChangeTypesInPartition {
    Map<Integer, StructLikeMap<Set<SerializableChangelogTask.Type>>> changeTypesPerPartition =
        new HashMap<>();

    void add(PartitionSpec spec, StructLike partition, SerializableChangelogTask.Type type) {
      changeTypesPerPartition
          .computeIfAbsent(spec.specId(), id -> StructLikeMap.create(spec.partitionType()))
          .computeIfAbsent(partition, p -> new HashSet<>())
          .add(type);
    }

    @Nullable
    Set<SerializableChangelogTask.Type> typesFor(Integer specId, StructLike partition) {
      if (!changeTypesPerPartition.containsKey(specId)) {
        return null;
      }
      return checkStateNotNull(changeTypesPerPartition.get(specId)).get(partition);
    }
  }

  /** Checks if a set of change types include both inserts and deletes. */
  private static boolean containsBiDirectionalChanges(
      Set<SerializableChangelogTask.Type> changeTypes) {
    return changeTypes.contains(ADDED_ROWS) && changeTypes.size() > 1;
  }

  /** Helper class for analyzing overlaps between opposing tasks. */
  static class AnalysisResult {
    final List<ChangelogScanTask> unidirectional;
    final List<ChangelogScanTask> bidirectional;
    final @Nullable StructLike overlapLower;
    final @Nullable StructLike overlapUpper;

    AnalysisResult(
        List<ChangelogScanTask> unidirectional,
        List<ChangelogScanTask> bidirectional,
        @Nullable StructLike overlapLower,
        @Nullable StructLike overlapUpper) {
      this.unidirectional = unidirectional;
      this.bidirectional = bidirectional;
      this.overlapLower = overlapLower;
      this.overlapUpper = overlapUpper;
    }

    @Nullable
    Row overlapLowerRow(org.apache.beam.sdk.schemas.Schema idSchema) {
      return this.overlapLower == null
          ? null
          : IcebergUtils.icebergRecordToBeamRow(idSchema, (Record) this.overlapLower);
    }

    @Nullable
    Row overlapUpperRow(org.apache.beam.sdk.schemas.Schema idSchema) {
      return this.overlapUpper == null
          ? null
          : IcebergUtils.icebergRecordToBeamRow(idSchema, (Record) this.overlapUpper);
    }

    static AnalysisResult allBidirectional(List<ChangelogScanTask> tasks) {
      return new AnalysisResult(Collections.emptyList(), tasks, null, null);
    }
  }

  /**
   * Analyzes all tasks in the given list by comparing the bounds of each task's underlying files.
   * If a task's partition key bounds overlap with an opposing task's partition key bounds, they are
   * both considered bi-directional changes. If a task's bounds do not overlap with any opposing
   * task's bounds, it is considered a uni-directional change.
   */
  static AnalysisResult analyzeFiles(
      boolean metricsAreAvailable,
      List<ChangelogScanTask> tasks,
      Schema recIdSchema,
      Comparator<StructLike> idComp) {
    // if table doesn't keep track of metrics, we need to play it safe and consider all tasks may
    // overlap.
    if (!metricsAreAvailable) {
      return AnalysisResult.allBidirectional(tasks);
    }

    List<TaskAndBounds> insertTasks = new ArrayList<>();
    List<TaskAndBounds> deleteTasks = new ArrayList<>();

    try {
      for (ChangelogScanTask task : tasks) {
        if (task instanceof AddedRowsScanTask) {
          insertTasks.add(TaskAndBounds.of(task, recIdSchema, idComp));
        } else if (task instanceof DeletedDataFileScanTask || task instanceof DeletedRowsScanTask) {
          deleteTasks.add(TaskAndBounds.of(task, recIdSchema, idComp));
        } else {
          throw new IllegalStateException("Unknown ChangelogScanTask type: " + task.getClass());
        }
      }
    } catch (TaskAndBounds.NoBoundMetricsException e) {
      // if metrics are not available for some files, we should also play it safe.
      return AnalysisResult.allBidirectional(tasks);
    }

    if (!insertTasks.isEmpty() && !deleteTasks.isEmpty()) {
      Comparator<TaskAndBounds> lowerBoundComp = (t1, t2) -> idComp.compare(t1.lowerId, t2.lowerId);
      Comparator<TaskAndBounds> upperBoundComp = (t1, t2) -> idComp.compare(t1.upperId, t2.upperId);

      insertTasks.sort(lowerBoundComp);
      deleteTasks.sort(lowerBoundComp);

      TaskAndBounds firstInsert = insertTasks.get(0);
      TaskAndBounds firstDelete = deleteTasks.get(0);
      TaskAndBounds lastInsert = insertTasks.stream().max(upperBoundComp).orElseThrow();
      TaskAndBounds lastDelete = deleteTasks.stream().max(upperBoundComp).orElseThrow();

      boolean overlapExists =
          idComp.compare(firstDelete.lowerId, lastInsert.upperId) <= 0
              && idComp.compare(firstInsert.lowerId, lastDelete.upperId) <= 0;

      if (overlapExists) {
        // Iterate through inserts and only check relevant deletes
        for (TaskAndBounds insert : insertTasks) {
          // First check if the insert task overlaps with the global delete window.
          // If not, we can just skip it.
          if (idComp.compare(insert.upperId, firstDelete.lowerId) < 0
              || idComp.compare(insert.lowerId, lastDelete.upperId) > 0) {
            continue;
          }

          for (TaskAndBounds del : deleteTasks) {
            // if the delete task's lower bound is already past the insert task's upper bound,
            // no subsequent delete can overlap this insert (because we sorted above).
            // We can break inner loop.
            if (idComp.compare(del.lowerId, insert.upperId) > 0) {
              break;
            }

            del.checkOverlapWith(insert, idComp);
          }
        }
      }
    }

    // collect results and return.
    // overlapping tasks are bidirectional.
    // otherwise they are unidirectional.
    List<ChangelogScanTask> unidirectional = new ArrayList<>();
    List<ChangelogScanTask> bidirectional = new ArrayList<>();

    for (TaskAndBounds taskAndBounds : Iterables.concat(deleteTasks, insertTasks)) {
      if (taskAndBounds.overlaps) {
        bidirectional.add(taskAndBounds.task);
      } else {
        unidirectional.add(taskAndBounds.task);
      }
    }

    StructLike overlapLower = null;
    StructLike overlapUpper = null;
    if (!bidirectional.isEmpty()) {
      StructLike globalInsertLower = null;
      StructLike globalInsertUpper = null;
      for (TaskAndBounds t : insertTasks) {
        if (t.overlaps) {
          if (globalInsertLower == null || idComp.compare(t.lowerId, globalInsertLower) < 0) {
            globalInsertLower = t.lowerId;
          }
          if (globalInsertUpper == null || idComp.compare(t.upperId, globalInsertUpper) > 0) {
            globalInsertUpper = t.upperId;
          }
        }
      }

      StructLike globalDeleteLower = null;
      StructLike globalDeleteUpper = null;
      for (TaskAndBounds t : deleteTasks) {
        if (t.overlaps) {
          if (globalDeleteLower == null || idComp.compare(t.lowerId, globalDeleteLower) < 0) {
            globalDeleteLower = t.lowerId;
          }
          if (globalDeleteUpper == null || idComp.compare(t.upperId, globalDeleteUpper) > 0) {
            globalDeleteUpper = t.upperId;
          }
        }
      }

      if (globalInsertLower == null
          || globalDeleteLower == null
          || globalInsertUpper == null
          || globalDeleteUpper == null) {
        throw new IllegalStateException(
            "Expected at least one overlapping task in bidirectional list");
      }

      overlapLower =
          idComp.compare(globalInsertLower, globalDeleteLower) > 0
              ? globalInsertLower
              : globalDeleteLower;
      overlapUpper =
          idComp.compare(globalInsertUpper, globalDeleteUpper) < 0
              ? globalInsertUpper
              : globalDeleteUpper;
    }

    return new AnalysisResult(unidirectional, bidirectional, overlapLower, overlapUpper);
  }

  /**
   * Routes bi-directional tasks from an {@link AnalysisResult} to either the in-memory local
   * resolve path (when the estimated overlap region fits in one split) or the CoGroupByKey shuffle
   * path otherwise.
   *
   * <p>For LOCAL routing, all bi-directional tasks for this snapshot/partition group are emitted as
   * a batch so that the downstream {@link LocalResolveDoFn} can resolve them together in-memory. //
   * * The total byte size may exceed {@code splitSize}, but the in-memory // * footprint is bounded
   * by the overlap byte estimate (the local resolver still does per-record PK // * routing to avoid
   * buffering records outside the overlap range).
   *
   * <p>Returns the number of tasks routed to LOCAL so the caller can update counters.
   */
  private void routeBidirectional(
      Table table,
      Snapshot snapshot,
      AnalysisResult result,
      TaskBatcher largeBiBatcher,
      MultiOutputReceiver multiOutputReceiver) {

    if (result.bidirectional.isEmpty()) {
      return;
    }

    long totalBytes =
        result.bidirectional.stream().mapToLong(SerializableChangelogTask::getLength).sum();

    @Nullable Row overlapLowerRow = result.overlapLowerRow(scanConfig.rowIdBeamSchema());
    @Nullable Row overlapUpperRow = result.overlapUpperRow(scanConfig.rowIdBeamSchema());
    ChangelogDescriptor descriptor =
        ChangelogDescriptor.builder()
            .setTableIdentifierString(scanConfig.getTableIdentifier())
            .setSnapshotSequenceNumber(snapshot.sequenceNumber())
            .setCommitSnapshotId(snapshot.snapshotId())
            .setOverlapLower(overlapLowerRow)
            .setOverlapUpper(overlapUpperRow)
            .build();

    List<SerializableChangelogTask> serializedTasks =
        result.bidirectional.stream().map(t -> makeTask(t, table)).collect(Collectors.toList());

    // If the batch is small enough, we can route to LOCAL (in-memory) resolver
    if (totalBytes <= splitSize(table)) {
      Instant ts = Instant.ofEpochMilli(snapshot.timestampMillis());
      multiOutputReceiver
          .get(SMALL_BIDIRECTIONAL_TASKS)
          .outputWithTimestamp(KV.of(descriptor, serializedTasks), ts);
      numSmallBiDirTasks += result.bidirectional.size();
      numSmallBiDirSplits++;
      return;
    }

    // If the batch is too big, we need to route to the CoGBK for distributed resolution
    for (SerializableChangelogTask t : serializedTasks) {
      largeBiBatcher.add(descriptor, t, t.getLength());
    }
  }

  private static SerializableChangelogTask makeTask(ChangelogScanTask task, Table table) {
    return SerializableChangelogTask.from(task, table.specs());
  }

  /**
   * Wraps the {@link ChangelogScanTask}, and stores its lower and upper Primary Keys. Identifies
   * overlaps with other tasks by comparing lower and upper keys using Iceberg libraries.
   */
  static class TaskAndBounds {
    ChangelogScanTask task;
    StructLike lowerId;
    StructLike upperId;
    boolean overlaps = false;

    private TaskAndBounds(ChangelogScanTask task, StructLike lowerId, StructLike upperId) {
      this.task = task;
      this.lowerId = lowerId;
      this.upperId = upperId;
    }

    static TaskAndBounds of(
        ChangelogScanTask task, Schema recIdSchema, Comparator<StructLike> idComp)
        throws NoBoundMetricsException {
      @MonotonicNonNull GenericRecord lowerId = null;
      @MonotonicNonNull GenericRecord upperId = null;

      if (task instanceof AddedRowsScanTask || task instanceof DeletedDataFileScanTask) {
        // just store the bounds of the DataFile
        DataFile df = getDataFile(task);
        @Nullable Map<Integer, ByteBuffer> lowerBounds = df.lowerBounds();
        @Nullable Map<Integer, ByteBuffer> upperBounds = df.upperBounds();
        if (lowerBounds == null || upperBounds == null) {
          throw new NoBoundMetricsException(
              format(
                  "Upper and/or lower bounds are missing for %s with DataFile: %s.",
                  task.getClass().getSimpleName(), df.location()));
        }

        lowerId = createRecId(recIdSchema, lowerBounds);
        upperId = createRecId(recIdSchema, upperBounds);
      } else if (task instanceof DeletedRowsScanTask) {
        // iterate over all added DeleteFiles and keep track of only the
        // minimum and maximum bounds over the list
        for (DeleteFile deleteFile : ((DeletedRowsScanTask) task).addedDeletes()) {
          @Nullable Map<Integer, ByteBuffer> lowerDelBounds = deleteFile.lowerBounds();
          @Nullable Map<Integer, ByteBuffer> upperDelBounds = deleteFile.upperBounds();
          if (lowerDelBounds == null || upperDelBounds == null) {
            throw new NoBoundMetricsException(
                format(
                    "Upper and/or lower bounds are missing for %s with "
                        + "DataFile '%s' and DeleteFile '%s'",
                    task.getClass().getSimpleName(),
                    getDataFile(task).location(),
                    deleteFile.location()));
          }

          GenericRecord delFileLower = createRecId(recIdSchema, lowerDelBounds);
          GenericRecord delFileUpper = createRecId(recIdSchema, upperDelBounds);

          if (lowerId == null || idComp.compare(delFileLower, lowerId) < 0) {
            lowerId = delFileLower;
          }
          if (upperId == null || idComp.compare(delFileUpper, upperId) > 0) {
            upperId = delFileUpper;
          }
        }
      } else {
        throw new UnsupportedOperationException(
            "Unsupported task type: " + task.getClass().getSimpleName());
      }

      if (lowerId == null || upperId == null) {
        throw new NoBoundMetricsException(
            format(
                "Could not compute min and/or max bounds for %s with DataFile: %s",
                task.getClass().getSimpleName(), getDataFile(task).location()));
      }
      return new TaskAndBounds(task, lowerId, upperId);
    }

    /**
     * Compares itself with another task. If the bounds overlap, sets {@link #overlaps} to true for
     * both tasks.
     */
    private void checkOverlapWith(TaskAndBounds other, Comparator<StructLike> idComp) {
      if (overlaps && other.overlaps) {
        return;
      }

      int left = idComp.compare(lowerId, other.upperId);
      int right = idComp.compare(other.lowerId, upperId);

      if (left <= 0 && right <= 0) {
        overlaps = true;
        other.overlaps = true;
      }
    }

    private static GenericRecord createRecId(Schema recIdSchema, Map<Integer, ByteBuffer> bounds)
        throws NoBoundMetricsException {
      GenericRecord recId = GenericRecord.create(recIdSchema);

      for (Types.NestedField field : recIdSchema.columns()) {
        int fieldId = field.fieldId();
        Type type = field.type();
        String name = field.name();
        @Nullable ByteBuffer value = bounds.get(fieldId);
        if (value == null) {
          throw new NoBoundMetricsException("Could not fetch metric value for column: " + name);
        }
        Object data = checkStateNotNull(Conversions.fromByteBuffer(type, value));
        recId.setField(name, data);
      }
      return recId;
    }

    static class NoBoundMetricsException extends Exception {
      public NoBoundMetricsException(String msg) {
        super(msg);
      }
    }
  }

  /** Checks if all partition fields are derived from record identifier fields. */
  private static boolean doesSpecPinRecordsToPartition(PartitionSpec spec) {
    Set<Integer> identifierFieldsIds = spec.schema().identifierFieldIds();
    if (spec.isUnpartitioned() || identifierFieldsIds.isEmpty()) {
      return false;
    }

    for (PartitionField field : spec.fields()) {
      if (!identifierFieldsIds.contains(field.sourceId())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Helper class to batch tasks going to the same tagged PCollection.
   *
   * <p>Used to create batches of uni-directional tasks to send to {@link #UNIDIRECTIONAL_TASKS}
   * tag.
   *
   * <p>Also used to create batches of large bi-directional tasks to send to {@link
   * #LARGE_BIDIRECTIONAL_TASKS} tag.
   *
   * <p>A batch is emitted once it reaches {@link #splitSize(Table)}.
   *
   * <p>Note: This is not used by small bi-directional tasks. Instead, they are emitted immediately
   * to {@link #SMALL_BIDIRECTIONAL_TASKS}.
   */
  static class TaskBatcher {
    Map<ChangelogDescriptor, List<SerializableChangelogTask>> tasks = new HashMap<>();
    long byteSize = 0L;
    final long maxSplitSize;
    final String tableIdentifier;
    final Instant timestamp;
    final OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output;
    int totalSplits = 0;

    TaskBatcher(
        String tableIdentifier,
        Long timestampMillis,
        long maxSplitSize,
        OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output) {
      this.tableIdentifier = tableIdentifier;
      this.timestamp = Instant.ofEpochMilli(timestampMillis);
      this.maxSplitSize = maxSplitSize;
      this.output = output;
    }

    boolean canTake(long sizeBytes) {
      return byteSize + sizeBytes <= maxSplitSize;
    }

    void add(List<ChangelogScanTask> tasks, long sequenceNumber, Table table) {
      tasks.forEach(t -> add(makeTask(t, table), sequenceNumber, getLength(t)));
    }

    void add(SerializableChangelogTask task, long snapshotSequenceNumber, long sizeBytes) {
      add(
          ChangelogDescriptor.builder()
              .setTableIdentifierString(tableIdentifier)
              .setSnapshotSequenceNumber(snapshotSequenceNumber)
              .setCommitSnapshotId(task.getCommitSnapshotId())
              .build(),
          task,
          sizeBytes);
    }

    void add(ChangelogDescriptor descriptor, SerializableChangelogTask task, long sizeBytes) {
      if (!canTake(sizeBytes)) {
        flush();
      }
      byteSize += sizeBytes;
      tasks.computeIfAbsent(descriptor, d -> new ArrayList<>()).add(task);
    }

    void flush() {
      if (tasks.isEmpty()) {
        return;
      }

      for (Map.Entry<ChangelogDescriptor, List<SerializableChangelogTask>> entry :
          tasks.entrySet()) {
        ChangelogDescriptor descriptor = entry.getKey();
        List<SerializableChangelogTask> taskList = entry.getValue();
        output.outputWithTimestamp(KV.of(descriptor, taskList), timestamp);
      }

      byteSize = 0;
      tasks = new HashMap<>();
      totalSplits++;
    }
  }

  /**
   * Fetch the desired split size for downstream read DoFn. We do our best to put tasks into groups
   * of that size. This allows the user to control load per worker by tuning <a
   * href="https://iceberg.apache.org/docs/latest/configuration/#read-properties">`read.split.target-size`</a>
   */
  long splitSize(Table table) {
    return PropertyUtil.propertyAsLong(
        table.properties(), TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
  }

  private void resetLocalMetrics() {
    numAddedRowsTasks = 0;
    numDeletedRowsTasks = 0;
    numDeletedFileTasks = 0;
    numUniDirTasks = 0;
    numLargeBiDirTasks = 0;
    numSmallBiDirTasks = 0;
    numUniDirSplits = 0;
    numSmallBiDirSplits = 0;
    numLargeBiDirSplits = 0;
  }

  private void gatherTaskTypeMetrics(SerializableChangelogTask.Type type) {
    switch (type) {
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
  }

  private void updateTaskCounters() {
    int totalTasks = numAddedRowsTasks + numDeletedRowsTasks + numDeletedFileTasks;
    totalChangelogScanTasks.inc(totalTasks);
    numAddedRowsScanTasks.inc(numAddedRowsTasks);
    numDeletedRowsScanTasks.inc(numDeletedRowsTasks);
    numDeletedDataFileScanTasks.inc(numDeletedFileTasks);
    numUniDirectionalTasks.inc(numUniDirTasks);
    numSmallBiDirectionalTasks.inc(numSmallBiDirTasks);
    numLargeBiDirectionalTasks.inc(numLargeBiDirTasks - numSmallBiDirTasks);
  }

  private String scanResultMessage(Snapshot snapshot, int totalTasks) {
    StringBuilder message = new StringBuilder();
    message.append(
        format(
            "Snapshot %s (seq: %s) produced %s changelog tasks.",
            snapshot.snapshotId(), snapshot.sequenceNumber(), totalTasks));
    if (totalTasks > 0) {
      message.append("Emitted:");
      if (numUniDirTasks > 0) {
        message.append(
            format(
                "%n\t%s splits containing %s uni-directional tasks",
                numUniDirSplits, numUniDirTasks));
      }
      if (numSmallBiDirTasks > 0) {
        message.append(
            format(
                "%n\t%s splits containing %s small bi-directional tasks (for local resolution)",
                numSmallBiDirSplits, numSmallBiDirTasks));
      }
      if (numLargeBiDirTasks > 0) {
        message.append(
            format(
                "%n\t%s splits containing %s large bi-directional tasks (to be shuffled)",
                numLargeBiDirSplits, numLargeBiDirTasks));
      }
    }
    return message.toString();
  }
}

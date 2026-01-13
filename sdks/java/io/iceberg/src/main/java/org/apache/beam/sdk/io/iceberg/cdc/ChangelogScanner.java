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
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.io.iceberg.IcebergScanConfig;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.io.iceberg.TableCache;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.*;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DoFn that takes a list of snapshots and scans for changelogs using Iceberg's {@link
 * IncrementalChangelogScan} and routes them to different downstream PCollections based on
 * complexity.
 *
 * <p>The Iceberg scan generates groups of changelog scan tasks, where each task belongs to a
 * specific "ordinal" (a position in the sequence of table snapshots). Task grouping depends on the
 * table's <a
 * href="https://iceberg.apache.org/docs/latest/configuration/#read-properties">split-size
 * property</a>.
 *
 * <p>This DoFn analyzes the nature of changes within each ordinal and routes them to accordingly:
 *
 * <ol>
 *   <li><b>Unidirectional (Fast Path):</b> If an ordinal contains only inserts OR only deletes, its
 *       tasks are emitted to {@link #UNIDIRECTIONAL_CHANGES}. These records <b>bypass the CoGBK
 *       shuffle</b> and are output immediately.
 *   <li><b>Bidirectional (Slow Path):</b> If an ordinal contains a mix of inserts and deletes, its
 *       tasks are emitted to {@link #BIDIRECTIONAL_CHANGES}. These records are grouped by Primary
 *       Key and processed by {@link ResolveChanges} to identify potential updates.
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
 * <p>If the table's partition fields are derived entirely from Primary Key fields, we assume that a
 * record will not migrate between partitions. This narrows down data locality and allows us to only
 * check for bi-directional changes <b>within a partition</b>. Doing this will allow partitions with
 * uni-directional changes to bypass the expensive CoGBK shuffle.
 *
 * <h3>Optimization for Individual Files</h3>
 *
 * When we have narrowed down our group of tasks with bi-directional changes, we start analyzing the
 * metadata of their underlying files. We compare the upper and lower bounds of Partition Keys
 * relevant to each file, and consider any overlaps as potentially containing an update. If a given
 * task's bounds of inserted Partition Keys has no overlap with any other task's bounds of deleted
 * Partition Keys, then we can safely let that task bypass the shuffle, as it would be impossible to
 * create an (insert, delete) pair with it.
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
 * <p>Tasks A and B add rows, and overlap with Task C which deletes row. We need to shuffle the rows
 * in these 3 tasks because there may be (insert, delete) pairs that lead to an update.
 *
 * <p>Task D however, does not overlap with any delete rows. It will never produce an (insert,
 * delete) pair, so we don't need to shuffle it's output rows.
 */
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
  private Map<Long, SnapshotInfo> snapshotMap = new HashMap<>();

  ChangelogScanner(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
    TableCache.setup(scanConfig);
  }

  @StartBundle
  public void start() {
    snapshotMap = new HashMap<>();
  }

  @ProcessElement
  public void process(@Element KV<String, List<SnapshotInfo>> element, MultiOutputReceiver out)
      throws IOException {
    Table table = TableCache.getRefreshed(scanConfig.getTableIdentifier());

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

    // configure the scan to store upper/lower bound metrics only
    // if it's available for primary key fields
    boolean metricsAvailable = true;
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Collection<String> pkFields = table.schema().identifierFieldNames();
    for (String field : pkFields) {
      MetricsModes.MetricsMode mode = metricsConfig.columnMode(field);
      if (!(mode instanceof MetricsModes.Full) && !(mode instanceof MetricsModes.Truncate)) {
        metricsAvailable = false;
        break;
      }
    }
    if (metricsAvailable) {
      scan = scan.includeColumnStats(pkFields);
    }
    if (fromSnapshotId != null) {
      scan = scan.fromSnapshotExclusive(fromSnapshotId);
    }
    @Nullable Expression filter = scanConfig.getFilter();
    if (filter != null) {
      scan = scan.filter(filter);
    }
    LOG.info("Planning to scan snapshot range [{}, {}]", fromSnapshotId, toSnapshot);

    snapshots.forEach(s -> snapshotMap.put(s.getSnapshotId(), s));

    createAndOutputReadTasks(
        element.getKey(),
        element.getValue(),
        scan,
        startSnapshot,
        endSnapshot,
        SerializableTable.copyOf(table),
        out);
  }

  private void gatherPartitionData(
      IncrementalChangelogScan scan,
      Map<Long, Map<StructLike, Set<SerializableChangelogTask.Type>>>
          changeTypesPerPartitionPerSnapshot,
      Set<Integer> pinnedSpecs,
      Set<Long> snapshotsWithUnpinnedSpecs)
      throws IOException {
    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = scan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> group : groups) {
        for (ChangelogScanTask task : group.tasks()) {
          long snapshotId = task.commitSnapshotId();
          int specId = getSpecId(task);

          if (!pinnedSpecs.contains(specId)) {
            snapshotsWithUnpinnedSpecs.add(snapshotId);
            continue;
          }

          SerializableChangelogTask.Type type = SerializableChangelogTask.getType(task);
          StructLike partition = getPartition(task);

          changeTypesPerPartitionPerSnapshot
              .computeIfAbsent(snapshotId, (id) -> new HashMap<>())
              .computeIfAbsent(partition, (p) -> new HashSet<>())
              .add(type);
        }
      }
    }
  }

  private void createAndOutputReadTasks(
      String tableIdentifier,
      List<SnapshotInfo> snapshots,
      IncrementalChangelogScan scan,
      SnapshotInfo startSnapshot,
      SnapshotInfo endSnapshot,
      Table table,
      MultiOutputReceiver multiOutputReceiver)
      throws IOException {
    int numAddedRowsTasks = 0;
    int numDeletedRowsTasks = 0;
    int numDeletedFileTasks = 0;

    // ******** Partition Optimization ********
    // First pass over the scan to get a full picture of the nature of changes per partition, per
    // snapshot.
    // if partition fields are sourced entirely from a record's PK, that record will
    // always be pinned to that partition (so long as the spec doesn't change).
    // we can optimize the scan by only shuffling bi-directional changes *within* a partition.
    // this is safe to do because we can assume no cross-partition changes will occur
    Set<Integer> pinnedSpecs =
        table.specs().entrySet().stream()
            .filter(e -> isRowPinnedToPartition(e.getValue()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    // optimization cannot apply to snapshots that have any file using an unpinned spec
    Set<Long> snapshotsWithUnpinnedSpecs = new HashSet<>();
    Map<Long, Map<StructLike, Set<SerializableChangelogTask.Type>>>
        changeTypesPerPartitionPerSnapshot = new HashMap<>();
    // Don't bother scanning if the table never had a spec where records are pinned to their
    // partitions.
    if (!pinnedSpecs.isEmpty()) {
      gatherPartitionData(
          scan, changeTypesPerPartitionPerSnapshot, pinnedSpecs, snapshotsWithUnpinnedSpecs);
    }

    // Second pass to route uni-directional tasks downstream, and buffer bi-directional tasks for
    // further processing
    Schema schema = table.schema();
    Schema recIdSchema = TypeUtil.select(schema, schema.identifierFieldIds());
    Comparator<StructLike> idComp = Comparators.forType(recIdSchema.asStruct());

    // Best effort maintain the same scan task groupings produced by Iceberg's binpacking, for
    // better work load distribution among readers.
    // This allows the user to control load per worker by tuning `read.split.target-size`:
    // https://iceberg.apache.org/docs/latest/configuration/#read-properties
    Map<Long, Map<StructLike, List<ChangelogScanTask>>> potentiallyBidirectionalTasks =
        new HashMap<>();
    long splitSize =
        PropertyUtil.propertyAsLong(
            table.properties(), TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);

    TaskBatcher batcher =
        new TaskBatcher(
            tableIdentifier, splitSize, multiOutputReceiver.get(UNIDIRECTIONAL_CHANGES));

    int numUniDirTasks = 0;
    int numBiDirTasks = 0;

    try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> scanTaskGroups = scan.planTasks()) {
      for (ScanTaskGroup<ChangelogScanTask> scanTaskGroup : scanTaskGroups) {
        for (ChangelogScanTask task : scanTaskGroup.tasks()) {
          long snapshotId = task.commitSnapshotId();
          String snapshotOperation = checkStateNotNull(snapshotMap.get(snapshotId)).getOperation();
          SerializableChangelogTask.Type type = SerializableChangelogTask.getType(task);
          // gather metrics
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

          StructLike partition = getPartition(task);

          // potentially bi-directional if it's an overwrite snapshot AND:
          // 1. we are not dealing with a spec where rows are pinned to partitions, OR
          // 2. rows are pinned to partitions, but the changes in this partition are still
          // bi-directional
          // in such a case, we buffer the task for more thorough analysis below
          if (DataOperations.OVERWRITE.equals(snapshotOperation)
              && (snapshotsWithUnpinnedSpecs.contains(snapshotId)
                  || containsBiDirectionalChanges(
                      checkStateNotNull(
                          checkStateNotNull(changeTypesPerPartitionPerSnapshot.get(snapshotId))
                              .get(partition))))) {
            // TODO: remove debug printing
            System.out.printf("\tUnidirectional task with partition '%s':\n", partition);
            System.out.printf(
                "\t\t(%s) DF: %s\n",
                task.getClass().getSimpleName(), name(getDataFile(task).location()));
            for (DeleteFile delf : getDeleteFiles(task)) {
              System.out.println("\t\t\tAdded DelF: " + name(delf.location()));
            }
            potentiallyBidirectionalTasks
                .computeIfAbsent(snapshotId, (id) -> new HashMap<>())
                .computeIfAbsent(partition, (p) -> new ArrayList<>())
                .add(task);
            continue;
          }

          // unidirectional. put into batches of appropriate split size and flush to downstream
          // route, bypassing the shuffle
          batcher.add(task);
          numUniDirTasks++;
        }
      }
      // we won't see these snapshots again, so flush what we have before processing the more
      // complex case below
      batcher.flush();
    }

    // pass over the buffered bi-directional files and analyze further
    for (Map.Entry<Long, Map<StructLike, List<ChangelogScanTask>>> tasksInSnapshot :
        potentiallyBidirectionalTasks.entrySet()) {
      long snapshotId = tasksInSnapshot.getKey();
      SnapshotBuffer uniBuffer =
          new SnapshotBuffer(
              tableIdentifier,
              snapshotId,
              splitSize,
              multiOutputReceiver.get(UNIDIRECTIONAL_CHANGES));
      SnapshotBuffer biBuffer =
          new SnapshotBuffer(
              tableIdentifier,
              snapshotId,
              splitSize,
              multiOutputReceiver.get(BIDIRECTIONAL_CHANGES));

      if (snapshotsWithUnpinnedSpecs.contains(snapshotId)) {
        // Records are not pinned to partition
        // We need to compare the underlying files in the whole snapshot
        List<ChangelogScanTask> tasks = new ArrayList<>();
        tasksInSnapshot.getValue().values().forEach(tasks::addAll);
        Pair<List<ChangelogScanTask>, List<ChangelogScanTask>> uniBirTasks =
            analyzeFiles(tasks, schema, idComp);

        uniBirTasks.first().forEach(uniBuffer::add);
        uniBirTasks.second().forEach(biBuffer::add);

        // metrics
        numUniDirTasks += uniBirTasks.first().size();

        numBiDirTasks += uniBirTasks.second().size();
        System.out.println("\t\tUnpinned spec:");
        for (ChangelogScanTask task : tasks) {
          System.out.printf(
              "\t\t\t(%s) DF: %s\n",
              task.getClass().getSimpleName(), name(getDataFile(task).location()));
          for (DeleteFile delf : getDeleteFiles(task)) {
            System.out.println("\t\t\tAdded DelF: " + name(delf.location()));
          }
        }
      } else {
        // Records are pinned to partition
        // We can narrow down by only comparing the underlying files within each partition
        for (Map.Entry<StructLike, List<ChangelogScanTask>> tasksInPartition :
            tasksInSnapshot.getValue().entrySet()) {
          Pair<List<ChangelogScanTask>, List<ChangelogScanTask>> uniBirTasks =
              analyzeFiles(tasksInPartition.getValue(), schema, idComp);

          uniBirTasks.first().forEach(uniBuffer::add);
          uniBirTasks.second().forEach(biBuffer::add);

          // metrics
          numUniDirTasks += uniBirTasks.first().size();
          numBiDirTasks += uniBirTasks.second().size();

          // TODO: remove debug printing
          System.out.printf("\t\tPartition '%s' bidirectional:\n", tasksInPartition.getKey());
          for (ChangelogScanTask task : tasksInPartition.getValue()) {
            System.out.printf(
                "\t\t\t(%s) DF: %s\n",
                task.getClass().getSimpleName(), name(getDataFile(task).location()));
            for (DeleteFile delf : getDeleteFiles(task)) {
              System.out.println("\t\t\tAdded DelF: " + name(delf.location()));
            }
          }
        }
      }

      // we won't see this snapshot again, so flush what we have and continue processing the next
      // snapshot's metadata
      uniBuffer.flush();
      biBuffer.flush();
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

  /** Checks if a set of change types include both inserts and deletes. */
  private static boolean containsBiDirectionalChanges(
      Set<SerializableChangelogTask.Type> changeTypes) {
    return changeTypes.contains(ADDED_ROWS) && changeTypes.size() > 1;
  }

  /**
   * Analyzes all tasks in the given list by comparing the bounds of each task's underlying files.
   * If a task's partition key bounds overlap with an opposing task's partition key bounds, they are
   * both considered bi-directional changes. If a task's bounds do not overlap with any opposing
   * task's bounds, it is considered a uni-directional change.
   *
   * <p>Note: "opposing" refers to a change that happens in the opposite direction (e.g. insert is
   * "positive", delete is "negative")
   */
  private Pair<List<ChangelogScanTask>, List<ChangelogScanTask>> analyzeFiles(
      List<ChangelogScanTask> tasks, Schema schema, Comparator<StructLike> idComp) {
    // separate insert and delete tasks
    List<TaskAndBounds> insertTasks = new ArrayList<>();
    List<TaskAndBounds> deleteTasks = new ArrayList<>();
    try {
      for (ChangelogScanTask task : tasks) {
        if (task instanceof AddedRowsScanTask) {
          insertTasks.add(TaskAndBounds.of(task, schema, idComp));
        } else {
          deleteTasks.add(TaskAndBounds.of(task, schema, idComp));
        }
      }
    } catch (TaskAndBounds.NoBoundMetricsException e) {
      // if metrics are not fully available, we need to play it safe and shuffle all the tasks.
      return Pair.of(Collections.emptyList(), tasks);
    }

    // check for any overlapping delete and insert tasks
    for (TaskAndBounds insertTask : insertTasks) {
      for (TaskAndBounds deleteTask : deleteTasks) {
        deleteTask.checkOverlapWith(insertTask, idComp);
      }
    }

    // collect results and return.
    // overlapping tasks are bidirectional.
    // otherwise they are unidirectional.
    List<ChangelogScanTask> unidirectional = new ArrayList<>();
    List<ChangelogScanTask> bidirectional = new ArrayList<>();

    for (List<TaskAndBounds> boundsList : Arrays.asList(deleteTasks, insertTasks)) {
      for (TaskAndBounds taskAndBounds : boundsList) {
        String msg = "";
        if (taskAndBounds.overlaps) {
          msg +=
              String.format(
                  "overlapping task: (%s, %s)",
                  taskAndBounds.task.commitSnapshotId(),
                  taskAndBounds.task.getClass().getSimpleName());
          bidirectional.add(taskAndBounds.task);
        } else {
          unidirectional.add(taskAndBounds.task);
          msg +=
              String.format(
                  "NON-overlapping task: (%s, %s)",
                  taskAndBounds.task.commitSnapshotId(),
                  taskAndBounds.task.getClass().getSimpleName());
        }
        msg += "\n\tDF: " + name(getDataFile(taskAndBounds.task).location());
        msg += "\n\t\tlower: " + taskAndBounds.lowerId + ", upper: " + taskAndBounds.upperId;
        if (!getDeleteFiles(taskAndBounds.task).isEmpty()) {
          for (DeleteFile df : getDeleteFiles(taskAndBounds.task)) {
            msg += "\n\tAdded DelF: " + name(df.location());
            msg += "\n\t\tlower: " + taskAndBounds.lowerId + ", upper: " + taskAndBounds.upperId;
          }
        }
        System.out.println(msg);
      }
    }
    return Pair.of(unidirectional, bidirectional);
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

    static TaskAndBounds of(ChangelogScanTask task, Schema schema, Comparator<StructLike> idComp)
        throws NoBoundMetricsException {
      Schema recIdSchema = TypeUtil.select(schema, schema.identifierFieldIds());
      GenericRecord wrapper = GenericRecord.create(recIdSchema);
      @MonotonicNonNull GenericRecord lowerId = null;
      @MonotonicNonNull GenericRecord upperId = null;

      if (task instanceof AddedRowsScanTask || task instanceof DeletedDataFileScanTask) {
        // just store the bounds of the DataFile
        DataFile df = getDataFile(task);
        @Nullable Map<Integer, ByteBuffer> lowerBounds = df.lowerBounds();
        @Nullable Map<Integer, ByteBuffer> upperBounds = df.upperBounds();
        if (lowerBounds == null || upperBounds == null) {
          throw new NoBoundMetricsException(
              String.format(
                  "Upper and/or lower bounds are missing for %s with DataFile: %s.",
                  task.getClass().getSimpleName(), name(df.location())));
        }

        lowerId = fillValues(wrapper, schema, lowerBounds);
        upperId = fillValues(wrapper, schema, upperBounds);
      } else if (task instanceof DeletedRowsScanTask) {
        // iterate over all added DeleteFiles and keep track of only the
        // minimum and maximum bounds over the list
        for (DeleteFile deleteFile : getDeleteFiles(task)) {
          @Nullable Map<Integer, ByteBuffer> lowerDelBounds = deleteFile.lowerBounds();
          @Nullable Map<Integer, ByteBuffer> upperDelBounds = deleteFile.upperBounds();
          if (lowerDelBounds == null || upperDelBounds == null) {
            throw new NoBoundMetricsException(
                String.format(
                    "Upper and/or lower bounds are missing for %s with "
                        + "DataFile '%s' and DeleteFile '%s'",
                    task.getClass().getSimpleName(),
                    name(getDataFile(task).location()),
                    name(deleteFile.location())));
          }

          GenericRecord delFileLower = fillValues(wrapper, schema, lowerDelBounds);
          GenericRecord delFileUpper = fillValues(wrapper, schema, upperDelBounds);

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
            String.format(
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

    private static GenericRecord fillValues(
        GenericRecord wrapper, Schema schema, Map<Integer, ByteBuffer> bounds)
        throws NoBoundMetricsException {
      for (Types.NestedField field : schema.columns()) {
        int idx = field.fieldId();
        Type type = field.type();
        String name = field.name();
        if (!schema.identifierFieldIds().contains(idx)) {
          continue;
        }
        @Nullable ByteBuffer value = bounds.get(idx);
        if (value == null) {
          throw new NoBoundMetricsException("Could not fetch metric value for column: " + name);
        }
        Object data = checkStateNotNull(Conversions.fromByteBuffer(type, value));
        wrapper.setField(name, data);
      }
      return wrapper.copy();
    }

    static class NoBoundMetricsException extends NullPointerException {
      public NoBoundMetricsException(String msg) {
        super(msg);
      }
    }
  }

  /** Checks if all partition fields are derived from record identifier fields. */
  private static boolean isRowPinnedToPartition(PartitionSpec spec) {
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

  class TaskBatcher {
    Map<Long, SnapshotBuffer> taskBuffers = new HashMap<>();
    final long maxSplitSize;
    final String tableIdentifier;
    final OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output;

    TaskBatcher(
        String tableIdentifier,
        long maxSplitSize,
        OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output) {
      this.tableIdentifier = tableIdentifier;
      this.maxSplitSize = maxSplitSize;
      this.output = output;
    }

    void add(ChangelogScanTask task) {
      taskBuffers
          .computeIfAbsent(
              task.commitSnapshotId(),
              (id) -> new SnapshotBuffer(tableIdentifier, id, maxSplitSize, output))
          .add(task);
    }

    void flush() {
      taskBuffers.values().forEach(SnapshotBuffer::flush);
    }
  }

  class SnapshotBuffer {
    List<ChangelogScanTask> tasks = new ArrayList<>();
    long byteSize = 0L;
    final long maxSplitSize;
    final String tableIdentifier;
    final long snapshotId;
    final OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output;

    SnapshotBuffer(
        String tableIdentifier,
        long snapshotId,
        long maxSplitSize,
        OutputReceiver<KV<ChangelogDescriptor, List<SerializableChangelogTask>>> output) {
      this.tableIdentifier = tableIdentifier;
      this.snapshotId = snapshotId;
      this.maxSplitSize = maxSplitSize;
      this.output = output;
    }

    boolean canTake(ChangelogScanTask task) {
      return byteSize + task.sizeBytes() <= maxSplitSize;
    }

    void add(ChangelogScanTask task) {
      if (!canTake(task)) {
        flush();
      }
      byteSize += task.sizeBytes();
      tasks.add(task);
    }

    void flush() {
      ChangelogDescriptor descriptor =
          ChangelogDescriptor.builder().setTableIdentifierString(tableIdentifier).build();
      Instant timestamp =
          Instant.ofEpochMilli(checkStateNotNull(snapshotMap.get(snapshotId)).getTimestampMillis());

      List<SerializableChangelogTask> serializableTasks =
          tasks.stream().map(SerializableChangelogTask::from).collect(Collectors.toList());

      output.outputWithTimestamp(KV.of(descriptor, serializableTasks), timestamp);

      byteSize = 0;
      tasks = new ArrayList<>();
    }
  }

  // TODO: remove
  private static DataFile getDataFile(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).file();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).file();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).file();
    }
    throw new IllegalStateException("Unknown task type: " + task.getClass());
  }

  private static List<DeleteFile> getDeleteFiles(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return Collections.emptyList();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).addedDeletes();
    } else if (task instanceof DeletedDataFileScanTask) {
      return Collections.emptyList();
    }
    throw new IllegalStateException("Unknown task type: " + task.getClass());
  }

  private static StructLike getPartition(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).partition();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).partition();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).partition();
    }
    throw new IllegalStateException("Unknown task type: " + task.getClass());
  }

  private static int getSpecId(ChangelogScanTask task) {
    if (task instanceof AddedRowsScanTask) {
      return ((AddedRowsScanTask) task).spec().specId();
    } else if (task instanceof DeletedRowsScanTask) {
      return ((DeletedRowsScanTask) task).spec().specId();
    } else if (task instanceof DeletedDataFileScanTask) {
      return ((DeletedDataFileScanTask) task).spec().specId();
    }
    throw new IllegalStateException("Unknown task type: " + task.getClass());
  }

  static String name(String path) {
    return Iterables.getLast(Splitter.on("-").split(path));
  }
}

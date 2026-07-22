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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.CommitRange;
import io.delta.kernel.CommitRangeBuilder;
import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableManager;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddCDCFile;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A DoFn that reads the Delta log and plans read tasks for Change Data Feed. */
class CreateCDCReadTasksDoFn extends DoFn<String, DeltaCDCReadTask> {
  private static final long MAX_TASK_SIZE_BYTES = 1024L * 1024L * 1024L; // 1 GB
  private final @Nullable Map<String, String> hadoopConfig;
  private final @Nullable Long startVersion;
  private final @Nullable String startTimestamp;
  private final @Nullable Long endVersion;
  private final @Nullable String endTimestamp;

  public CreateCDCReadTasksDoFn(
      @Nullable Map<String, String> hadoopConfig,
      @Nullable Long startVersion,
      @Nullable String startTimestamp,
      @Nullable Long endVersion,
      @Nullable String endTimestamp) {
    this.hadoopConfig = hadoopConfig;
    this.startVersion = startVersion;
    this.startTimestamp = startTimestamp;
    this.endVersion = endVersion;
    this.endTimestamp = endTimestamp;
  }

  @ProcessElement
  public void processElement(@Element String tablePath, OutputReceiver<DeltaCDCReadTask> out)
      throws Exception {
    Configuration conf = new Configuration();
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    Engine engine = DefaultEngine.create(conf);
    Table table = Table.forPath(engine, tablePath);
    TableImpl tableImpl = (TableImpl) table;

    // 1. Resolve starting and ending versions
    long resolvedStartVersion;
    if (startVersion != null) {
      resolvedStartVersion = startVersion;
    } else if (startTimestamp != null) {
      long startMillis = Instant.parse(startTimestamp).toEpochMilli();
      resolvedStartVersion = tableImpl.getVersionAtOrAfterTimestamp(engine, startMillis);
    } else {
      throw new IllegalArgumentException("Starting version or timestamp must be specified.");
    }

    long resolvedEndVersion;
    if (endVersion != null) {
      resolvedEndVersion = endVersion;
    } else if (endTimestamp != null) {
      long endMillis = Instant.parse(endTimestamp).toEpochMilli();
      resolvedEndVersion = tableImpl.getVersionBeforeOrAtTimestamp(engine, endMillis);
    } else {
      resolvedEndVersion = table.getLatestSnapshot(engine).getVersion();
    }

    if (resolvedStartVersion > resolvedEndVersion) {
      throw new IllegalArgumentException(
          String.format(
              "Resolved start version %d is greater than resolved end version %d",
              resolvedStartVersion, resolvedEndVersion));
    }

    // 2. Load snapshot at resolvedEndVersion to get the scanStateRow
    // We use endVersion's schema because it represents the latest schema in the
    // read range
    // which handles schema evolution (older files will just lack new columns).
    Snapshot endSnapshot = table.getSnapshotAsOfVersion(engine, resolvedEndVersion);
    Scan scan = endSnapshot.getScanBuilder().build();
    Row scanState = scan.getScanState(engine);
    SerializableRow serializableScanState = new SerializableRow(scanState);

    // 3. Load snapshot at resolvedStartVersion to initialize the CommitRange
    Snapshot startSnapshot = table.getSnapshotAsOfVersion(engine, resolvedStartVersion);

    CommitRangeBuilder rangeBuilder =
        TableManager.loadCommitRange(
            tablePath, CommitRangeBuilder.CommitBoundary.atVersion(resolvedStartVersion));
    rangeBuilder.withEndBoundary(CommitRangeBuilder.CommitBoundary.atVersion(resolvedEndVersion));
    CommitRange range = rangeBuilder.build(engine);

    // We need both CDC and ADD actions.
    // If a commit version has CDC files, we only read CDC files.
    // If a commit version has no CDC files, we read ADD files (inserts).
    Set<DeltaAction> actionSet = new HashSet<>();
    actionSet.add(DeltaAction.CDC);
    actionSet.add(DeltaAction.ADD);

    // 4. Iterate over commits in the range and group actions by version
    try (CloseableIterator<ColumnarBatch> batchIter =
        range.getActions(engine, startSnapshot, actionSet)) {
      Map<Long, CommitActionsInfo> commitActionsMap = new HashMap<>();

      while (batchIter.hasNext()) {
        ColumnarBatch batch = batchIter.next();
        int versionIdx = batch.getSchema().indexOf("version");
        int timestampIdx = batch.getSchema().indexOf("timestamp");
        int cdcIdx = batch.getSchema().indexOf("cdc");
        int addIdx = batch.getSchema().indexOf("add");

        for (int i = 0; i < batch.getSize(); i++) {
          long version = batch.getColumnVector(versionIdx).getLong(i);
          long timestamp = batch.getColumnVector(timestampIdx).getLong(i);

          CommitActionsInfo info =
              commitActionsMap.computeIfAbsent(
                  version, k -> new CommitActionsInfo(version, timestamp));

          if (cdcIdx >= 0 && !batch.getColumnVector(cdcIdx).isNullAt(i)) {
            Row cdcRow =
                (Row)
                    VectorUtils.getValueAsObject(
                        batch.getColumnVector(cdcIdx),
                        batch.getSchema().at(cdcIdx).getDataType(),
                        i);
            info.cdcRows.add(cdcRow);
          }
          if (addIdx >= 0 && !batch.getColumnVector(addIdx).isNullAt(i)) {
            Row addRow =
                (Row)
                    VectorUtils.getValueAsObject(
                        batch.getColumnVector(addIdx),
                        batch.getSchema().at(addIdx).getDataType(),
                        i);
            AddFile addFile = new AddFile(addRow);
            // Only consider add files that change data (ignore OPTIMIZE etc.)
            if (addFile.getDataChange()) {
              info.addRows.add(addRow);
            }
          }
        }
      }

      // 5. Emit tasks for each version
      List<DeltaCDCReadTask> currentGroup = new ArrayList<>();
      long currentGroupSize = 0L;

      // Sort versions to process them in order
      List<Long> versions = new ArrayList<>(commitActionsMap.keySet());
      Collections.sort(versions);

      for (long version : versions) {
        CommitActionsInfo info = commitActionsMap.get(version);
        if (info == null) {
          throw new IllegalStateException("CommitActionsInfo was not found for version " + version);
        }
        boolean hasCDC = !info.cdcRows.isEmpty();

        List<Row> rowsToProcess = hasCDC ? info.cdcRows : info.addRows;
        boolean isCDC = hasCDC;

        for (Row fileRow : rowsToProcess) {
          String relPath;
          long size;
          Map<String, String> partitionValues;

          if (isCDC) {
            relPath = fileRow.getString(AddCDCFile.FULL_SCHEMA.indexOf("path"));
            size = fileRow.getLong(AddCDCFile.FULL_SCHEMA.indexOf("size"));
            partitionValues =
                VectorUtils.toJavaMap(
                    fileRow.getMap(AddCDCFile.FULL_SCHEMA.indexOf("partitionValues")));
          } else {
            AddFile addFile = new AddFile(fileRow);
            relPath = addFile.getPath();
            size = addFile.getSize();
            partitionValues = VectorUtils.toJavaMap(addFile.getPartitionValues());
          }

          String fullPath = new org.apache.hadoop.fs.Path(tablePath, relPath).toString();
          List<Long> rowGroupSizes = getRowGroupSizes(fullPath, conf);

          DeltaCDCReadTask task =
              new DeltaCDCReadTask(
                  fullPath,
                  size,
                  partitionValues,
                  info.version,
                  info.timestamp,
                  isCDC,
                  rowGroupSizes,
                  serializableScanState);

          if (size >= MAX_TASK_SIZE_BYTES) {
            if (!currentGroup.isEmpty()) {
              emitGroup(currentGroup, out);
              currentGroup = new ArrayList<>();
              currentGroupSize = 0L;
            }
            out.output(task);
          } else {
            if (currentGroupSize + size > MAX_TASK_SIZE_BYTES) {
              emitGroup(currentGroup, out);
              currentGroup = new ArrayList<>();
              currentGroup.add(task);
              currentGroupSize = size;
            } else {
              currentGroup.add(task);
              currentGroupSize += size;
            }
          }
        }
      }

      if (!currentGroup.isEmpty()) {
        emitGroup(currentGroup, out);
      }
    }
  }

  private void emitGroup(List<DeltaCDCReadTask> group, OutputReceiver<DeltaCDCReadTask> out) {
    for (DeltaCDCReadTask task : group) {
      out.output(task);
    }
  }

  private List<Long> getRowGroupSizes(String pathStr, Configuration conf) {
    List<Long> sizes = new ArrayList<>();
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(pathStr);
      org.apache.parquet.hadoop.metadata.ParquetMetadata metadata =
          org.apache.parquet.hadoop.ParquetFileReader.readFooter(
              conf,
              hadoopPath,
              org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER);
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block : metadata.getBlocks()) {
        sizes.add(block.getTotalByteSize());
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to read Parquet footer for " + pathStr, e);
    }
    return sizes;
  }

  private static class CommitActionsInfo {
    final long version;
    final long timestamp;
    final List<Row> cdcRows = new ArrayList<>();
    final List<Row> addRows = new ArrayList<>();

    CommitActionsInfo(long version, long timestamp) {
      this.version = version;
      this.timestamp = timestamp;
    }
  }
}

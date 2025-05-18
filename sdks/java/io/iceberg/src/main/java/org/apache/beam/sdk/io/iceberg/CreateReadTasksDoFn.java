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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scans the given snapshot and creates multiple {@link ReadTask}s. Each task represents a portion
 * of a data file that was appended within the snapshot range.
 */
class CreateReadTasksDoFn
    extends DoFn<KV<String, List<SnapshotInfo>>, KV<ReadTaskDescriptor, ReadTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateReadTasksDoFn.class);
  private static final Counter totalScanTasks =
      Metrics.counter(CreateReadTasksDoFn.class, "totalScanTasks");
  private final IcebergScanConfig scanConfig;

  CreateReadTasksDoFn(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Setup
  public void setup() {
    TableCache.setup(scanConfig);
  }

  @ProcessElement
  public void process(
      @Element KV<String, List<SnapshotInfo>> element,
      OutputReceiver<KV<ReadTaskDescriptor, ReadTask>> out)
      throws IOException, ExecutionException {
    // force refresh because the table must be updated before scanning snapshots
    Table table = TableCache.getRefreshed(element.getKey());

    // scan snapshots individually and assign commit timestamp to files
    for (SnapshotInfo snapshot : element.getValue()) {
      @Nullable Long fromSnapshot = snapshot.getParentId();
      long toSnapshot = snapshot.getSnapshotId();

      if (!DataOperations.APPEND.equals(snapshot.getOperation())) {
        LOG.info(
            "Skipping non-append snapshot of operation '{}'. Sequence number: {}, id: {}",
            snapshot.getOperation(),
            snapshot.getSequenceNumber(),
            snapshot.getSnapshotId());
      }

      LOG.info("Planning to scan snapshot {}", toSnapshot);
      IncrementalAppendScan scan =
          table
              .newIncrementalAppendScan()
              .toSnapshot(toSnapshot)
              .project(scanConfig.getProjectedSchema());
      if (fromSnapshot != null) {
        scan = scan.fromSnapshotExclusive(fromSnapshot);
      }
      @Nullable Expression filter = scanConfig.getFilter();
      if (filter != null) {
        scan = scan.filter(filter);
      }

      createAndOutputReadTasks(scan, snapshot, out);
    }
  }

  private void createAndOutputReadTasks(
      IncrementalAppendScan scan,
      SnapshotInfo snapshot,
      OutputReceiver<KV<ReadTaskDescriptor, ReadTask>> out)
      throws IOException {
    int numTasks = 0;
    try (CloseableIterable<CombinedScanTask> combinedScanTasks = scan.planTasks()) {
      for (CombinedScanTask combinedScanTask : combinedScanTasks) {
        ReadTask task = ReadTask.builder().setCombinedScanTask(combinedScanTask).build();
        ReadTaskDescriptor descriptor =
            ReadTaskDescriptor.builder()
                .setTableIdentifierString(checkStateNotNull(snapshot.getTableIdentifierString()))
                .build();

        out.outputWithTimestamp(
            KV.of(descriptor, task), Instant.ofEpochMilli(snapshot.getTimestampMillis()));
        numTasks += combinedScanTask.tasks().size();
      }
    }
    totalScanTasks.inc(numTasks);
    LOG.info("Snapshot {} produced {} read tasks.", snapshot.getSnapshotId(), numTasks);
  }
}

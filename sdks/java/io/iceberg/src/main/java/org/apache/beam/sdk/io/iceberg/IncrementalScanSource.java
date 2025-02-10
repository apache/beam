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

import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An Iceberg source that reads a table incrementally using range(s) of table Snapshots. The
 * unbounded implementation will continuously poll for new Snapshots at the specified frequency. A
 * collection of FileScanTasks are created for each snapshot range. An SDF (shared by bounded and
 * unbounded implementations) is used to process each task and output Beam rows.
 */
class IncrementalScanSource extends PTransform<PBegin, PCollection<Row>> {
  // For the unbounded implementation.
  private static final long MAX_FILES_BATCH_BYTE_SIZE = 1L << 32; // 4 GB
  private final @Nullable Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  IncrementalScanSource(IcebergScanConfig scanConfig, @Nullable Duration pollInterval) {
    this.scanConfig = scanConfig;
    this.pollInterval = pollInterval;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Table table =
        TableCache.getRefreshed(
            scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());

    PCollection<Row> rows =
        pollInterval == null
            ? readBounded(input, table.currentSnapshot())
            : readUnbounded(input, pollInterval);
    return rows.setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(table.schema()));
  }

  /**
   * Watches for new snapshots and creates tasks for each range. Using GiB for autosharding, this
   * groups tasks in batches of up to 4GB, then reads from each batch using an SDF.
   */
  private PCollection<Row> readUnbounded(PBegin input, Duration duration) {
    return input
        .apply("Watch for Snapshots", new WatchForSnapshots(scanConfig, duration))
        .apply(
            "Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig.getCatalogConfig())))
        .setCoder(KvCoder.of(ReadTaskDescriptor.getCoder(), ReadTask.getCoder()))
        .apply(
            "Apply User Trigger",
            Window.<KV<ReadTaskDescriptor, ReadTask>>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterProcessingTime.pastFirstElementInPane().plusDelayOf(duration)))
                .discardingFiredPanes())
        .apply(
            GroupIntoBatches.<ReadTaskDescriptor, ReadTask>ofByteSize(
                    MAX_FILES_BATCH_BYTE_SIZE, ReadTask::getByteSize)
                .withMaxBufferingDuration(duration)
                .withShardedKey())
        .setCoder(
            KvCoder.of(
                ShardedKey.Coder.of(ReadTaskDescriptor.getCoder()),
                IterableCoder.of(ReadTask.getCoder())))
        .apply(
            "Read Rows From Grouped Tasks",
            ParDo.of(new ReadFromGroupedTasks(scanConfig.getCatalogConfig())));
  }

  /**
   * Scans a single snapshot range and creates read tasks. Tasks are redistributed and processed
   * individually using a regular DoFn.
   */
  private PCollection<Row> readBounded(PBegin input, @Nullable Snapshot toSnapshot) {
    checkStateNotNull(
        toSnapshot,
        "Table %s does not have any snapshots to read from.",
        scanConfig.getTableIdentifier());
    long to = MoreObjects.firstNonNull(scanConfig.getToSnapshot(), toSnapshot.snapshotId());
    return input
        .apply(
            "Create Single Snapshot Range",
            Create.of(
                SnapshotRange.builder()
                    .setTableIdentifierString(scanConfig.getTableIdentifier())
                    .setFromSnapshotExclusive(scanConfig.getFromSnapshotExclusive())
                    .setToSnapshot(to)
                    .build()))
        .apply(
            "Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig.getCatalogConfig())))
        .setCoder(KvCoder.of(ReadTaskDescriptor.getCoder(), ReadTask.getCoder()))
        .apply(Redistribute.arbitrarily())
        .apply("Read Rows From Tasks", ParDo.of(new ReadFromTasks(scanConfig.getCatalogConfig())));
  }
}

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

import java.util.List;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An Iceberg source that reads a table incrementally using range(s) of table snapshots. The bounded
 * source creates a single range, while the unbounded implementation continuously polls for new
 * snapshots at the specified interval.
 *
 * <p>Outputs CDC Rows with the following schema:
 *
 * <ul>
 *   <li>"record" [Row]: the data record itself
 *   <li>"operation" [String]: the operation associated with this record
 * </ul>
 */
class IncrementalScanSource extends PTransform<PBegin, PCollection<Row>> {
  // (streaming) We will group files into batches of this size
  // Downstream, we create one reader per batch
  // TODO(ahmedabu98): should we make this configurable?
  private static final long MAX_FILES_BATCH_BYTE_SIZE = 1L << 32; // 4 GB
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(60);
  private final IcebergScanConfig scanConfig;

  IncrementalScanSource(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Table table =
        TableCache.getRefreshed(
            scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());

    PCollection<Row> rows =
        MoreObjects.firstNonNull(scanConfig.getStreaming(), false)
            ? readUnbounded(input)
            : readBounded(input, table);
    return rows.setRowSchema(ReadUtils.outputCdcSchema(table.schema()));
  }

  /**
   * Watches for new snapshots and creates tasks for each range. Uses GiB (with auto-sharding) to
   * groups tasks in batches of size {@link #MAX_FILES_BATCH_BYTE_SIZE}, then reads from each batch
   * using an SDF.
   *
   * <p>Output schema is:
   *
   * <ul>
   *   <li>"record": the actual data record
   *   <li>"operation": the snapshot operation associated with this record (e.g. "append",
   *       "replace", "delete")
   * </ul>
   */
  private PCollection<Row> readUnbounded(PBegin input) {
    @Nullable
    Duration pollInterval =
        MoreObjects.firstNonNull(scanConfig.getPollInterval(), DEFAULT_POLL_INTERVAL);
    return input
        .apply("Watch for Snapshots", new WatchForSnapshots(scanConfig, pollInterval))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(SnapshotInfo.getCoder())))
        .apply("Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig)))
        .setCoder(KvCoder.of(ReadTaskDescriptor.getCoder(), ReadTask.getCoder()))
        .apply(
            GroupIntoBatches.<ReadTaskDescriptor, ReadTask>ofByteSize(
                    MAX_FILES_BATCH_BYTE_SIZE, ReadTask::getByteSize)
                .withMaxBufferingDuration(pollInterval)
                .withShardedKey())
        .setCoder(
            KvCoder.of(
                ShardedKey.Coder.of(ReadTaskDescriptor.getCoder()),
                IterableCoder.of(ReadTask.getCoder())))
        .apply(
            "Iterable to List",
            MapElements.via(
                new SimpleFunction<
                    KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>>,
                    KV<ReadTaskDescriptor, List<ReadTask>>>() {
                  @Override
                  public KV<ReadTaskDescriptor, List<ReadTask>> apply(
                      KV<ShardedKey<ReadTaskDescriptor>, Iterable<ReadTask>> input) {
                    return KV.of(input.getKey().getKey(), Lists.newArrayList(input.getValue()));
                  }
                }))
        .apply("Read Rows From Grouped Tasks", ParDo.of(new ReadFromGroupedTasks(scanConfig)));
  }

  /**
   * Scans a single snapshot range and creates read tasks. Tasks are redistributed and processed
   * individually using a regular DoFn.
   */
  private PCollection<Row> readBounded(PBegin input, Table table) {
    checkStateNotNull(
        table.currentSnapshot().snapshotId(),
        "Table %s does not have any snapshots to read from.",
        scanConfig.getTableIdentifier());

    @Nullable Long from = ReadUtils.getFromSnapshotExclusive(table, scanConfig);
    // if no end snapshot is provided, we read up to the current snapshot.
    long to =
        MoreObjects.firstNonNull(
            ReadUtils.getToSnapshot(table, scanConfig), table.currentSnapshot().snapshotId());
    return input
        .apply(
            "Create Snapshot Range",
            Create.of(
                KV.of(
                    scanConfig.getTableIdentifier(),
                    ReadUtils.snapshotsBetween(table, scanConfig.getTableIdentifier(), from, to))))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(SnapshotInfo.getCoder())))
        .apply("Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig)))
        .setCoder(KvCoder.of(ReadTaskDescriptor.getCoder(), ReadTask.getCoder()))
        .apply(Redistribute.arbitrarily())
        .apply("Read Rows From Tasks", ParDo.of(new ReadFromTasks(scanConfig)));
  }
}

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
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Redistribute;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An Iceberg source that reads a table incrementally using range(s) of table snapshots. The bounded
 * source creates a single range, while the unbounded implementation continuously polls for new
 * snapshots at the specified interval.
 */
class IncrementalScanSource extends PTransform<PBegin, PCollection<Row>> {
  private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(60);
  private final IcebergScanConfig scanConfig;

  IncrementalScanSource(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    Table table =
        scanConfig
            .getCatalogConfig()
            .catalog()
            .loadTable(TableIdentifier.parse(scanConfig.getTableIdentifier()));

    PCollection<KV<String, List<SnapshotInfo>>> snapshots =
        MoreObjects.firstNonNull(scanConfig.getStreaming(), false)
            ? unboundedSnapshots(input)
            : boundedSnapshots(input, table);

    return snapshots
        .setCoder(KvCoder.of(StringUtf8Coder.of(), ListCoder.of(SnapshotInfo.getCoder())))
        .apply(Redistribute.byKey())
        .apply("Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig)))
        .setCoder(KvCoder.of(ReadTaskDescriptor.getCoder(), ReadTask.getCoder()))
        .apply(Redistribute.arbitrarily())
        .apply("Read Rows From Tasks", ParDo.of(new ReadFromTasks(scanConfig)))
        .setRowSchema(IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema()));
  }

  /** Continuously watches for new snapshots. */
  private PCollection<KV<String, List<SnapshotInfo>>> unboundedSnapshots(PBegin input) {
    Duration pollInterval =
        MoreObjects.firstNonNull(scanConfig.getPollInterval(), DEFAULT_POLL_INTERVAL);
    return input.apply("Watch for Snapshots", new WatchForSnapshots(scanConfig, pollInterval));
  }

  /** Creates a fixed snapshot range. */
  private PCollection<KV<String, List<SnapshotInfo>>> boundedSnapshots(PBegin input, Table table) {
    checkStateNotNull(
        table.currentSnapshot().snapshotId(),
        "Table %s does not have any snapshots to read from.",
        scanConfig.getTableIdentifier());

    @Nullable Long from = ReadUtils.getFromSnapshotExclusive(table, scanConfig);
    // if no end snapshot is provided, we read up to the current snapshot.
    long to =
        MoreObjects.firstNonNull(
            ReadUtils.getToSnapshot(table, scanConfig), table.currentSnapshot().snapshotId());
    return input.apply(
        "Create Snapshot Range",
        Create.of(
            KV.of(
                scanConfig.getTableIdentifier(),
                ReadUtils.snapshotsBetween(table, scanConfig.getTableIdentifier(), from, to))));
  }
}

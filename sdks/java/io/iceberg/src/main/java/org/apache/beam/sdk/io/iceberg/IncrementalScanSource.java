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

import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.Table;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * An incremental Iceberg source that reads range(s) of table Snapshots. The unbounded
 * implementation will continuously poll for new Snapshots at a specified frequency. For each range
 * of Snapshots, the transform will create a list of FileScanTasks. An SDF is used to process each
 * task and output its rows.
 */
class IncrementalScanSource extends PTransform<PBegin, PCollection<Row>> {
  private final @Nullable Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  IncrementalScanSource(IcebergScanConfig scanConfig, @Nullable Duration pollInterval) {
    this.scanConfig = scanConfig;
    this.pollInterval = pollInterval;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    PCollection<SnapshotRange> snapshotRanges;
    if (pollInterval != null) { // unbounded
      snapshotRanges = input.apply(new WatchForSnapshots(scanConfig, pollInterval));
    } else { // bounded
      @Nullable Long to = scanConfig.getToSnapshot();
      if (to == null) {
        Table table =
            TableCache.getRefreshed(
                scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());
        to = table.currentSnapshot().snapshotId();
      }
      snapshotRanges =
          input.apply(
              "Create Single Snapshot Range",
              Create.of(
                  SnapshotRange.builder()
                      .setTableIdentifierString(scanConfig.getTableIdentifier())
                      .setFromSnapshot(scanConfig.getFromSnapshotExclusive())
                      .setToSnapshot(to)
                      .build()));
    }

    return snapshotRanges
        .apply(
            "Create Read Tasks", ParDo.of(new CreateReadTasksDoFn(scanConfig.getCatalogConfig())))
        .apply("Read Rows From Tasks", ParDo.of(new ReadFromTasks(scanConfig.getCatalogConfig())));
  }
}

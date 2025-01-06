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

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

/**
 * An unbounded source that polls for new {@link org.apache.iceberg.Snapshot}s and performs {@link
 * org.apache.iceberg.IncrementalAppendScan}s to create a list of {@link
 * org.apache.iceberg.FileScanTask}s for each range of snapshots. An SDF is used to process each
 * file and output its rows.
 */
class IncrementalScanSource extends PTransform<PBegin, PCollection<Row>> {
  private final Duration pollInterval;
  private final IcebergScanConfig scanConfig;

  IncrementalScanSource(IcebergScanConfig scanConfig, Duration pollInterval) {
    this.scanConfig = scanConfig;
    this.pollInterval = pollInterval;
  }

  @Override
  public PCollection<Row> expand(PBegin input) {
    return input
        .apply(new WatchForSnapshots(scanConfig, pollInterval))
        .apply(ParDo.of(new CreateReadTasksDoFn(scanConfig.getCatalogConfig())))
        .apply(ParDo.of(new ReadFromTasks(scanConfig.getCatalogConfig())));
  }
}

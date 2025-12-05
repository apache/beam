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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Bounded read implementation.
 *
 * <p>For each {@link ReadTask}, reads Iceberg {@link Record}s, and converts to Beam {@link Row}s.
 *
 * <p>Implemented as an SDF to leverage communicating bundle size (i.e. {@link DoFn.GetSize}) to the
 * runner, to help with scaling decisions.
 */
@DoFn.BoundedPerElement
class ReadFromTasks extends DoFn<KV<ReadTaskDescriptor, ReadTask>, Row> {
  private final IcebergScanConfig scanConfig;
  private final Counter scanTasksCompleted =
      Metrics.counter(ReadFromTasks.class, "scanTasksCompleted");

  ReadFromTasks(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Setup
  public void setup() {
    TableCache.setup(scanConfig);
  }

  @ProcessElement
  public void process(
      @Element KV<ReadTaskDescriptor, ReadTask> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws IOException, ExecutionException, InterruptedException {
    ReadTask readTask = element.getValue();
    Table table = TableCache.get(scanConfig.getTableIdentifier());

    List<FileScanTask> fileScanTasks = readTask.getFileScanTasks();

    for (long l = tracker.currentRestriction().getFrom();
        l < tracker.currentRestriction().getTo();
        l++) {
      if (!tracker.tryClaim(l)) {
        return;
      }
      FileScanTask task = fileScanTasks.get((int) l);
      Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
      try (CloseableIterable<Record> fullIterable =
          ReadUtils.createReader(task, table, scanConfig.getRequiredSchema())) {
        CloseableIterable<Record> reader = ReadUtils.maybeApplyFilter(fullIterable, scanConfig);

        for (Record record : reader) {
          Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
          out.output(row);
        }
      }
      scanTasksCompleted.inc();
    }
  }

  @GetSize
  public double getSize(
      @Element KV<ReadTaskDescriptor, ReadTask> element, @Restriction OffsetRange restriction) {
    // TODO(ahmedabu98): this is actually the file byte size, likely compressed.
    //  find a way to output the actual Beam Row byte size.
    return element.getValue().getSize(restriction.getFrom(), restriction.getTo());
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(@Element KV<ReadTaskDescriptor, ReadTask> element) {
    return new OffsetRange(0, element.getValue().getFileScanTaskJsons().size());
  }
}

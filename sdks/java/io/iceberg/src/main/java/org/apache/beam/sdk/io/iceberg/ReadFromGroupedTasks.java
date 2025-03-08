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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Unbounded read implementation.
 *
 * <p>An SDF that takes a batch of {@link ReadTask}s. For each task, reads Iceberg {@link Record}s,
 * and converts to Beam {@link Row}s.
 *
 * <p>The SDF checkpoints after reading each task, and can split the batch of read tasks as needed.
 */
@DoFn.BoundedPerElement
class ReadFromGroupedTasks extends DoFn<KV<ReadTaskDescriptor, List<ReadTask>>, Row> {
  private final IcebergScanConfig scanConfig;
  private final Counter scanTasksCompleted =
      Metrics.counter(ReadFromGroupedTasks.class, "scanTasksCompleted");

  ReadFromGroupedTasks(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @ProcessElement
  public void process(
      @Element KV<ReadTaskDescriptor, List<ReadTask>> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws IOException, ExecutionException {
    List<ReadTask> readTasks = element.getValue();
    Table table =
        TableCache.get(scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());

    // SDF can split by the number of read tasks
    for (long taskIndex = tracker.currentRestriction().getFrom();
        taskIndex < tracker.currentRestriction().getTo();
        ++taskIndex) {
      if (!tracker.tryClaim(taskIndex)) {
        return;
      }

      ReadTask readTask = readTasks.get((int) taskIndex);
      @Nullable String operation = readTask.getOperation();
      FileScanTask task = readTask.getFileScanTask();
      Instant outputTimestamp = ReadUtils.getReadTaskTimestamp(readTask, scanConfig);

      try (CloseableIterable<Record> reader = ReadUtils.createReader(task, table)) {
        for (Record record : reader) {
          Row row =
              Row.withSchema(scanConfig.getCdcSchema())
                  .addValue(IcebergUtils.icebergRecordToBeamRow(scanConfig.getSchema(), record))
                  .addValue(operation)
                  .build();
          out.outputWithTimestamp(row, outputTimestamp);
        }
      }
      scanTasksCompleted.inc();
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(@Element KV<ReadTaskDescriptor, List<ReadTask>> element) {
    return new OffsetRange(0, element.getValue().size());
  }

  @GetSize
  public double getSize(
      @Element KV<ReadTaskDescriptor, List<ReadTask>> element,
      @Restriction OffsetRange restriction) {
    double size = 0;
    List<ReadTask> tasks = element.getValue();
    for (ReadTask task : tasks.subList((int) restriction.getFrom(), tasks.size())) {
      size += task.getByteSize();
    }
    return size;
  }

  // infinite skew in case we encounter some files that don't support watermark column statistics,
  // in which case we output a -inf timestamp.
  @Override
  public Duration getAllowedTimestampSkew() {
    return Duration.millis(Long.MAX_VALUE);
  }
}

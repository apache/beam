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
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Bounded read implementation.
 *
 * <p>For each {@link ReadTask}, reads Iceberg {@link Record}s, and converts to Beam {@link Row}s.
 */
class ReadFromTasks extends DoFn<KV<ReadTaskDescriptor, ReadTask>, Row> {
  private final IcebergScanConfig scanConfig;
  private final Counter scanTasksCompleted =
      Metrics.counter(ReadFromTasks.class, "scanTasksCompleted");

  ReadFromTasks(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @ProcessElement
  public void process(@Element KV<ReadTaskDescriptor, ReadTask> element, OutputReceiver<Row> out)
      throws IOException, ExecutionException {
    String tableIdentifier = element.getKey().getTableIdentifierString();
    ReadTask readTask = element.getValue();
    Table table = TableCache.get(tableIdentifier, scanConfig.getCatalogConfig().catalog());
    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(table.schema());

    Instant outputTimestamp = ReadUtils.getReadTaskTimestamp(readTask, scanConfig);
    FileScanTask task = readTask.getFileScanTask();

    try (CloseableIterable<Record> reader = ReadUtils.createReader(task, table)) {
      for (Record record : reader) {
        Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
        out.outputWithTimestamp(row, outputTimestamp);
      }
    }
    scanTasksCompleted.inc();
  }

  // infinite skew in case we encounter some files that don't support watermark column statistics,
  // in which case we output a -inf timestamp.
  @Override
  public Duration getAllowedTimestampSkew() {
    return Duration.millis(Long.MAX_VALUE);
  }
}

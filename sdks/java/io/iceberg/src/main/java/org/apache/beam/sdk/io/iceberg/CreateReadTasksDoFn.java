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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates a collection of {@link FileScanTask}s for each {@link SnapshotRange}. Each task
 * represents a data file that was appended within a given snapshot range.
 */
class CreateReadTasksDoFn extends DoFn<SnapshotRange, ReadTaskDescriptor> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateReadTasksDoFn.class);
  private final Counter numFileScanTasks =
      Metrics.counter(CreateReadTasksDoFn.class, "numFileScanTasks");
  private final IcebergCatalogConfig catalogConfig;

  CreateReadTasksDoFn(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void process(@Element SnapshotRange descriptor, OutputReceiver<ReadTaskDescriptor> out)
      throws IOException, ExecutionException {
    Table table = TableCache.get(descriptor.getTableIdentifier(), catalogConfig.catalog());

    long fromSnapshot = descriptor.getFromSnapshot();
    long toSnapshot = descriptor.getToSnapshot();

    LOG.info("Planning to scan snapshot range ({}, {}]", fromSnapshot, toSnapshot);
    IncrementalAppendScan scan = table.newIncrementalAppendScan().toSnapshot(toSnapshot);
    if (fromSnapshot > -1) {
      scan = scan.fromSnapshotExclusive(fromSnapshot);
    }
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        ReadTaskDescriptor taskDescriptor =
            ReadTaskDescriptor.builder()
                .setTableIdentifierString(descriptor.getTable())
                .setFileScanTaskJson(ScanTaskParser.toJson(task))
                .setRecordCount(task.file().recordCount())
                .build();

        numFileScanTasks.inc();
        out.output(taskDescriptor);
      }
    }
  }
}

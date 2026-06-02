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
package org.apache.beam.sdk.io.delta;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.hadoop.conf.Configuration;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A DoFn that reads the Delta log and outputs a list of DeltaReadTask records to read. */
class CreateReadTasksDoFn extends DoFn<String, DeltaReadTask> {
  private static final long MAX_TASK_SIZE_BYTES = 1024L * 1024L * 1024L; // 1 GB
  private final @Nullable Map<String, String> hadoopConfig;

  public CreateReadTasksDoFn(@Nullable Map<String, String> hadoopConfig) {
    this.hadoopConfig = hadoopConfig;
  }

  @ProcessElement
  public void processElement(@Element String tablePath, OutputReceiver<DeltaReadTask> out)
      throws Exception {
    Configuration conf = new Configuration();
    if (hadoopConfig != null) {
      for (Map.Entry<String, String> entry : hadoopConfig.entrySet()) {
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    Engine engine = DefaultEngine.create(conf);
    Table table = Table.forPath(engine, tablePath);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    Scan scan = snapshot.getScanBuilder().build();
    Row scanState = scan.getScanState(engine);
    SerializableRow serializableScanState = new SerializableRow(scanState);

    List<SerializableRow> currentGroup = new ArrayList<>();
    List<List<Long>> currentGroupRowGroupSizes = new ArrayList<>();
    long currentGroupSize = 0L;

    try (CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine)) {
      while (scanFiles.hasNext()) {
        FilteredColumnarBatch batch = scanFiles.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();
            SerializableRow fileRow = new SerializableRow(scanFileRow);
            long fileSize = InternalScanFileUtils.getAddFileStatus(fileRow).getSize();
            List<Long> fileRowGroupSizes = getRowGroupSizes(fileRow, conf);

            if (fileSize >= MAX_TASK_SIZE_BYTES) {
              if (!currentGroup.isEmpty()) {
                DeltaReadTask readTask =
                    new DeltaReadTask(
                        currentGroup, serializableScanState, currentGroupRowGroupSizes);
                out.output(readTask);
                currentGroup = new ArrayList<>();
                currentGroupRowGroupSizes = new ArrayList<>();
                currentGroupSize = 0L;
              }

              DeltaReadTask readTask =
                  new DeltaReadTask(
                      Collections.singletonList(fileRow),
                      serializableScanState,
                      Collections.singletonList(fileRowGroupSizes));
              out.output(readTask);
            } else {
              if (currentGroupSize + fileSize > MAX_TASK_SIZE_BYTES) {
                DeltaReadTask readTask =
                    new DeltaReadTask(
                        currentGroup, serializableScanState, currentGroupRowGroupSizes);
                out.output(readTask);
                currentGroup = new ArrayList<>();
                currentGroupRowGroupSizes = new ArrayList<>();
                currentGroup.add(fileRow);
                currentGroupRowGroupSizes.add(fileRowGroupSizes);
                currentGroupSize = fileSize;
              } else {
                currentGroup.add(fileRow);
                currentGroupRowGroupSizes.add(fileRowGroupSizes);
                currentGroupSize += fileSize;
              }
            }
          }
        }
      }
    }

    if (!currentGroup.isEmpty()) {
      DeltaReadTask readTask =
          new DeltaReadTask(currentGroup, serializableScanState, currentGroupRowGroupSizes);
      out.output(readTask);
    }
  }

  private List<Long> getRowGroupSizes(SerializableRow scanFileRow, Configuration conf) {
    List<Long> sizes = new ArrayList<>();
    String pathStr = InternalScanFileUtils.getAddFileStatus(scanFileRow).getPath();
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(pathStr);
      org.apache.parquet.hadoop.metadata.ParquetMetadata metadata =
          org.apache.parquet.hadoop.ParquetFileReader.readFooter(
              conf,
              hadoopPath,
              org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER);
      for (org.apache.parquet.hadoop.metadata.BlockMetaData block : metadata.getBlocks()) {
        sizes.add(block.getTotalByteSize());
      }
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failed to read Parquet footer for " + pathStr, e);
    }
    return sizes;
  }
}

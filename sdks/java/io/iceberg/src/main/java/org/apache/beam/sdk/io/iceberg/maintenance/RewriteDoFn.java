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
package org.apache.beam.sdk.io.iceberg.maintenance;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.iceberg.ReadUtils;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.util.PropertyUtil;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RewriteDoFn extends DoFn<RewriteFileGroup, SerializableDataFile> {
  private static final Logger LOG = LoggerFactory.getLogger(RewriteDoFn.class);
  private final String operationId;
  private final SerializableTable table;
  private final FileFormat format;
  private @MonotonicNonNull WriterFactory writerFactory;
  private static final Counter activeRewriters =
      Metrics.counter(RewriteDoFn.class, "activeRewriters");
  private static final Distribution filesToRewriteByteSize =
      Metrics.distribution(RewriteDoFn.class, "filesToRewriteByteSize");
  private static final Distribution outputFileByteSize =
      Metrics.distribution(RewriteDoFn.class, "outputFileByteSize");

  RewriteDoFn(SerializableTable table, String operationId) {
    this.operationId = operationId;
    this.table = table;

    String formatString =
        PropertyUtil.propertyAsString(
            table.properties(),
            TableProperties.DEFAULT_FILE_FORMAT,
            TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
    this.format = FileFormat.fromString(formatString);
  }

  @Setup
  public void start() {
    long workerId = ThreadLocalRandom.current().nextLong();
    LOG.info(
        RewriteDataFiles.REWRITE_PREFIX + "Starting up a RewriteDoFn with worker-id: " + workerId);
    writerFactory = new WriterFactory(format, Long.MAX_VALUE, workerId, operationId);
  }

  @ProcessElement
  public void processElement(
      @Element RewriteFileGroup group, OutputReceiver<SerializableDataFile> output)
      throws IOException {
    try (TaskWriter<Record> writer = createWriter()) {
      activeRewriters.inc();
      for (String jsonTask : group.getJsonTasks()) {
        FileScanTask fileScanTask = ScanTaskParser.fromJson(jsonTask, true);
        if (fileScanTask.start() == 0) {
          // A single underlying data file can be split into multiple FileScanTasks (e.g., in a
          // split plan). We only track the file's total size once, when processing the task
          // that starts at offset 0.
          filesToRewriteByteSize.update(fileScanTask.length());
        }

        try (CloseableIterable<Record> iterable = ReadUtils.createReader(fileScanTask, table)) {
          GenericDeleteFilter deleteFilter =
              new GenericDeleteFilter(table.io(), fileScanTask, table.schema(), table.schema());
          CloseableIterable<Record> reader = deleteFilter.filter(iterable);

          for (Record record : reader) {
            writer.write(record);
          }
        }
      }

      DataFile[] datafiles = writer.dataFiles();
      for (DataFile dataFile : datafiles) {
        SerializableDataFile serializableDataFile =
            SerializableDataFile.from(dataFile, table.specs());
        output.output(serializableDataFile);

        outputFileByteSize.update(dataFile.fileSizeInBytes());
      }
      System.out.println(
          "xxx output files: "
              + Arrays.stream(datafiles)
                  .map(d -> SerializableDataFile.from(d, table.specs()))
                  .collect(Collectors.toList()));
    }
    activeRewriters.dec();
  }

  TaskWriter<Record> createWriter() {
    checkStateNotNull(writerFactory).init(table);
    return checkStateNotNull(writerFactory).create();
  }
}

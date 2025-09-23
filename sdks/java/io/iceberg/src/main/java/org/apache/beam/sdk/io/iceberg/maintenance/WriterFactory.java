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

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

class WriterFactory {
  private final long targetFileSizeBytes;
  private final String operationId;
  private final long workerId;
  private final FileFormat format;
  private @MonotonicNonNull OutputFileFactory outputFileFactory;
  private @MonotonicNonNull Table table;

  WriterFactory(FileFormat format, long targetFileSizeBytes, long workerId, String operationId) {
    this.format = format;
    this.targetFileSizeBytes = targetFileSizeBytes;
    this.operationId = operationId;
    this.workerId = workerId;
  }

  void init(Table table) {
    if (outputFileFactory == null) {
      this.table = table;

      outputFileFactory =
          OutputFileFactory.builderFor(table, 0, workerId)
              .format(format)
              .ioSupplier(table::io)
              .defaultSpec(table.spec())
              .operationId(operationId)
              .build();
    }
  }

  TaskWriter<Record> create() {
    Table table = checkStateNotNull(this.table);
    FileAppenderFactory<Record> appenderFactory = new GenericAppenderFactory(table.schema());

    if (table.spec().isUnpartitioned()) {
      return new UnpartitionedWriter<>(
          table.spec(),
          format,
          appenderFactory,
          checkStateNotNull(outputFileFactory),
          table.io(),
          targetFileSizeBytes);
    } else {
      return new RecordPartitionedFanoutWriter(
          table.spec(),
          format,
          appenderFactory,
          checkStateNotNull(outputFileFactory),
          table.io(),
          targetFileSizeBytes,
          table.schema());
    }
  }

  private static class RecordPartitionedFanoutWriter extends PartitionedFanoutWriter<Record> {

    private final PartitionKey partitionKey;
    private final InternalRecordWrapper recordWrapper;

    RecordPartitionedFanoutWriter(
        PartitionSpec spec,
        FileFormat format,
        FileAppenderFactory<Record> appenderFactory,
        OutputFileFactory fileFactory,
        FileIO io,
        long targetFileSize,
        Schema schema) {
      super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
      this.partitionKey = new PartitionKey(spec, schema);
      this.recordWrapper = new InternalRecordWrapper(schema.asStruct());
    }

    @Override
    protected PartitionKey partition(Record row) {
      partitionKey.partition(recordWrapper.wrap(row));
      return partitionKey;
    }
  }
}

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
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * Reads Iceberg {@link Record}s from a {@link FileScanTask} and converts to Beam {@link Row}s
 * before outputting.
 */
@DoFn.BoundedPerElement
class ReadFromTasks extends DoFn<ReadTaskDescriptor, Row> {
  private final IcebergCatalogConfig catalogConfig;

  ReadFromTasks(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void process(
      @Element ReadTaskDescriptor taskDescriptor,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws IOException, ExecutionException {
    Table table =
        TableCache.get(taskDescriptor.getTableIdentifierString(), catalogConfig.catalog());

    FileScanTask task = taskDescriptor.getFileScanTask();
    DataFile dataFile = task.file();
    String filePath = dataFile.path().toString();
    ByteBuffer encryptionKeyMetadata = dataFile.keyMetadata();
    Schema tableSchema = table.schema();

    // TODO(ahmedabu98): maybe cache this file ref?
    InputFile inputFile;
    try (FileIO io = table.io()) {
      inputFile = io.newInputFile(filePath);
    }
    if (encryptionKeyMetadata != null) {
      inputFile =
          table
              .encryption()
              .decrypt(EncryptedFiles.encryptedInput(inputFile, encryptionKeyMetadata));
    }

    Parquet.ReadBuilder readBuilder =
        Parquet.read(inputFile)
            .split(task.start(), task.length())
            .project(tableSchema)
            .createReaderFunc(
                fileSchema -> GenericParquetReaders.buildReader(tableSchema, fileSchema))
            .filter(task.residual());

    long fromRecord = tracker.currentRestriction().getFrom();
    long toRecord = tracker.currentRestriction().getTo();
    try (CloseableIterable<Record> iterable = readBuilder.build()) {
      CloseableIterator<Record> reader = iterable.iterator();
      // Skip until fromRecord
      // TODO(ahmedabu98): this is extremely inefficient
      for (long skipped = 0; skipped < fromRecord && reader.hasNext(); ++skipped) {
        reader.next();
      }

      for (long l = fromRecord; l < toRecord && reader.hasNext(); ++l) {
        if (!tracker.tryClaim(l)) {
          break;
        }
        Record record = reader.next();
        Row row = IcebergUtils.icebergRecordToBeamRow(tableSchema, record);
        out.output(row);
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(@Element ReadTaskDescriptor task) {
    return new OffsetRange(0, task.getRecordCount());
  }
}

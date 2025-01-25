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
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An SDF that operates on a collection of {@link FileScanTask}s. For each task, reads Iceberg
 * {@link Record}s, and converts to Beam {@link Row}s.
 *
 * <p>Can split
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
    Schema tableSchema = table.schema();
    org.apache.beam.sdk.schemas.Schema beamSchema =
        IcebergUtils.icebergSchemaToBeamSchema(tableSchema);
    @Nullable String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping =
        nameMapping != null ? NameMappingParser.fromJson(nameMapping) : NameMapping.empty();

    InputFilesDecryptor decryptor;
    try (FileIO io = table.io()) {
      decryptor =
          new InputFilesDecryptor(taskDescriptor.getCombinedScanTask(), io, table.encryption());
    }

    for (long taskIndex = tracker.currentRestriction().getFrom();
        taskIndex < tracker.currentRestriction().getTo();
        ++taskIndex) {
      if (!tracker.tryClaim(taskIndex)) {
        return;
      }
      FileScanTask task = taskDescriptor.getFileScanTask(taskIndex);
      InputFile inputFile = decryptor.getInputFile(task);

      Map<Integer, ?> idToConstants =
          IcebergUtils.constantsMap(
              task, IdentityPartitionConverters::convertConstant, tableSchema);

      Parquet.ReadBuilder readBuilder =
          Parquet.read(inputFile)
              .split(task.start(), task.length())
              .project(tableSchema)
              .createReaderFunc(
                  fileSchema ->
                      GenericParquetReaders.buildReader(tableSchema, fileSchema, idToConstants))
              .withNameMapping(mapping)
              .filter(task.residual());

      try (CloseableIterable<Record> iterable = readBuilder.build()) {
        for (Record record : iterable) {
          Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
          out.output(row);
        }
      }
    }
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(@Element ReadTaskDescriptor taskDescriptor) {
    return new OffsetRange(0, taskDescriptor.numTasks());
  }

  @GetSize
  public double getSize(
      @Element ReadTaskDescriptor taskDescriptor, @Restriction OffsetRange restriction)
      throws Exception {
    double size = 0;
    for (long i = restriction.getFrom(); i < restriction.getTo(); i++) {
      size += taskDescriptor.getFileScanTask(i).sizeBytes();
    }
    return size;
  }
}

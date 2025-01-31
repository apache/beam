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
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetReader;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Bounded read implementation.
 *
 * <p>For each {@link ReadTask}, reads Iceberg {@link Record}s, and converts to Beam {@link Row}s.
 */
class ReadFromTasks extends DoFn<KV<ReadTaskDescriptor, ReadTask>, Row> {
  private final IcebergCatalogConfig catalogConfig;

  ReadFromTasks(IcebergCatalogConfig catalogConfig) {
    this.catalogConfig = catalogConfig;
  }

  @ProcessElement
  public void process(@Element KV<ReadTaskDescriptor, ReadTask> element, OutputReceiver<Row> out)
      throws IOException, ExecutionException {
    String tableIdentifier = element.getKey().getTableIdentifierString();
    ReadTask readTask = element.getValue();
    Table table = TableCache.get(tableIdentifier, catalogConfig.catalog());
    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(table.schema());
    @Nullable String nameMapping = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping mapping =
        nameMapping != null ? NameMappingParser.fromJson(nameMapping) : NameMapping.empty();

    FileScanTask task = readTask.getFileScanTask();

    try (CloseableIterable<Record> reader = getReader(task, table, mapping)) {
      for (Record record : reader) {
        Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
        out.output(row);
      }
    }
  }

  private static final Collection<String> READ_PROPERTIES_TO_REMOVE =
      Sets.newHashSet(
          "parquet.read.filter",
          "parquet.private.read.filter.predicate",
          "parquet.read.support.class",
          "parquet.crypto.factory.class");

  static ParquetReader<Record> getReader(FileScanTask task, Table table, NameMapping mapping) {
    String filePath = task.file().path().toString();
    InputFile inputFile;
    try (FileIO io = table.io()) {
      EncryptedInputFile encryptedInput =
          EncryptedFiles.encryptedInput(io.newInputFile(filePath), task.file().keyMetadata());
      inputFile = table.encryption().decrypt(encryptedInput);
    }
    Map<Integer, ?> idToConstants =
        IcebergUtils.constantsMap(
            task, IdentityPartitionConverters::convertConstant, table.schema());

    ParquetReadOptions.Builder optionsBuilder;
    if (inputFile instanceof HadoopInputFile) {
      // remove read properties already set that may conflict with this read
      Configuration conf = new Configuration(((HadoopInputFile) inputFile).getConf());
      for (String property : READ_PROPERTIES_TO_REMOVE) {
        conf.unset(property);
      }
      optionsBuilder = HadoopReadOptions.builder(conf);
    } else {
      optionsBuilder = ParquetReadOptions.builder();
    }
    optionsBuilder =
        optionsBuilder
            .withRange(task.start(), task.start() + task.length())
            .withMaxAllocationInBytes(1 << 20); // 1MB

    return new ParquetReader<>(
        inputFile,
        table.schema(),
        optionsBuilder.build(),
        fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema, idToConstants),
        mapping,
        task.residual(),
        false,
        true);
  }
}

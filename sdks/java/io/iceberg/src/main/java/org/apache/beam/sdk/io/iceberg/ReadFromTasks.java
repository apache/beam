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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
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

    try (CloseableIterable<Record> reader = ReadUtils.createReader(task, table, mapping)) {
      for (Record record : reader) {
        Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
        out.output(row);
      }
    }
  }
}

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
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.avro.io.AvroSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link BigQuerySourceBase} for reading BigQuery tables. */
@VisibleForTesting
class BigQueryTableSource<T> extends BigQuerySourceBase<T> {

  static <T> BigQueryTableSource<T> create(
      String stepUuid,
      BigQueryTableSourceDef tableDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory,
      boolean useAvroLogicalTypes) {
    return new BigQueryTableSource<>(
        stepUuid, tableDef, bqServices, coder, readerFactory, useAvroLogicalTypes);
  }

  private final BigQueryTableSourceDef tableDef;
  private final AtomicReference<@Nullable Long> tableSizeBytes;

  private BigQueryTableSource(
      String stepUuid,
      BigQueryTableSourceDef tableDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<TableSchema, AvroSource.DatumReaderFactory<T>> readerFactory,
      boolean useAvroLogicalTypes) {
    super(stepUuid, bqServices, coder, readerFactory, useAvroLogicalTypes);
    this.tableDef = tableDef;
    this.tableSizeBytes = new AtomicReference<>();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions) throws IOException {
    return tableDef.getTableReference(bqOptions);
  }

  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    Long maybeNumBytes = tableSizeBytes.get();
    if (maybeNumBytes != null) {
      return maybeNumBytes;
    } else {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReference tableRef = tableDef.getTableReference(bqOptions);
      try (DatasetService datasetService = bqServices.getDatasetService(bqOptions)) {
        Table table = datasetService.getTable(tableRef);

        if (table == null) {
          throw new IllegalStateException("Table not found: " + table);
        }

        Long numBytes = table.getNumBytes();
        if (table.getStreamingBuffer() != null
            && table.getStreamingBuffer().getEstimatedBytes() != null) {
          numBytes += table.getStreamingBuffer().getEstimatedBytes().longValue();
        }

        tableSizeBytes.compareAndSet(null, numBytes);
        return numBytes;
      }
    }
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    // Do nothing.
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("table", tableDef.getJsonTable()));
    builder.add(
        DisplayData.item("launchesBigQueryJobs", true)
            .withLabel("This transform launches BigQuery jobs to read/write elements."));
  }
}

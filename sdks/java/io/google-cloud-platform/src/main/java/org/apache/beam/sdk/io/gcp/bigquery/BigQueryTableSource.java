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
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link BigQuerySourceBase} for reading BigQuery tables. */
@VisibleForTesting
class BigQueryTableSource<T> extends BigQuerySourceBase<T> {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableSource.class);

  static <T> BigQueryTableSource<T> create(
      String stepUuid,
      BigQueryTableSourceDef tableDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn) {
    return new BigQueryTableSource<>(stepUuid, tableDef, bqServices, coder, parseFn);
  }

  private final BigQueryTableSourceDef tableDef;
  private final AtomicReference<Long> tableSizeBytes;

  private BigQueryTableSource(
      String stepUuid,
      BigQueryTableSourceDef tableDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn) {
    super(stepUuid, bqServices, coder, parseFn);
    this.tableDef = tableDef;
    this.tableSizeBytes = new AtomicReference<>();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions) throws IOException {
    return tableDef.getTableReference(bqOptions);
  }

  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (tableSizeBytes.get() == null) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReference tableRef = tableDef.getTableReference(bqOptions);
      Table table = bqServices.getDatasetService(bqOptions).getTable(tableRef);
      Long numBytes = table.getNumBytes();
      if (table.getStreamingBuffer() != null
          && table.getStreamingBuffer().getEstimatedBytes() != null) {
        numBytes += table.getStreamingBuffer().getEstimatedBytes().longValue();
      }

      tableSizeBytes.compareAndSet(null, numBytes);
    }
    return tableSizeBytes.get();
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

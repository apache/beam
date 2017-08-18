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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToJson;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BigQuerySourceBase} for reading BigQuery tables.
 */
@VisibleForTesting
class BigQueryTableSource extends BigQuerySourceBase {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryTableSource.class);

  static BigQueryTableSource create(
      String stepUuid,
      ValueProvider<TableReference> table,
      BigQueryServices bqServices) {
    return new BigQueryTableSource(stepUuid, table, bqServices);
  }

  private final ValueProvider<String> jsonTable;
  private final AtomicReference<Long> tableSizeBytes;

  private BigQueryTableSource(
      String stepUuid,
      ValueProvider<TableReference> table,
      BigQueryServices bqServices) {
    super(stepUuid, bqServices);
    this.jsonTable = NestedValueProvider.of(checkNotNull(table, "table"), new TableRefToJson());
    this.tableSizeBytes = new AtomicReference<>();
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions) throws IOException {
    TableReference tableReference =
        BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class);
    return setDefaultProjectIfAbsent(bqOptions, tableReference);
  }

  /**
   * Sets the {@link TableReference#projectId} of the provided table reference to the id of the
   * default project if the table reference does not have a project ID specified.
   */
  private TableReference setDefaultProjectIfAbsent(
      BigQueryOptions bqOptions, TableReference tableReference) {
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      checkState(
          !Strings.isNullOrEmpty(bqOptions.getProject()),
          "No project ID set in %s or %s, cannot construct a complete %s",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReference.class.getSimpleName());
      LOG.info(
          "Project ID not set in {}. Using default project from {}.",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName());
      tableReference.setProjectId(bqOptions.getProject());
    }
    return tableReference;
  }

  @Override
  public BoundedReader<TableRow> createReader(PipelineOptions options) throws IOException {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    TableReference tableRef = BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(),
        TableReference.class);
    return new BigQueryReader(this, bqServices.getReaderFromTable(bqOptions, tableRef));
  }

  @Override
  public synchronized long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (tableSizeBytes.get() == null) {
      TableReference table = setDefaultProjectIfAbsent(options.as(BigQueryOptions.class),
          BigQueryIO.JSON_FACTORY.fromString(jsonTable.get(), TableReference.class));

      Long numBytes = bqServices.getDatasetService(options.as(BigQueryOptions.class))
          .getTable(table).getNumBytes();
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
    builder.add(DisplayData.item("table", jsonTable));
  }
}

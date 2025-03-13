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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing reading from a table. */
public class BigQueryStorageTableSource<T> extends BigQueryStorageSourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageTableSource.class);

  private final ValueProvider<TableReference> tableReferenceProvider;
  private final boolean projectionPushdownApplied;

  private transient AtomicReference<@Nullable Table> cachedTable;

  public static <T> BigQueryStorageTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      DataFormat format,
      @Nullable ValueProvider<List<String>> selectedFields,
      @Nullable ValueProvider<String> rowRestriction,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices,
      boolean projectionPushdownApplied) {
    return new BigQueryStorageTableSource<>(
        tableRefProvider,
        format,
        selectedFields,
        rowRestriction,
        parseFn,
        outputCoder,
        bqServices,
        projectionPushdownApplied);
  }

  public static <T> BigQueryStorageTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable ValueProvider<List<String>> selectedFields,
      @Nullable ValueProvider<String> rowRestriction,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageTableSource<>(
        tableRefProvider,
        null,
        selectedFields,
        rowRestriction,
        parseFn,
        outputCoder,
        bqServices,
        false);
  }

  private BigQueryStorageTableSource(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable DataFormat format,
      @Nullable ValueProvider<List<String>> selectedFields,
      @Nullable ValueProvider<String> rowRestriction,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices,
      boolean projectionPushdownApplied) {
    super(format, selectedFields, rowRestriction, parseFn, outputCoder, bqServices);
    this.tableReferenceProvider = checkNotNull(tableRefProvider, "tableRefProvider");
    this.projectionPushdownApplied = projectionPushdownApplied;
    cachedTable = new AtomicReference<>();
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    cachedTable = new AtomicReference<>();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder
        .addIfNotNull(
            DisplayData.item("table", BigQueryHelpers.displayTable(tableReferenceProvider))
                .withLabel("Table"))
        .addIfNotDefault(
            DisplayData.item("projectionPushdownApplied", projectionPushdownApplied)
                .withLabel("Projection Pushdown Applied"),
            false);

    if (selectedFieldsProvider != null && selectedFieldsProvider.isAccessible()) {
      builder.add(
          DisplayData.item("selectedFields", String.join(", ", selectedFieldsProvider.get()))
              .withLabel("Selected Fields"));
    }

    // Note: This transform does not set launchesBigQueryJobs because it doesn't launch
    // BigQuery jobs, but instead uses the storage api to directly read the table.
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    Table table = getTargetTable(options.as(BigQueryOptions.class));
    if (table != null) {
      return table.getNumBytes();
    }
    // If the table does not exist, then it will be null.
    // Avoid the NullPointerException here, allow a more meaningful table "not_found"
    // error to be shown to the user, upon table read.
    return 0;
  }

  @Override
  protected String getTargetTableId(BigQueryOptions options) throws Exception {
    TableReference tableReference = tableReferenceProvider.get();
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      checkState(
          !Strings.isNullOrEmpty(options.getProject()),
          "No project ID set in %s or %s, cannot construct a complete %s",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReference.class.getSimpleName());
      LOG.info(
          "Project ID not set in {}. Using default project from {}.",
          TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName());
      tableReference.setProjectId(options.getProject());
    }
    return String.format(
        "projects/%s/datasets/%s/tables/%s",
        tableReference.getProjectId(), tableReference.getDatasetId(), tableReference.getTableId());
  }

  @Override
  protected Table getTargetTable(BigQueryOptions options) throws Exception {
    Table maybeTable = cachedTable.get();
    if (maybeTable != null) {
      return maybeTable;
    } else {
      TableReference tableReference = tableReferenceProvider.get();
      if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
        checkState(
            !Strings.isNullOrEmpty(options.getProject()),
            "No project ID set in %s or %s, cannot construct a complete %s",
            TableReference.class.getSimpleName(),
            BigQueryOptions.class.getSimpleName(),
            TableReference.class.getSimpleName());
        LOG.info(
            "Project ID not set in {}. Using default project from {}.",
            TableReference.class.getSimpleName(),
            BigQueryOptions.class.getSimpleName());
        tableReference.setProjectId(
            options.getBigQueryProject() == null
                ? options.getProject()
                : options.getBigQueryProject());
      }
      try (DatasetService datasetService = bqServices.getDatasetService(options)) {
        Table table = datasetService.getTable(tableReference);
        if (table == null) {
          throw new IllegalArgumentException("Table not found" + table);
        }
        cachedTable.compareAndSet(null, table);
        return table;
      }
    }
  }
}

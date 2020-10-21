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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing reading from a table. */
public class BigQueryStorageTableSource<T> extends BigQueryStorageSourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageTableSource.class);

  public static <T> BigQueryStorageTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable ValueProvider<List<String>> selectedFields,
      @Nullable ValueProvider<String> rowRestriction,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageTableSource<>(
        tableRefProvider, selectedFields, rowRestriction, parseFn, outputCoder, bqServices);
  }

  private final ValueProvider<TableReference> tableReferenceProvider;

  private transient AtomicReference<Table> cachedTable;

  private BigQueryStorageTableSource(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable ValueProvider<List<String>> selectedFields,
      @Nullable ValueProvider<String> rowRestriction,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    super(selectedFields, rowRestriction, parseFn, outputCoder, bqServices);
    this.tableReferenceProvider = checkNotNull(tableRefProvider, "tableRefProvider");
    cachedTable = new AtomicReference<>();
  }

  private void readObject(ObjectInputStream in) throws ClassNotFoundException, IOException {
    in.defaultReadObject();
    cachedTable = new AtomicReference<>();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.addIfNotNull(
        DisplayData.item("table", BigQueryHelpers.displayTable(tableReferenceProvider))
            .withLabel("Table"));
    // Note: This transform does not set launchesBigQueryJobs because it doesn't launch
    // BigQuery jobs, but instead uses the storage api to directly read the table.
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return getTargetTable(options.as(BigQueryOptions.class)).getNumBytes();
  }

  @Override
  protected Table getTargetTable(BigQueryOptions options) throws Exception {
    if (cachedTable.get() == null) {
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
      Table table = bqServices.getDatasetService(options).getTable(tableReference);
      cachedTable.compareAndSet(null, table);
    }

    return cachedTable.get();
  }
}

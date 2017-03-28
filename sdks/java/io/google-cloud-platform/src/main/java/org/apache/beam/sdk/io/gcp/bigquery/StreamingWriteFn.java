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
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableSchemaToJsonSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;

/**
 * Implementation of DoFn to perform streaming BigQuery write.
 */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn
    extends DoFn<KV<ShardedKey<String>, TableRowInfo>, Void> {
  /** TableSchema in JSON. Use String to make the class Serializable. */
  @Nullable
  private final ValueProvider<String> jsonTableSchema;

  @Nullable private final String tableDescription;

  private final BigQueryServices bqServices;

  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<TableRow>> tableRows;

  private final Write.CreateDisposition createDisposition;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  /** The list of tables created so far, so we don't try the creation
      each time. */
  private static Set<String> createdTables =
      Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = Metrics.counter(StreamingWriteFn.class, "ByteCount");

  /** Constructor. */
  StreamingWriteFn(@Nullable ValueProvider<TableSchema> schema,
                   Write.CreateDisposition createDisposition,
                   @Nullable String tableDescription, BigQueryServices bqServices) {
    this.jsonTableSchema = schema == null ? null :
        NestedValueProvider.of(schema, new TableSchemaToJsonSchema());
    this.createDisposition = createDisposition;
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.tableDescription = tableDescription;
  }

  /**
   * Clear the cached map of created tables. Used for testing.
   */
  static void clearCreatedTables() {
    synchronized (createdTables) {
      createdTables.clear();
    }
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle(Context context) {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(ProcessContext context) {
    String tableSpec = context.element().getKey().getKey();
    List<TableRow> rows = BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds = BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows,
        tableSpec);

    rows.add(context.element().getValue().tableRow);
    uniqueIds.add(context.element().getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(Context context) throws Exception {
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);

    for (Map.Entry<String, List<TableRow>> entry : tableRows.entrySet()) {
      TableReference tableReference = getOrCreateTable(options, entry.getKey());
      flushRows(tableReference, entry.getValue(),
          uniqueIdsForTableRows.get(entry.getKey()), options);
    }
    tableRows.clear();
    uniqueIdsForTableRows.clear();
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    builder
        .addIfNotNull(DisplayData.item("schema", jsonTableSchema)
          .withLabel("Table Schema"))
        .addIfNotNull(DisplayData.item("tableDescription", tableDescription)
          .withLabel("Table Description"));
  }

  public TableReference getOrCreateTable(BigQueryOptions options, String tableSpec)
      throws InterruptedException, IOException {
    TableReference tableReference = BigQueryHelpers.parseTableSpec(tableSpec);
    if (createDisposition != createDisposition.CREATE_NEVER
        && !createdTables.contains(tableSpec)) {
      synchronized (createdTables) {
        // Another thread may have succeeded in creating the table in the meanwhile, so
        // check again. This check isn't needed for correctness, but we add it to prevent
        // every thread from attempting a create and overwhelming our BigQuery quota.
        DatasetService datasetService = bqServices.getDatasetService(options);
        if (!createdTables.contains(tableSpec)) {
          if (datasetService.getTable(tableReference) == null) {
            TableSchema tableSchema = BigQueryIO.JSON_FACTORY.fromString(
                jsonTableSchema.get(), TableSchema.class);
            datasetService.createTable(
                new Table()
                    .setTableReference(tableReference)
                    .setSchema(tableSchema)
                    .setDescription(tableDescription));
          }
          createdTables.add(tableSpec);
        }
      }
    }
    return tableReference;
  }

  /**
   * Writes the accumulated rows into BigQuery with streaming API.
   */
  private void flushRows(TableReference tableReference,
      List<TableRow> tableRows, List<String> uniqueIds, BigQueryOptions options)
          throws InterruptedException {
    if (!tableRows.isEmpty()) {
      try {
        long totalBytes = bqServices.getDatasetService(options).insertAll(
            tableReference, tableRows, uniqueIds);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}

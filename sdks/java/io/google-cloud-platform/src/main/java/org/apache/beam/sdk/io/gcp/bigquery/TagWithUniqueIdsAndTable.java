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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.JsonTableRefToTableRef;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToTableSpec;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Fn that tags each table row with a unique id and destination table.
 * To avoid calling UUID.randomUUID() for each element, which can be costly,
 * a randomUUID is generated only once per bucket of data. The actual unique
 * id is created by concatenating this randomUUID with a sequential number.
 */
@VisibleForTesting
class TagWithUniqueIdsAndTable<T>
    extends DoFn<T, KV<ShardedKey<String>, TableRowInfo>> {
  /** TableSpec to write to. */
  private final ValueProvider<String> tableSpec;

  /** User function mapping windowed values to {@link TableReference} in JSON. */
  private final SerializableFunction<ValueInSingleWindow<T>, TableReference> tableRefFunction;

  /** User function mapping user type to a TableRow. */
  private final SerializableFunction<T, TableRow> formatFunction;

  private transient String randomUUID;
  private transient long sequenceNo = 0L;

  TagWithUniqueIdsAndTable(BigQueryOptions options,
                           ValueProvider<TableReference> table,
                           SerializableFunction<ValueInSingleWindow<T>, TableReference>
                               tableRefFunction,
                           SerializableFunction<T, TableRow> formatFunction) {
    checkArgument(table == null ^ tableRefFunction == null,
        "Exactly one of table or tableRefFunction should be set");
    if (table != null) {
      if (table.isAccessible() && Strings.isNullOrEmpty(table.get().getProjectId())) {
        TableReference tableRef = table.get()
            .setProjectId(options.as(BigQueryOptions.class).getProject());
        table = NestedValueProvider.of(
            StaticValueProvider.of(BigQueryHelpers.toJsonString(tableRef)),
            new JsonTableRefToTableRef());
      }
      this.tableSpec = NestedValueProvider.of(table, new TableRefToTableSpec());
    } else {
      tableSpec = null;
    }
    this.tableRefFunction = tableRefFunction;
    this.formatFunction = formatFunction;
  }


  @StartBundle
  public void startBundle(Context context) {
    randomUUID = UUID.randomUUID().toString();
  }

  /** Tag the input with a unique id. */
  @ProcessElement
  public void processElement(ProcessContext context, BoundedWindow window) throws IOException {
    String uniqueId = randomUUID + sequenceNo++;
    ThreadLocalRandom randomGenerator = ThreadLocalRandom.current();
    String tableSpec = tableSpecFromWindowedValue(
        context.getPipelineOptions().as(BigQueryOptions.class),
        ValueInSingleWindow.of(context.element(), context.timestamp(), window, context.pane()));
    // We output on keys 0-50 to ensure that there's enough batching for
    // BigQuery.
    context.output(KV.of(ShardedKey.of(tableSpec, randomGenerator.nextInt(0, 50)),
        new TableRowInfo(formatFunction.apply(context.element()), uniqueId)));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);

    builder.addIfNotNull(DisplayData.item("table", tableSpec));
    if (tableRefFunction != null) {
      builder.add(DisplayData.item("tableFn", tableRefFunction.getClass())
        .withLabel("Table Reference Function"));
    }
  }

  @VisibleForTesting
  ValueProvider<String> getTableSpec() {
    return tableSpec;
  }

  private String tableSpecFromWindowedValue(BigQueryOptions options,
                                            ValueInSingleWindow<T> value) {
    if (tableSpec != null) {
      return tableSpec.get();
    } else {
      TableReference table = tableRefFunction.apply(value);
      if (table.getProjectId() == null) {
        table.setProjectId(options.getProject());
      }
      return BigQueryHelpers.toTableSpec(table);
    }
  }
}

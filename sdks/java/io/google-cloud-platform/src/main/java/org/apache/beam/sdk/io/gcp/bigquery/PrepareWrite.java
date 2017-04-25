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

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Strings;
import java.io.IOException;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * Prepare an input {@link PCollection} for writing to BigQuery. Use the table function to determine
 * which tables each element is written to, and format the element into a {@link TableRow} using the
 * user-supplied format function.
 */
public class PrepareWrite<T>
    extends PTransform<PCollection<T>, PCollection<KV<TableDestination, TableRow>>> {
  private SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction;
  private SerializableFunction<T, TableRow> formatFunction;

  public PrepareWrite(
      SerializableFunction<ValueInSingleWindow<T>, TableDestination> tableFunction,
      SerializableFunction<T, TableRow> formatFunction) {
    this.tableFunction = tableFunction;
    this.formatFunction = formatFunction;
  }

  @Override
  public PCollection<KV<TableDestination, TableRow>> expand(PCollection<T> input) {
    return input.apply(
        ParDo.of(
            new DoFn<T, KV<TableDestination, TableRow>>() {
              @ProcessElement
              public void processElement(ProcessContext context, BoundedWindow window)
                  throws IOException {
                TableDestination tableDestination =
                    tableSpecFromWindowedValue(
                        context.getPipelineOptions().as(BigQueryOptions.class),
                        ValueInSingleWindow.of(
                            context.element(), context.timestamp(), window, context.pane()));
                TableRow tableRow = formatFunction.apply(context.element());
                context.output(KV.of(tableDestination, tableRow));
              }
            }));
  }

  private TableDestination tableSpecFromWindowedValue(
      BigQueryOptions options, ValueInSingleWindow<T> value) {
    TableDestination tableDestination = tableFunction.apply(value);
    TableReference tableReference = tableDestination.getTableReference();
    if (Strings.isNullOrEmpty(tableReference.getProjectId())) {
      tableReference.setProjectId(options.getProject());
      tableDestination =
          new TableDestination(tableReference, tableDestination.getTableDescription());
    }
    return tableDestination;
  }
}

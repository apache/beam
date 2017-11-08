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

package org.apache.beam.sdk.nexmark.sinks.bigquery;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.nexmark.NexmarkOptions;
import org.apache.beam.sdk.nexmark.NexmarkUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

/**
 * Writes to BigQuery table.
 * Used by {@link BigQueryResultsSink} to output Nexmark query execution results.
 */
public class BigQueryTableWriter extends PTransform<PCollection<String>, POutput> {

  private NexmarkOptions options;
  private String queryName;
  private String tableName;

  public BigQueryTableWriter(
      NexmarkOptions options,
      String queryName,
      String tableName) {

    this.options = options;
    this.queryName = queryName;
    this.tableName = tableName;
  }

  @Override
  public POutput expand(PCollection<String> input) {
    TableSchema tableSchema = getTableSchema();
    NexmarkUtils.console("Writing results to BigQuery table %s", tableName);
    BigQueryIO.Write io = createWriter(tableName, tableSchema);
    return input
        .apply(queryName + ".StringToTableRow", stringToTableRow())
        .apply(queryName + ".WriteBigQueryResults", io);
  }

  private BigQueryIO.Write createWriter(String tableSpec, TableSchema tableSchema) {
    return BigQueryIO
        .write()
        .to(tableSpec)
        .withSchema(tableSchema)
        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
  }

  private TableSchema getTableSchema() {
    return new TableSchema().setFields(ImmutableList.of(
        new TableFieldSchema().setName("result").setType("STRING"),
        new TableFieldSchema().setName("records").setMode("REPEATED").setType("RECORD")
            .setFields(ImmutableList.of(
                new TableFieldSchema().setName("index").setType("INTEGER"),
                new TableFieldSchema().setName("value").setType("STRING")))));
  }


  private ParDo.SingleOutput<String, TableRow> stringToTableRow() {
    return ParDo.of(new DoFn<String, TableRow>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        int n = ThreadLocalRandom.current().nextInt(10);
        List<TableRow> records = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
          records.add(new TableRow().set("index", i).set("value", Integer.toString(i)));
        }
        c.output(new TableRow().set("result", c.element()).set("records", records));
      }
    });
  }
}

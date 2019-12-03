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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigquery;

import java.util.List;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class BigQueryPerfTable extends BigQueryTable {
  private final String namespace;
  private final String metric;

  BigQueryPerfTable(Table table, ConversionOptions options, String namespace, String metric) {
    super(table, options);
    this.namespace = namespace;
    this.metric = metric;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return super.buildIOReader(begin).apply(ParDo.of(new RowMonitor(namespace, metric)));
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    return super.buildIOReader(begin, filters, fieldNames)
        .apply(ParDo.of(new RowMonitor(namespace, metric)));
  }

  /** Monitor that records the number of Fields in each Row read from an IO. */
  private static class RowMonitor extends DoFn<Row, Row> {

    private Counter totalRows;

    RowMonitor(String namespace, String name) {
      this.totalRows = Metrics.counter(namespace, name);
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      totalRows.inc(c.element().getFieldCount());
      c.output(c.element());
    }
  }
}

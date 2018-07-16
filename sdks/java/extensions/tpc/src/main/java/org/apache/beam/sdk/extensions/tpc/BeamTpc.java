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
package org.apache.beam.sdk.extensions.tpc;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.tpc.query.TpcHQuery;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.LoggerFactory;

/** Tpc Benchmark. */
public class BeamTpc {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BeamTpc.class);

  private static PCollectionTuple getHTables(
      Pipeline pipeline, CSVFormat csvFormat, TpcOptions tpcOptions) {
    ImmutableMap<String, Schema> hSchemas =
        ImmutableMap.<String, Schema>builder()
            .put("customer", SchemaUtil.customerSchema)
            .put("lineitem", SchemaUtil.lineitemSchema)
            .put("nation", SchemaUtil.nationSchema)
            .put("orders", SchemaUtil.orderSchema)
            .put("part", SchemaUtil.partSchema)
            .put("partsupp", SchemaUtil.partsuppSchema)
            .put("region", SchemaUtil.regionSchema)
            .put("supplier", SchemaUtil.supplierSchema)
            .build();

//    Monitor<Row> resultMonitor = new Monitor<Row>("tpc", "start");

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);

    for (Map.Entry<String, Schema> tableSchema : hSchemas.entrySet()) {
      String filePattern = tpcOptions.getInputFile() + tableSchema.getKey() + ".tbl";

      PCollection<Row> table =
          new TextTable(
                  SchemaUtil.nationSchema,
                  filePattern,
                  new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat),
                  new TextTableProvider.RowToCsv(csvFormat))
              .buildIOReader(pipeline.begin())
              .setCoder(tableSchema.getValue().getRowCoder());

//      table = table.apply(resultMonitor.getTransform());

      tables = tables.and(new TupleTag<>(tableSchema.getKey()), table);
    }

    return tables;
  }

  public static void main(String[] args) {
    // Option for lanunch Tpc benchmark.
    TpcOptions tpcOptions =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcOptions.class);

    Pipeline pipeline = Pipeline.create(tpcOptions);

    CSVFormat csvFormat = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

    PCollectionTuple tables = getHTables(pipeline, csvFormat, tpcOptions);

    String outputPath = tpcOptions.getOutput();
    System.out.println(tpcOptions.getInputFile());
    System.out.println(outputPath);

    Monitor<Row> resultMonitor = new Monitor<Row>("tpc", "query1");

    tables
        .apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + tpcOptions.getQuery(),
            SqlTransform.query(TpcHQuery.QUERYTEST))
        .apply(resultMonitor.getTransform())
        .apply(
            "exp_table",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    new SerializableFunction<Row, String>() {
                      @Override
                      public @Nullable String apply(Row input) {
                        // expect output:
                        //  PCOLLECTION: [3, row, 3.0]
                        System.out.println("row: " + input.getValues());
                        return "row: " + input.getValues();
                      }
                    }))
        .apply(TextIO.write().to(outputPath));

    tables.apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + "1",
            SqlTransform.query(TpcHQuery.QUERY1));

    tables.apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + "3",
            SqlTransform.query(TpcHQuery.QUERY3));

    tables.apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + "4",
            SqlTransform.query(TpcHQuery.QUERY4));

    tables.apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + "5",
            SqlTransform.query(TpcHQuery.QUERY5));

    tables.apply(
            "SqlTransform " + tpcOptions.getTable() + ":" + "6",
            SqlTransform.query(TpcHQuery.QUERY6));

    long startTs = System.currentTimeMillis();
    PipelineResult pipelineResult = pipeline.run();
    pipelineResult.waitUntilFinish();
    long endTs = System.currentTimeMillis();

    System.out.println("performance: " + ((endTs - startTs)/1000) + "s");

    TpcPerf perf = new TpcPerf();

    long count = perf.getCounterMetric(pipelineResult, "tpc", "query1.elements", -1);
    System.out.println("count row: " + count);
    LOG.info("count row: " + count);
    long time =
        perf.getDistributionMetric(
            pipelineResult, "tpc", "query1.timestamp", TpcPerf.DistributionType.MIN, -1);
    System.out.println("start time: " + time);
    LOG.info("start time: " + time);
  }
}

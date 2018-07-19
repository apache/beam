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
import org.apache.beam.sdk.extensions.tpc.query.TpcDsQuery;
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
            .put("customer", Schemas.getCustomerDsSchema)
//            .put("lineitem", Schemas.lineitemSchema)
//            .put("nation", Schemas.nationSchema)
//            .put("orders", Schemas.orderSchema)
//            .put("part", Schemas.partSchema)
//            .put("partsupp", Schemas.partsuppSchema)
//            .put("region", Schemas.regionSchema)
//            .put("supplier", Schemas.supplierSchema)
              .put("store_sales", Schemas.storeSalesSchema)
              .put("catalog_sales", Schemas.catalogSalesSchema)
              .put("item", Schemas.itemSchema)
              .put("date_dim", Schemas.dateDimSchema)
              .put("promotion", Schemas.promotionSchema)
              .put("customer_demographics", Schemas.customerDemographicsSchema)
              .put("web_sales", Schemas.webSalesSchema)
              .put("inventory", Schemas.inventorySchema)
            .build();

    PCollectionTuple tables = PCollectionTuple.empty(pipeline);

    for (Map.Entry<String, Schema> tableSchema : hSchemas.entrySet()) {
      String filePattern = tpcOptions.getInputFile() + tableSchema.getKey() + ".dat";

      PCollection<Row> table =
          new TextTable(
                  tableSchema.getValue(),
                  filePattern,
                  new TextTableProvider.CsvToRow(tableSchema.getValue(), csvFormat),
                  new TextTableProvider.RowToCsv(csvFormat))
              .buildIOReader(pipeline.begin())
              .setCoder(tableSchema.getValue().getRowCoder()).setName(tableSchema.getKey());

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
            "SqlTransform " + "DS" + ":" + "7",
            SqlTransform.query(TpcDsQuery.QUERY7))
//            SqlTransform.query(TpcDsQuery.QUERY7))
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

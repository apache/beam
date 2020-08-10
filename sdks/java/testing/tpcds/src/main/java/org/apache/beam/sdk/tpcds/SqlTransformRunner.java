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
package org.apache.beam.sdk.tpcds;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.*;
import org.apache.commons.csv.CSVFormat;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class executes jobs using PCollection and SqlTransform, it uses SqlTransform.query to run queries.
 */
public class SqlTransformRunner {
    private static final String dataDirectory = "gs://beamsql_tpcds_1/data";
    private static final String resultDirectory = "gs://beamsql_tpcds_1/tpcds_results";

    /**
     * Get all tables (in the form of TextTable) needed for a specific query execution
     * @param pipeline The pipeline that will be run to execute the query
     * @param csvFormat The csvFormat to construct readConverter (CsvToRow) and writeConverter (RowToCsv)
     * @param queryName The name of the query which will be executed (for example: query3, query55, query96)
     * @return A PCollectionTuple which is constructed by all tables needed for running query.
     * @throws Exception
     */
    private static PCollectionTuple getTables(Pipeline pipeline, CSVFormat csvFormat, String queryName) throws Exception {
        Map<String, Schema> schemaMap = TpcdsSchemas.getTpcdsSchemas();
        TpcdsOptions tpcdsOptions = pipeline.getOptions().as(TpcdsOptions.class);
        String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
        String queryString = QueryReader.readQuery(queryName);

        PCollectionTuple tables = PCollectionTuple.empty(pipeline);
        for (Map.Entry<String, Schema> tableSchema : schemaMap.entrySet()) {
            String tableName = tableSchema.getKey();

            // Only when queryString contains tableName, the table is relevant to this query and will be added. This can avoid reading unnecessary data files.
            if (queryString.contains(tableName)) {
                // This is location path where the data are stored
                String filePattern = dataDirectory + "/" + dataSize + "/" + tableName + ".dat";

                PCollection<Row> table =
                        new TextTable(
                                tableSchema.getValue(),
                                filePattern,
                                new CsvToRow(tableSchema.getValue(), csvFormat),
                                new RowToCsv(csvFormat))
                                .buildIOReader(pipeline.begin())
                                .setCoder(SchemaCoder.of(tableSchema.getValue()))
                                .setName(tableSchema.getKey());

                tables = tables.and(new TupleTag<>(tableName), table);
            }
        }
        return tables;
    }

    /**
     * This is the default method in BeamTpcds.main method. Run job using SqlTranform.query() method.
     * @param args Command line arguments
     * @throws Exception
     */
    public static void runUsingSqlTransform(String[] args) throws Exception {
        TpcdsOptions tpcdsOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

        String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
        String[] queryNameArr = TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptions);
        int nThreads = TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptions);

        // Using ExecutorService and CompletionService to fulfill multi-threading functionality
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        CompletionService<PipelineResult> completion = new ExecutorCompletionService<>(executor);

        // Make an array of pipelines, each pipeline is responsible for running a corresponding query.
        Pipeline[] pipelines = new Pipeline[queryNameArr.length];
        CSVFormat csvFormat = CSVFormat.MYSQL.withDelimiter('|').withNullString("");

        // Execute all queries, transform the each result into a PCollection<String>, write them into the txt file and store in a GCP directory.
        for (int i = 0; i < queryNameArr.length; i++) {
            // For each query, get a copy of pipelineOptions from command line arguments.
            TpcdsOptions tpcdsOptionsCopy = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

            // Cast tpcdsOptions as a BeamSqlPipelineOptions object to read and set queryPlanner (the default one is Calcite, can change to ZetaSQL).
            BeamSqlPipelineOptions beamSqlPipelineOptionsCopy = tpcdsOptionsCopy.as(BeamSqlPipelineOptions.class);

            // Finally, cast BeamSqlPipelineOptions as a DataflowPipelineOptions object to read and set other required pipeline optionsparameters .
            DataflowPipelineOptions dataflowPipelineOptionsCopy = beamSqlPipelineOptionsCopy.as(DataflowPipelineOptions.class);

            // Set a unique job name using the time stamp so that multiple different pipelines can run together.
            dataflowPipelineOptionsCopy.setJobName(queryNameArr[i] + "result" + System.currentTimeMillis());

            pipelines[i] = Pipeline.create(dataflowPipelineOptionsCopy);
            String queryString = QueryReader.readQuery(queryNameArr[i]);
            PCollectionTuple tables = getTables(pipelines[i], csvFormat, queryNameArr[i]);

            tables
                    .apply(
                            SqlTransform.query(queryString))
                    .apply(
                            MapElements.into(TypeDescriptors.strings()).via((Row row) -> row.toString()))
                    .apply(TextIO.write()
                            .to(resultDirectory + "/" + dataSize + "/" + pipelines[i].getOptions().getJobName())
                            .withSuffix(".txt")
                            .withNumShards(1));

            completion.submit(new TpcdsRun(pipelines[i]));
        }

        executor.shutdown();
    }
}

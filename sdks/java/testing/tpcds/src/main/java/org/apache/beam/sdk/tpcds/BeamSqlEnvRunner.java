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

import com.alibaba.fastjson.JSONObject;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class executes jobs using BeamSqlEnv, it uses BeamSqlEnv.executeDdl and BeamSqlEnv.parseQuery to run queries.
 */
public class BeamSqlEnvRunner {
    private static final String DATA_DIRECTORY = "gs://beamsql_tpcds_1/data";
    private static final String RESULT_DIRECTORY = "gs://beamsql_tpcds_1/tpcds_results";
    private static final String SUMMARY_START = "\n" + "TPC-DS Query Execution Summary:";
    private static final List<String> SUMMARY_HEADERS_LIST = Arrays.asList("Query Name", "Job Name", "Data Size", "Dialect", "Status", "Start Time", "End Time", "Elapsed Time(sec)");

    private static final Logger Log = LoggerFactory.getLogger(BeamSqlEnvRunner.class);

    private static String buildTableCreateStatement(String tableName) {
        String createStatement = "CREATE EXTERNAL TABLE " + tableName + " (%s) TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'";
        return createStatement;
    }

    private static String buildDataLocation(String dataSize, String tableName) {
        String dataLocation = DATA_DIRECTORY + "/" + dataSize + "/" + tableName + ".dat";
        return dataLocation;
    }

    /** Register all tables into BeamSqlEnv, set their schemas, and set the locations where their corresponding data are stored.
     *  Currently this method is not supported by ZetaSQL planner. */
    private static void registerAllTablesByBeamSqlEnv(BeamSqlEnv env, String dataSize) throws Exception {
        List<String> tableNames = TableSchemaJSONLoader.getAllTableNames();
        for (String tableName : tableNames) {
            String createStatement = buildTableCreateStatement(tableName);
            String tableSchema = TableSchemaJSONLoader.parseTableSchema(tableName);
            String dataLocation = buildDataLocation(dataSize, tableName);
            env.executeDdl(String.format(createStatement, tableSchema, dataLocation));
        }
    }

    /** Register all tables into InMemoryMetaStore, set their schemas, and set the locations where their corresponding data are stored. */
    private static void registerAllTablesByInMemoryMetaStore(InMemoryMetaStore inMemoryMetaStore, String dataSize) throws Exception {
        JSONObject properties = new JSONObject();
        properties.put("csvformat", "InformixUnload");

        Map<String, Schema> schemaMap = TpcdsSchemas.getTpcdsSchemas();
        for (String tableName : schemaMap.keySet()) {
            String dataLocation = DATA_DIRECTORY + "/" + dataSize + "/" + tableName + ".dat";
            Schema tableSchema = schemaMap.get(tableName);
            Table table = Table.builder().name(tableName).schema(tableSchema).location(dataLocation).properties(properties).type("text").build();
            inMemoryMetaStore.createTable(table);
        }
    }

    /**
     * Print the summary table after all jobs are finished.
     * @param completion A collection of all TpcdsRunResult that are from finished jobs.
     * @param numOfResults The number of results in the collection.
     * @throws Exception
     */
    private static void printExecutionSummary(CompletionService<TpcdsRunResult> completion, int numOfResults) throws Exception {
        List<List<String>> summaryRowsList = new ArrayList<>();
        for (int i = 0; i < numOfResults; i++) {
            TpcdsRunResult tpcdsRunResult = completion.take().get();
            List<String> list = new ArrayList<>();
            list.add(tpcdsRunResult.getQueryName());
            list.add(tpcdsRunResult.getJobName());
            list.add(tpcdsRunResult.getDataSize());
            list.add(tpcdsRunResult.getDialect());
            list.add(tpcdsRunResult.getIsSuccessful() ? "Successful" : "Failed");
            list.add(tpcdsRunResult.getIsSuccessful() ? tpcdsRunResult.getStartDate().toString() : "");
            list.add(tpcdsRunResult.getIsSuccessful() ? tpcdsRunResult.getEndDate().toString(): "");
            list.add(tpcdsRunResult.getIsSuccessful() ? Double.toString(tpcdsRunResult.getElapsedTime()) : "");
            summaryRowsList.add(list);
        }

        System.out.println(SUMMARY_START);
        System.out.println(SummaryGenerator.generateTable(SUMMARY_HEADERS_LIST, summaryRowsList));
    }

    /**
     * This is the alternative method in BeamTpcds.main method. Run job using BeamSqlEnv.parseQuery() method. (Doesn't perform well when running query96).
     * @param args Command line arguments
     * @throws Exception
     */
    public static void runUsingBeamSqlEnv(String[] args) throws Exception {
        InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
        inMemoryMetaStore.registerProvider(new TextTableProvider());

        TpcdsOptions tpcdsOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

        String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
        String[] queryNameArr = TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptions);
        int nThreads = TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptions);

        // Using ExecutorService and CompletionService to fulfill multi-threading functionality
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        CompletionService<TpcdsRunResult> completion = new ExecutorCompletionService<>(executor);

        // Directly create all tables and register them into inMemoryMetaStore before creating BeamSqlEnv object.
        registerAllTablesByInMemoryMetaStore(inMemoryMetaStore, dataSize);

        BeamSqlPipelineOptions beamSqlPipelineOptions = tpcdsOptions.as(BeamSqlPipelineOptions.class);
        BeamSqlEnv env =
                BeamSqlEnv
                        .builder(inMemoryMetaStore)
                        .setPipelineOptions(beamSqlPipelineOptions)
                        .setQueryPlannerClassName(beamSqlPipelineOptions.getPlannerName())
                        .build();

        // Make an array of pipelines, each pipeline is responsible for running a corresponding query.
        Pipeline[] pipelines = new Pipeline[queryNameArr.length];

        // Execute all queries, transform the each result into a PCollection<String>, write them into the txt file and store in a GCP directory.
        for (int i = 0; i < queryNameArr.length; i++) {
            // For each query, get a copy of pipelineOptions from command line arguments, cast tpcdsOptions as a DataflowPipelineOptions object to read and set required parameters for pipeline execution.
            TpcdsOptions tpcdsOptionsCopy = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);
            DataflowPipelineOptions dataflowPipelineOptionsCopy = tpcdsOptionsCopy.as(DataflowPipelineOptions.class);

            // Set a unique job name using the time stamp so that multiple different pipelines can run together.
            dataflowPipelineOptionsCopy.setJobName(queryNameArr[i] + "result" + System.currentTimeMillis());

            pipelines[i] = Pipeline.create(dataflowPipelineOptionsCopy);
            String queryString = QueryReader.readQuery(queryNameArr[i]);

            try {
                // Query execution
                PCollection<Row> rows = BeamSqlRelUtils.toPCollection(pipelines[i], env.parseQuery(queryString));

                // Transform the result from PCollection<Row> into PCollection<String>, and write it to the location where results are stored.
                PCollection<String> rowStrings = rows.apply(MapElements
                        .into(TypeDescriptors.strings())
                        .via((Row row) -> row.toString()));
                rowStrings.apply(TextIO.write().to(RESULT_DIRECTORY + "/" + dataSize + "/" + pipelines[i].getOptions().getJobName()).withSuffix(".txt").withNumShards(1));
            } catch (Exception e) {
                Log.error("{} failed to execute", queryNameArr[i]);
                e.printStackTrace();
            }

            completion.submit(new TpcdsRun(pipelines[i]));
        }

        executor.shutdown();

        printExecutionSummary(completion, queryNameArr.length);
    }
}

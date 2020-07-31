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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.meta.provider.text.TextTableProvider;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import java.util.List;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * To execute this main() method, run the following example command from the command line.
 *
 * ./gradlew :sdks:java:testing:tpcds:run -Ptpcds.args="--dataSize=1G \
 *         --queries=3,26,55 \
 *         --tpcParallel=2 \
 *         --project=apache-beam-testing \
 *         --stagingLocation=gs://beamsql_tpcds_1/staging \
 *         --tempLocation=gs://beamsql_tpcds_2/temp \
 *         --runner=DataflowRunner \
 *         --region=us-west1 \
 *         --maxNumWorkers=10"
 */
public class BeamTpcds {
    public static void main(String[] args) throws Exception {
        InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
        inMemoryMetaStore.registerProvider(new TextTableProvider());

        TpcdsOptions tpcdsOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);

        String dataSize = TpcdsParametersReader.getAndCheckDataSize(tpcdsOptions);
        String[] queryNameArr = TpcdsParametersReader.getAndCheckQueryNameArray(tpcdsOptions);
        int nThreads = TpcdsParametersReader.getAndCheckTpcParallel(tpcdsOptions);

        // Using ExecutorService and CompletionService to fulfill multi-threading functionality
        ExecutorService executor = Executors.newFixedThreadPool(nThreads);
        CompletionService<PipelineResult> completion = new ExecutorCompletionService<>(executor);

        // After getting necessary parameters from tpcdsOptions, cast tpcdsOptions as a DataflowPipelineOptions object to read and set required parameters for pipeline execution.
        DataflowPipelineOptions dataflowPipelineOptions = tpcdsOptions.as(DataflowPipelineOptions.class);

        BeamSqlEnv env =
                BeamSqlEnv
                        .builder(inMemoryMetaStore)
                        .setPipelineOptions(dataflowPipelineOptions)
                        .build();

        // Register all tables, set their schemas, and set the locations where their corresponding data are stored.
        List<String> tableNames = TableSchemaJSONLoader.getAllTableNames();
        for (String tableName : tableNames) {
            String createStatement = "CREATE EXTERNAL TABLE " + tableName + " (%s) TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'";
            String tableSchema = TableSchemaJSONLoader.parseTableSchema(tableName);
            String dataLocation = "gs://beamsql_tpcds_1/data/" + dataSize +"/" + tableName + ".dat";
            env.executeDdl(String.format(createStatement, tableSchema, dataLocation));
        }

        // Make an array of pipelines, each pipeline is responsible for running a corresponding query.
        Pipeline[] pipelines = new Pipeline[queryNameArr.length];

        // Execute all queries, transform the each result into a PCollection<String>, write them into the txt file and store in a GCP directory.
        for (int i = 0; i < queryNameArr.length; i++) {
            // For each query, get a copy of pipelineOptions from command line arguments, set a unique job name using the time stamp so that multiple different pipelines can run together.
            TpcdsOptions tpcdsOptionsCopy = PipelineOptionsFactory.fromArgs(args).withValidation().as(TpcdsOptions.class);
            DataflowPipelineOptions dataflowPipelineOptionsCopy = tpcdsOptionsCopy.as(DataflowPipelineOptions.class);
            dataflowPipelineOptionsCopy.setJobName(queryNameArr[i] + "result" + System.currentTimeMillis());

            pipelines[i] = Pipeline.create(dataflowPipelineOptionsCopy);
            String queryString = QueryReader.readQuery(queryNameArr[i]);

            // Query execution
            PCollection<Row> rows = BeamSqlRelUtils.toPCollection(pipelines[i], env.parseQuery(queryString));

            // Transform the result from PCollection<Row> into PCollection<String>, and write it to the location where results are stored.
            PCollection<String> rowStrings = rows.apply(MapElements
                    .into(TypeDescriptors.strings())
                    .via((Row row) -> row.toString()));
            rowStrings.apply(TextIO.write().to("gs://beamsql_tpcds_1/tpcds_results/" + dataSize + "/" + pipelines[i].getOptions().getJobName()).withSuffix(".txt").withNumShards(1));

            completion.submit(new TpcdsRun(pipelines[i]));
        }

        executor.shutdown();
    }
}

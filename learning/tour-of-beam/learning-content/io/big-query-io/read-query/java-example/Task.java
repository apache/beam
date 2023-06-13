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
/*
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// beam-playground:
//   name: read-query
//   description: BigQuery read query example.
//   multifile: false
//   context_line: 56
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DoubleCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        LOG.info("Running Task");
        System.setProperty("GOOGLE_APPLICATION_CREDENTIALS", "to\\path\\credential.json");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.setTempLocation("gs://bucket");
        options.as(BigQueryOptions.class).setProject("project-id");

        Pipeline pipeline = Pipeline.create(options);

        // pCollection.apply(BigQueryIO.read(... - This part of the pipeline reads from a BigQuery table using a SQL query and stores the result in a PCollection.
        // The BigQueryIO.read() function is used to read from BigQuery. It is configured with a lambda function to extract a field from each record.
        // The .fromQuery("SELECT field FROM project-id.dataset.table")
        // specifies the SQL query used to read from BigQuery. You should replace "field", "project-id", "dataset", and "table" with your specific field name, project id, dataset name, and table name, respectively.
/*
        PCollection<Double> pCollection = pipeline
                .apply(BigQueryIO.read(
                                (SchemaAndRecord elem) -> (Double) elem.getRecord().get("field"))
                        .fromQuery(
                                "SELECT field FROM `project-id.dataset.table`")
                        .usingStandardSql()
                        .withCoder(DoubleCoder.of()));
        pCollection
                .apply("Log words", ParDo.of(new LogOutput<>()));
*/

        pipeline.run();
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private final String prefix;

        LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
        }
    }
}
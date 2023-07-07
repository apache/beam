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
//   name: read-table
//   description: BigQueryIO read table example.
//   multifile: false
//   never_run: true
//   always_run: true
//   context_line: 56
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;


public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        LOG.info("Running Task");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        options.as(BigQueryOptions.class).setProject("apache-beam-testing");

        Pipeline pipeline = Pipeline.create(options);

        /*
         * BigQueryIO.readTableRows().from("bucket.project-id.table") reads from the specified BigQuery table, and outputs a
         * PCollection of TableRow objects. Each TableRow represents a row in the BigQuery table.
         * The .apply("Log words", ParDo.of(new LogOutput<>())) line applies a ParDo transform that logs each row. This is done using the LogOutput class, a custom DoFn (element-wise function).
         * LogOutput class: This is a custom DoFn that logs each element in the input PCollection. This is used to inspect the data in the pipeline for debugging or monitoring purposes.
         */

        PCollection<TableRow> pCollection = pipeline
                .apply("ReadFromBigQuery", BigQueryIO.readTableRows().from("clouddataflow-readonly:samples.weather_stations").withMethod(TypedRead.Method.DIRECT_READ));

        final PTransform<PCollection<TableRow>, PCollection<Iterable<TableRow>>> sample = Sample.fixedSizeGlobally(5);

        PCollection<TableRow> limitedPCollection = pCollection.apply(sample).apply(Flatten.iterables());


        limitedPCollection
                .apply("Log words", ParDo.of(new LogOutput<>()));



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
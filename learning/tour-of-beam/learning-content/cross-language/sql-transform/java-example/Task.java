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

// beam-playground:
//   name: sql-transform
//   description: Sql transform
//   multifile: false
//   context_line: 33
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);

        Schema schema = Schema.builder()
                .addField("id", Schema.FieldType.INT32)
                .addField("name", Schema.FieldType.STRING)
                .build();

        PCollection<Row> input = pipeline.apply(Create.of(
                Row.withSchema(schema).addValues(1, "Josh").build(),
                Row.withSchema(schema).addValues(103, "Anna").build()
        ).withRowSchema(schema));

        PCollection<Row> result = input
                .apply(SqlTransform.query("SELECT id, name FROM PCOLLECTION where id > 100"));

        result.apply(ParDo.of(new LogOutput<>("Sql result")));

        pipeline.run().waitUntilFinish();

    }

    static class LogOutput<T> extends DoFn<T, T> {

        private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);

        private final String prefix;

        public LogOutput() {
            this.prefix = "Processing element";
        }

        LogOutput(String prefix) {
            this.prefix = prefix;
        }

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info(prefix + ": {}", c.element());
            c.output(c.element());
        }
    }
}
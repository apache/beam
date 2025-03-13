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
//   name: FinalChallenge2
//   description: Final challenge 2.
//   multifile: true
//   files:
//     - name: analysis.csv
//   context_line: 50
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Task {
    private static final Logger LOG = LoggerFactory.getLogger(Task.class);
    private static final Integer WINDOW_TIME = 30;
    private static final Integer TIME_OUTPUT_AFTER_FIRST_ELEMENT = 5;
    private static final Integer ALLOWED_LATENESS_TIME = 1;

    private static final Schema schema = Schema.builder()
            .addField("word", Schema.FieldType.STRING)
            .addField("negative", Schema.FieldType.STRING)
            .addField("positive", Schema.FieldType.STRING)
            .addField("uncertainty", Schema.FieldType.STRING)
            .addField("litigious", Schema.FieldType.STRING)
            .addField("strong", Schema.FieldType.STRING)
            .addField("weak", Schema.FieldType.STRING)
            .addField("constraining", Schema.FieldType.STRING)
            .build();


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        runChallenge(options);
    }

    static void runChallenge(PipelineOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> shakespeare = getPCollection(pipeline);

        pipeline.run();
    }

    public static PCollection<String> getPCollection(Pipeline pipeline) {
        PCollection<String> rides = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"));
        return rides.apply(FlatMapElements.into(TypeDescriptors.strings()).via(line -> Arrays.asList(line.toLowerCase().split(" "))))
                .apply(Filter.by((String word) -> !word.isEmpty()));
    }

    public static PCollection<?> getAnalysisPCollection(Pipeline pipeline) {
        PCollection<String> words = pipeline.apply(TextIO.read().from("analysis.csv"));
        return words;
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
        public void processElement(ProcessContext c) {
            LOG.info(prefix + ": " + c.element());
            c.output(c.element());
        }
    }
}

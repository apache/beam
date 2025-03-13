// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// beam-playground:
//   name: group-by-key
//   description: GroupByKey example.
//   multifile: false
//   context_line: 43
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<String> input =
                pipeline.apply(
                        Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
                );

        // The applyTransform() converts [words] to [output]
        PCollection<KV<String, Iterable<String>>> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<KV<String, Iterable<String>>>()));

        pipeline.run();
    }

    // The method returns a map which key will be the first letter, and the values are a list of words
    static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
        return input
                .apply(MapElements.into(kvs(strings(), strings()))
                        .via(word -> KV.of(word.substring(0, 1), word)))

                .apply(GroupByKey.create());
    }

    static class LogOutput<T> extends DoFn<T, T> {
        private String prefix;

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
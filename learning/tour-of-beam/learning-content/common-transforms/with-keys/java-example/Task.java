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
//   name: With-keys
//   description: WithKeys example.
//   multifile: false
//   context_line: 46
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam


import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // Create input PCollection
        PCollection<String> input =
                pipeline.apply(
                        Create.of("apple", "banana", "cherry", "durian", "guava", "melon"));

        // The [words] filtered with the applyTransform()
        PCollection<KV<String, String>> output = applyTransform(input);

        output.apply("Log", ParDo.of(new LogOutput<KV<String,String>>("PCollection with-keys value")));

        pipeline.run();
    }

    // Thuis method groups the string collection with its first letter
    static PCollection<KV<String, String>> applyTransform(PCollection<String> input) {
        return input
                .apply(WithKeys.<String, String>of(fruit -> fruit.substring(0, 1))
                        .withKeyType(strings()));
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
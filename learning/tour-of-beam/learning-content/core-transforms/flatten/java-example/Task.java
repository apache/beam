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
//   name: flatten
//   description: Flatten example.
//   multifile: false
//   context_line: 40
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements start with a
        PCollection<String> wordsStartingWithA =
                pipeline.apply("Words starting with A",
                        Create.of("apple", "ant", "arrow")
                );

        // List of elements start with b
        PCollection<String> wordsStartingWithB =
                pipeline.apply("Words starting with B",
                        Create.of("ball", "book", "bow")
                );

        // The applyTransform() converts [wordsStartingWithA] and [wordsStartingWithB] to [output]
        PCollection<String> output = applyTransform(wordsStartingWithA, wordsStartingWithB);

        output.apply("Log", ParDo.of(new LogOutput<String>()));

        pipeline.run();
    }

    // The applyTransform two PCollection data types are the same combines and returns one PCollection
    static PCollection<String> applyTransform(
            PCollection<String> words1, PCollection<String> words2) {

        return PCollectionList.of(words1).and(words2)
                .apply(Flatten.pCollections());
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
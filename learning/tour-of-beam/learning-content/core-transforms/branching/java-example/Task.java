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

//   beam-playground:
//     name: branching
//     description: Branching example.
//     multifile: false
//     context_line: 41
//     categories:
//       - Quickstart
//     complexity: MEDIUM
//     tags:
//       - hellobeam

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> input =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        // The applyMultiply5Transform() converts [input] to [mult5Results]
        PCollection<Integer> mult5Results = applyMultiply5Transform(input);

        // The applyMultiply10Transform() converts [input] to [mult10Results]
        PCollection<Integer> mult10Results = applyMultiply10Transform(input);

        mult5Results.apply("Log multiplied by 5: ", ParDo.of(new LogOutput<Integer>("Multiplied by 5: ")));
        mult10Results.apply("Log multiplied by 10: ", ParDo.of(new LogOutput<Integer>("Multiplied by 10: ")));

        pipeline.run();
    }

    // The applyMultiply5Transform return PCollection with elements multiplied by 5
    static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 5", MapElements.into(integers()).via(num -> num * 5));
    }

    // The applyMultiply5Transform return PCollection with elements multiplied by 10
    static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {
        return input.apply("Multiply by 10", MapElements.into(integers()).via(num -> num * 10));
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
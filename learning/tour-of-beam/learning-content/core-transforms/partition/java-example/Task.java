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
//   name: partition
//   description: Partition example.
//   multifile: false
//   context_line: 42
//   categories:
//     - Quickstart
//   complexity: MEDIUM
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.transforms.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        // List of elements
        PCollection<Integer> input =
                pipeline.apply(
                        Create.of(1, 2, 3, 4, 5, 100, 110, 150, 250)
                );

        // The applyTransform() converts [input] to [partition]
        PCollectionList<Integer> partition = applyTransform(input);

        partition.get(0).apply("Log", ParDo.of(new LogOutput<Integer>("Number > 100: ")));
        partition.get(1).apply("Log", ParDo.of(new LogOutput<Integer>("Number <= 100: ")));

        pipeline.run();
    }

    // The applyTransform accepts PCollection and returns the PCollection array
    static PCollectionList<Integer> applyTransform(PCollection<Integer> input) {
        return input
                .apply(Partition.of(2,
                        (PartitionFn<Integer>) (number, numPartitions) -> {
                            if (number > 100) {
                                return 0;
                            } else {
                                return 1;
                            }
                        }));
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
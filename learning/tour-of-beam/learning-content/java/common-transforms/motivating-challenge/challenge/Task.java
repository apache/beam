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
//   name: CommonTransformsChallenge
//   description: Common Transforms motivating challenge.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam


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

        // List of elements
        PCollection<Integer> numbers =
                pipeline.apply(Create.of(12, -34, -1, 0, 93, -66, 53, 133, -133, 6, 13, 15));

        // // The [numbers] filtered with the positiveNumberFilter()
        // PCollection<Integer> filtered = getPositiveNumbers(numbers);

        // // Set key for each number
        // PCollection<KV<String,Integer>> getCollectionWithKey = setKeyForNumbers(filtered);

        // // Return count numbers
        // PCollection<KV<String,Long>> countPerKey = getCountPerKey(getCollectionWithKey);

        //countPerKey.apply("Log", ParDo.of(new LogOutput<KV<String,Long>>()));

        pipeline.run();
    }

    // //Write a method that returns positive numbers
    // static PCollection<Integer> getPositiveNumbers(PCollection<Integer> input) {
    //     return new PCollection<Integer>();
    
    // }

    // //Returns a map with a key that will not be odd or even , and the value will be the number itself at the input
    // static PCollection<KV<String, Integer>> setKeyForNumbers(PCollection<Integer> input) {
    //     return new PCollection<KV<String, Integer>>();
    // }

    // //Returns the count of numbers
    // static PCollection<KV<String,Long>> getCountPerKey(PCollection<KV<String, Integer>> input) {
    //     return new PCollection<KV<String, Long>>();
    // }

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
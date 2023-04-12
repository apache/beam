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

//   beam-playground:
//     name: CoreTransformsSolution2
//     description: Core Transforms second motivating challenge.
//     multifile: false
//     context_line: 44
//     categories:
//       - Quickstart
//     complexity: BASIC
//     tags:
//       - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.transforms.Flatten;

import java.util.Arrays;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        LOG.info("Running Task");
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> input = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/input_small_files/ascii_sort_1MB_input.0000000"));

        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);

        PCollection<String> limitedPCollection = input.apply(sample).apply(Flatten.iterables());

        PCollection<KV<String, Integer>> kvPCollection = getSplitLineAsMap(limitedPCollection);

        combine(kvPCollection).apply("Log words", ParDo.of(new LogOutput<>()));

        pipeline.run();
    }


    static PCollection<KV<String, Integer>> getSplitLineAsMap(PCollection<String> input) {
        return input.apply(FlatMapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.integers()))
                .via((SerializableFunction<String, Iterable<KV<String, Integer>>>) line -> {
                    String[] splitLine = line.split(":");
                    String word = splitLine[0];
                    int count = Integer.parseInt(splitLine[1].trim());
                    return Arrays.asList(KV.of(word, count));
                }));
    }

    static PCollection<KV<String, Integer>> combine(PCollection<KV<String, Integer>> input) {
        return input.apply(Combine.perKey(new SumWordLetterCombineFn()));
    }

    static class SumWordLetterCombineFn extends Combine.BinaryCombineFn<Integer> {
        @Override
        public Integer apply(Integer left, Integer right) {
            return left + right;
        }
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

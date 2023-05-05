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
//     name: CoreTransformsSolution1
//     description: Core Transforms first motivating challenge.
//     multifile: false
//     context_line: 49
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
import org.apache.beam.sdk.values.PCollectionList;
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

        PCollection<String> input = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));

        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(100);

        PCollection<String> limitedPCollection = input.apply(sample).apply(Flatten.iterables());

        PCollectionList<String> pCollectionList = partitionPCollectionByCase(limitedPCollection);

        PCollection<KV<String, Long>> allLetterUpperCase = countPerElement(pCollectionList.get(0));
        PCollection<KV<String, Long>> firstLetterUpperCase = countPerElement(pCollectionList.get(1));
        PCollection<KV<String, Long>> allLetterLowerCase = countPerElement(pCollectionList.get(2));

        PCollection<KV<String, Long>> newFirstPartPCollection = convertPCollectionToLowerCase(allLetterUpperCase);
        PCollection<KV<String, Long>> newSecondPartPCollection = convertPCollectionToLowerCase(firstLetterUpperCase);

        PCollection<KV<String, Long>> flattenPCollection = mergePCollections(newFirstPartPCollection, newSecondPartPCollection, allLetterLowerCase);

        PCollection<KV<String,Iterable<Long>>> groupPCollection = groupByKey(flattenPCollection);

        groupPCollection
                .apply("Log words", ParDo.of(new LogOutput<>()));


        pipeline.run();
    }

    static PCollection<KV<String, Long>> mergePCollections(PCollection<KV<String, Long>> input1, PCollection<KV<String, Long>> input2, PCollection<KV<String, Long>> input3) {
        return PCollectionList.of(input1).and(input2).and(input3).apply(Flatten.pCollections());
    }

    static PCollection<KV<String,Long>> countPerElement(PCollection<String> input) {
        return input.apply(Count.perElement());
    }

    static PCollection<KV<String, Long>> convertPCollectionToLowerCase(PCollection<KV<String, Long>> input) {
        return input.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                .via(word -> KV.of(word.getKey().toLowerCase(), word.getValue())));
    }


    static PCollectionList<String> partitionPCollectionByCase(PCollection<String> input) {
        return input
                .apply(Partition.of(3,
                        (word, numPartitions) -> {
                            if (word.equals(word.toUpperCase())) {
                                return 0;
                            } else if (Character.isUpperCase(word.charAt(0))) {
                                return 1;
                            } else {
                                return 2;
                            }
                        }));
    }

    static PCollection<KV<String,Iterable<Long>>> groupByKey(PCollection<KV<String, Long>> input) {
        return input.apply(GroupByKey.create());
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
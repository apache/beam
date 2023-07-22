/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
// beam-playground:
//   name: co-group-by-key
//   description: CoGroupByKey example.
//   multifile: false
//   context_line: 52
//   categories:
//     - Quickstart
//   complexity: ADVANCED
//   tags:
//     - hellobeam
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);


    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> fruits =
                pipeline.apply("Fruits",
                        Create.of("apple", "banana", "cherry")
                );

        PCollection<String> countries =
                pipeline.apply("Countries",
                        Create.of("australia", "brazil", "canada")
                );

        PCollection<String> output = applyTransform(fruits, countries);

        output.apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static PCollection<String> applyTransform(
            PCollection<String> fruits, PCollection<String> countries) {

        TupleTag<String> fruitsTag = new TupleTag<>();
        TupleTag<String> countriesTag = new TupleTag<>();

        MapElements<String, KV<String, String>> mapToAlphabetKv =
                MapElements.into(kvs(strings(), strings()))
                        .via(word -> KV.of(word.substring(0, 1), word));

        PCollection<KV<String, String>> fruitsPColl = fruits.apply("Fruit to KV", mapToAlphabetKv);
        PCollection<KV<String, String>> countriesPColl = countries
                .apply("Country to KV", mapToAlphabetKv);

        return KeyedPCollectionTuple
                .of(fruitsTag, fruitsPColl)
                .and(countriesTag, countriesPColl)

                .apply(CoGroupByKey.create())

                .apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

                    @ProcessElement
                    public void processElement(
                            @Element KV<String, CoGbkResult> element, OutputReceiver<String> out) {

                        String alphabet = element.getKey();
                        CoGbkResult coGbkResult = element.getValue();

                        String fruit = coGbkResult.getOnly(fruitsTag);
                        String country = coGbkResult.getOnly(countriesTag);

                        out.output(new WordsAlphabet(alphabet, fruit, country).toString());
                    }

                }));
    }

    static class WordsAlphabet {

        private String alphabet;
        private String fruit;
        private String country;

        public WordsAlphabet(String alphabet, String fruit, String country) {
            this.alphabet = alphabet;
            this.fruit = fruit;
            this.country = country;
        }

        @Override
        public String toString() {
            return "WordsAlphabet{" +
                    "alphabet='" + alphabet + '\'' +
                    ", fruit='" + fruit + '\'' +
                    ", country='" + country + '\'' +
                    '}';
        }

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
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
//   name: CoreTransformsSolution3
//   description: Core Transforms third motivating challenge.
//   multifile: false
//   context_line: 44
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);

    public static void main(String[] args) {
        LOG.info("Running Task");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> words = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));

        PCollection<String> countWords = words.apply(new CountWords());

        TupleTag<String> wordWithUpperCase = new TupleTag<String>() {
        };
        TupleTag<String> wordWithLowerCase = new TupleTag<String>() {
        };

        PCollectionTuple pCollectionTuple = createPCollectionTuple(countWords, wordWithUpperCase, wordWithLowerCase);

        PCollection<String> upperCaseWords = pCollectionTuple.get(wordWithUpperCase);

        PCollectionView<List<String>> lowerCaseWordsView = createView(pCollectionTuple.get(wordWithLowerCase));

        checkExistUpperWordsInLowerCaseView(upperCaseWords,lowerCaseWordsView).apply(ParDo.of(new LogOutput<>()));

        pipeline.run();
    }

    static class CountWords extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> input) {
            PCollection<String> wordsStartWithS = input.apply(
                    ParDo.of(new WordsStartWith("s")));

            return wordsStartWithS.apply(Count.perElement())
                    .apply(MapElements.into(TypeDescriptors.strings()).via(KV::getKey));
        }
    }

    static class WordsStartWith extends DoFn<String, String> {
        String letter;

        WordsStartWith(String letter) {
            this.letter = letter;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().toLowerCase().startsWith(letter)) {
                c.output(c.element());
            }
        }
    }


    static PCollection<String> checkExistUpperWordsInLowerCaseView(PCollection<String> upperCaseWords, PCollectionView<List<String>> lowerCaseWordsView) {
        return upperCaseWords.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String word, OutputReceiver<String> out, ProcessContext context) {
                List<String> upperCaseWords = context.sideInput(lowerCaseWordsView);
                if (upperCaseWords.contains(word.toLowerCase())) {
                    out.output(word);
                }
            }
        }).withSideInputs(lowerCaseWordsView));
    }

    static PCollectionView<List<String>> createView(PCollection<String> input) {
        return input.apply(View.asList());
    }

    static PCollectionTuple createPCollectionTuple(PCollection<String> wordsWithStartS, TupleTag<String> wordWithUpperCase, TupleTag<String> wordWithLowerCase) {
        return wordsWithStartS.apply(ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(@Element String word, MultiOutputReceiver out) {
                if (word.startsWith("S")) {
                    out.get(wordWithUpperCase).output(word);
                } else {
                    out.get(wordWithLowerCase).output(word);
                }
            }

        }).withOutputTags(wordWithUpperCase, TupleTagList.of(wordWithLowerCase)));
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
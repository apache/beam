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
//   name: TextIO
//   description: TextIO example.
//   multifile: false
//   context_line: 36
//   categories:
//     - Quickstart
//   complexity: BASIC
//   tags:
//     - hellobeam

package org.apache.beam.examples;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

    private static final Logger LOG = LoggerFactory.getLogger(Task.class);


    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        // Create the Pipeline object with the options we defined above
        Pipeline pipeline = Pipeline.create(options);

        // Concept #1. Read text file content line by line. resulting PCollection contains elements, where each element
        // contains a single line of text from the input file.
        PCollection<String> input = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(Filter.by((String line) -> !line.isEmpty()));

        // Concept #2. Output first 10 elements PCollection to the console.
        final PTransform<PCollection<String>, PCollection<Iterable<String>>> sample = Sample.fixedSizeGlobally(10);

        PCollection<String> sampleLines = input.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log lines", ParDo.of(new LogStrings()));


        //Concept #3. Read text file and split into PCollection of words.
        PCollection<String> words = pipeline.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
                .apply(FlatMapElements.into(TypeDescriptors.strings()).via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Filter.by((String word) -> !word.isEmpty()));

        PCollection<String> sampleWords = words.apply(sample)
                .apply(Flatten.iterables())
                .apply("Log words", ParDo.of(new LogStrings()));

        // Concept #4. Write PCollection to text file.
        sampleWords.apply(TextIO.write().to("sample-words"));
        pipeline.run().waitUntilFinish();
    }

    public static class LogStrings extends DoFn<String, String> {

        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
            LOG.info("Processing element: {}", c.element());
            c.output(c.element());
        }
    }

}

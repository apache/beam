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

package org.apache.beam.learning.katas.coretransforms.flattenWith;

// beam-playground:
//   name: Flatten
//   description: Task from katas that merges two PCollections of words into a single PCollection.
//   multifile: false
//   context_line: 47
//   categories:
//     - Combiners
//     - Flatten
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - join
//     - strings

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;


public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> wordsStartingWithA =
        pipeline.apply("Words starting with A",
            Create.of("apple", "ant", "arrow")
        );

    PCollection<String> wordsStartingWithB =
        pipeline.apply("Words starting with B",
            Create.of("ball", "book", "bow")
        );

    PCollection<String> output = applyTransform(wordsStartingWithA, wordsStartingWithB);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(
      PCollection<String> words1, PCollection<String> words2) {

    PTransform<PCollection<String>, PCollection<String>> flattenTransform = Flatten.with(words2);

    return words1
            .apply("Transform A to Uppercase",
                    MapElements.into(TypeDescriptors.strings())
                            .via((String word) -> word.toUpperCase()))
            .apply("Flatten with words2", flattenTransform);
  }

}
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

package org.apache.beam.learning.katas.coretransforms.groupbykey;

// beam-playground:
//   name: GroupByKeyKata
//   description: Task from katas that groups words by its first letter.
//   multifile: false
//   context_line: 43
//   categories:
//     - Combiners
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transform
//     - map
//     - strings

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> words =
        pipeline.apply(
            Create.of("apple", "ball", "car", "bear", "cheetah", "ant")
        );

    PCollection<KV<String, Iterable<String>>> output = applyTransform(words);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<KV<String, Iterable<String>>> applyTransform(PCollection<String> input) {
    return input
        .apply(MapElements.into(kvs(strings(), strings()))
            .via(word -> KV.of(word.substring(0, 1), word)))

        .apply(GroupByKey.create());
  }

}
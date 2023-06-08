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

package org.apache.beam.learning.katas.commontransforms.withkeys;

// beam-playground:
//   name: WithKeys
//   description: Task from katas to convert each fruit name into a KV of its first letter and itself.
//   multifile: false
//   context_line: 46
//   categories:
//     - Combiners
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> words =
        pipeline.apply(Create.of("apple", "banana", "cherry", "durian", "guava", "melon"));

    PCollection<KV<String, String>> output = applyTransform(words);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<KV<String, String>> applyTransform(PCollection<String> input) {
    return input
        .apply(WithKeys.<String, String>of(fruit -> fruit.substring(0, 1))
            .withKeyType(strings()));
  }

}
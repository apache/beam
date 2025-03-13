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

package org.apache.beam.learning.katas.coretransforms.branching;

// beam-playground:
//   name: Branching
//   description: Task from katas to branch out the numbers to two different transforms, one transform
//     is multiplying each number by 5 and the other transform is multiplying each number by 10.
//   multifile: false
//   context_line: 48
//   categories:
//     - Branching
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - branch
//     - transforms
//     - numbers

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers =
        pipeline.apply(Create.of(1, 2, 3, 4, 5));

    PCollection<Integer> mult5Results = applyMultiply5Transform(numbers);
    PCollection<Integer> mult10Results = applyMultiply10Transform(numbers);

    mult5Results.apply("Log multiply 5", Log.ofElements("Multiplied by 5: "));
    mult10Results.apply("Log multiply 10", Log.ofElements("Multiplied by 10: "));

    pipeline.run();
  }

  static PCollection<Integer> applyMultiply5Transform(PCollection<Integer> input) {
    return input.apply("Multiply by 5", MapElements.into(integers()).via(num -> num * 5));
  }

  static PCollection<Integer> applyMultiply10Transform(PCollection<Integer> input) {
    return input.apply("Multiply by 10", MapElements.into(integers()).via(num -> num * 10));
  }

}
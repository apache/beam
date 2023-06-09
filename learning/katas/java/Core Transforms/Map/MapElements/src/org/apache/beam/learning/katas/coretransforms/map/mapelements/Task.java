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

package org.apache.beam.learning.katas.coretransforms.map.mapelements;

// beam-playground:
//   name: Map
//   description: Task from katas to implement a simple map function that multiplies all input elements by 5.
//   multifile: false
//   context_line: 45
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - map
//     - numbers

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers =
        pipeline.apply(Create.of(10, 20, 30, 40, 50));

    PCollection<Integer> output = applyTransform(numbers);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Integer> applyTransform(PCollection<Integer> input) {
    return input.apply(
        MapElements.into(TypeDescriptors.integers())
            .via(number -> number * 5)
    );
  }

}
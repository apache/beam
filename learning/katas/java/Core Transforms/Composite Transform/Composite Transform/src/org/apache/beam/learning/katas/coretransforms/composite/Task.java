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

package org.apache.beam.learning.katas.coretransforms.composite;

// beam-playground:
//   name: CompositeTransform
//   description: Task from katas to implement a composite transform "ExtractAndMultiplyNumbers"
//     that extracts numbers from comma separated line and then multiplies each number by 10.
//   multifile: false
//   context_line: 54
//   categories:
//     - Combiners
//     - Flatten
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - count
//     - map
//     - transforms
//     - numbers

import static org.apache.beam.sdk.values.TypeDescriptors.integers;

import java.util.Arrays;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of("1,2,3,4,5", "6,7,8,9,10"))
        .apply(new ExtractAndMultiplyNumbers())
        .apply(Log.ofElements());

    pipeline.run();
  }

  static class ExtractAndMultiplyNumbers
      extends PTransform<PCollection<String>, PCollection<Integer>> {

    @Override
    public PCollection<Integer> expand(PCollection<String> input) {
      return input
          .apply(ParDo.of(new DoFn<String, Integer>() {

            @ProcessElement
            public void processElement(@Element String numbers, OutputReceiver<Integer> out) {
              Arrays.stream(numbers.split(","))
                  .forEach(numStr -> out.output(Integer.parseInt(numStr)));
            }

          }))

          .apply(MapElements.into(integers()).via(number -> number * 10));
    }

  }
}
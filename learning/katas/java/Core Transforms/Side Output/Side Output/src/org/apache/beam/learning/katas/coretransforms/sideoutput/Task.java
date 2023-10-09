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

package org.apache.beam.learning.katas.coretransforms.sideoutput;

// beam-playground:
//   name: SideOutput
//   description: Task from katas to implement additional output to your ParDo for numbers bigger than 100.
//   multifile: false
//   context_line: 50
//   categories:
//     - Combiners
//     - Core Transforms
//     - Multiple Outputs
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers
//     - output

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers = pipeline.apply(Create.of(10, 50, 120, 20, 200, 0));

    TupleTag<Integer> numBelow100Tag = new TupleTag<Integer>() {};
    TupleTag<Integer> numAbove100Tag = new TupleTag<Integer>() {};

    PCollectionTuple outputTuple = applyTransform(numbers, numBelow100Tag, numAbove100Tag);

    outputTuple.get(numBelow100Tag).apply(Log.ofElements("Number <= 100: "));
    outputTuple.get(numAbove100Tag).apply(Log.ofElements("Number > 100: "));

    pipeline.run();
  }

  static PCollectionTuple applyTransform(
      PCollection<Integer> numbers, TupleTag<Integer> numBelow100Tag,
      TupleTag<Integer> numAbove100Tag) {

    return numbers.apply(ParDo.of(new DoFn<Integer, Integer>() {

      @ProcessElement
      public void processElement(@Element Integer number, MultiOutputReceiver out) {
        if (number <= 100) {
          out.get(numBelow100Tag).output(number);
        } else {
          out.get(numAbove100Tag).output(number);
        }
      }

    }).withOutputTags(numBelow100Tag, TupleTagList.of(numAbove100Tag)));
  }

}
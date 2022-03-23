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

package org.apache.beam.learning.katas.coretransforms.map.flatmapelements;

//  eam-playground:
//   name: FlatMap
//   description: Task from katas to implement a function that maps each input sentence
//     into words split by whitespace (" ").
//   multifile: false
//   categories:
//     - Flatten

import java.util.Arrays;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> sentences =
        pipeline.apply(Create.of("Apache Beam", "Unified Batch and Streaming"));

    PCollection<String> output = applyTransform(sentences);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(PCollection<String> input) {
    return input.apply(
        FlatMapElements.into(TypeDescriptors.strings())
            .via(sentence -> Arrays.asList(sentence.split(" ")))
    );
  }
}
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

package org.apache.beam.learning.katas.coretransforms.map.pardoonetomany;

// beam-playground:
//   name: MapParDoOneToMany
//   description: Task from katas that maps each input sentence into words split by whitespace (" ").
//   multifile: false
//   context_line: 39
//   categories:
//     - Flatten
//     - Core Transforms

import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> sentences =
        pipeline.apply(Create.of("Hello Beam", "It is awesome"));

    PCollection<String> output = applyTransform(sentences);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(PCollection<String> input) {
    return input.apply(ParDo.of(new DoFn<String, String>() {

      @ProcessElement
      public void processElement(@Element String sentence, OutputReceiver<String> out) {
        String[] words = sentence.split(" ");

        for (String word : words) {
          out.output(word);
        }
      }

    }));
  }

}
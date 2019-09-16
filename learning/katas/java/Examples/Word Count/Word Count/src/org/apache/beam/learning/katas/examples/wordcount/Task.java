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

package org.apache.beam.learning.katas.examples.wordcount;

import java.util.Arrays;
import org.apache.beam.learning.katas.util.Log;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Task {

  public static void main(String[] args) {
    String[] lines = {
        "apple orange grape banana apple banana",
        "banana orange banana papaya"
    };

    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> wordCounts =
        pipeline.apply(Create.of(Arrays.asList(lines)));

    PCollection<String> output = applyTransform(wordCounts);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<String> applyTransform(PCollection<String> input) {
    return input

        .apply(FlatMapElements.into(TypeDescriptors.strings())
            .via(line -> Arrays.asList(line.split(" "))))

        .apply(Count.perElement())

        .apply(ParDo.of(new DoFn<KV<String, Long>, String>() {

          @ProcessElement
          public void processElement(
              @Element KV<String, Long> element, OutputReceiver<String> out) {

            out.output(element.getKey() + ":" + element.getValue());
          }

        }));
  }

}
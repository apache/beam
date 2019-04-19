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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import util.Log;

class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers = pipeline.apply(Create.of(10, 30, 50, 70, 90));

    PCollection<Integer> output = applyTransform(numbers);

    output.apply(Log.ofElements());

    pipeline.run();
  }

  static PCollection<Integer> applyTransform(PCollection<Integer> input) {
    return input.apply(Combine.globally(new SumIntegerFn()));
  }

  static class SumIntegerFn implements SerializableFunction<Iterable<Integer>, Integer> {

    @Override
    public Integer apply(Iterable<Integer> input) {
      int sum = 0;

      for (int item : input) {
        sum += item;
      }

      return sum;
    }

  }

}
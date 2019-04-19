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
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.Partition.PartitionFn;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import util.Log;

class Task {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Integer> numbers =
        pipeline.apply(
            Create.of(1, 2, 3, 4, 5, 100, 110, 150, 250)
        );

    PCollectionList<Integer> partition = applyTransform(numbers);

    partition.get(0).apply(Log.ofElements("Number > 100: "));
    partition.get(1).apply(Log.ofElements("Number <= 100: "));

    pipeline.run();
  }

  static PCollectionList<Integer> applyTransform(PCollection<Integer> input) {
    return input
        .apply(Partition.of(2,
            (PartitionFn<Integer>) (number, numPartitions) -> {
              if (number > 100) {
                return 0;
              } else {
                return 1;
              }
            }));
  }

}
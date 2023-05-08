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
package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: CoGroupByKey
//   description: Demonstration of CoGroupByKey transform usage.
//   multifile: false
//   default_example: false
//   context_line: 54
//   categories:
//     - Core Transforms
//     - Joins
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings
//     - integers
//     - tuples
//     - pairs
//     - group

public class CoGroupByKeyExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    // [START main_section]
    // Create pairs
    PCollection<KV<String, Integer>> pt1 =
        pipeline.apply(Create.of(KV.of("a", 1), KV.of("b", 2), KV.of("b", 3), KV.of("c", 4)));
    PCollection<KV<String, String>> pt2 =
        pipeline.apply(
            Create.of(
                KV.of("a", "apple"),
                KV.of("a", "avocado"),
                KV.of("b", "banana"),
                KV.of("c", "cherry")));

    final TupleTag<Integer> t1 = new TupleTag<>();
    final TupleTag<String> t2 = new TupleTag<>();
    PCollection<KV<String, CoGbkResult>> result =
        KeyedPCollectionTuple.of(t1, pt1).and(t2, pt2).apply(CoGroupByKey.create());
    // [END main_section]
    result.apply(ParDo.of(new LogOutput<>("PCollection pairs after CoGroupByKey transform: ")));
    pipeline.run();
  }

  static class LogOutput<T> extends DoFn<T, T> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);
    private final String prefix;

    public LogOutput(String prefix) {
      this.prefix = prefix;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      LOG.info(prefix + c.element());
      c.output(c.element());
    }
  }
}

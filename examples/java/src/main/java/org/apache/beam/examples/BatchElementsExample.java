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

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.BatchElements;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: BatchElements
//   description: Demonstration of BatchElements transform usage.
//   multifile: false
//   default_example: false
//   context_line: 47
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - batch

public class BatchElementsExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();

    Pipeline pipeline = Pipeline.create(options);

    // [START main_section]
    // Create input

    PCollection<String> inputs =
        pipeline.apply(Create.of("apple", "strawberry", "orange", "peach", "cherry", "pear"));

    // Create Batch Config
    BatchElements.BatchConfig config =
        BatchElements.BatchConfig.builder().withMinBatchSize(2).withMaxBatchSize(4).build();
    // Batch Elements
    PCollection<List<String>> result = inputs.apply(BatchElements.withConfig(config));
    // [END main_section]
    result.apply(ParDo.of(new LogOutput()));
    pipeline.run();
  }

  static class LogOutput extends DoFn<List<String>, String> {
    private static final Logger LOG = LoggerFactory.getLogger(LogOutput.class);

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      List<String> batch = c.element();

      LOG.info("Batch Contents: {}", batch);

      for (String element : batch) {
        c.output(element);
      }
    }
  }
}

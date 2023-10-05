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
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Latest
//   description: Demonstration of Latest transform usage.
//   multifile: false
//   default_example: false
//   context_line: 49
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - timestamps
//     - latest

public class LatestExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // [START main_section]
    Instant baseInstant = Instant.now().minus(Duration.standardSeconds(10));

    // Create collection
    PCollection<Integer> numbers = pipeline.apply(Create.of(5, 4, 3, 2, 1));

    // Add Timestamps for elements based on elements values. Largest element will be
    // the latest.
    PCollection<Integer> withTimestamps =
        numbers.apply(
            WithTimestamps.of(duration -> baseInstant.plus(Duration.standardSeconds(duration))));

    // Get the latest element from collection without timestamps. It will vary from
    // run to run
    PCollection<Integer> latest = numbers.apply(Latest.globally());

    // Get the latest element from collection with timestamps. Should always be 5
    PCollection<Integer> latestTimestamped = withTimestamps.apply(Latest.globally());
    // [END main_section]

    latest.apply(ParDo.of(new LogOutput<>("Latest element (without timestamps): ")));
    latestTimestamped.apply(ParDo.of(new LogOutput<>("Latest element (with timestamps): ")));
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

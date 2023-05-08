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

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: Window
//   description: Demonstration of Window transform usage.
//   multifile: false
//   default_example: false
//   context_line: 54
//   categories:
//     - Core Transforms
//     - Windowing
//   complexity: BASIC
//   tags:
//     - transforms
//     - strings
//     - timestamps
//     - windows

public class WindowExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // [START main_section]
    // Create some input data with timestamps
    List<String> inputData = Arrays.asList("foo", "bar", "foo", "foo");
    List<Long> timestamps =
        Arrays.asList(
            Duration.standardSeconds(15).getMillis(),
            Duration.standardSeconds(30).getMillis(),
            Duration.standardSeconds(45).getMillis(),
            Duration.standardSeconds(90).getMillis());

    // Create a PCollection from the input data with timestamps
    PCollection<String> items = pipeline.apply(Create.timestamped(inputData, timestamps));

    // Create a windowed PCollection
    PCollection<String> windowedItems =
        items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

    PCollection<KV<String, Long>> windowedCounts = windowedItems.apply(Count.perElement());
    // [END main_section]
    windowedCounts.apply(ParDo.of(new LogOutput<>("PCollection elements after Count transform: ")));
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

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

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: View
//   description: Demonstration of View transform usage.
//   multifile: false
//   default_example: false
//   context_line: 49
//   categories:
//     - Core Transforms
//   complexity: MEDIUM
//   tags:
//     - transforms
//     - views
//     - pairs

public class ViewExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // [START main_section]
    // List of elements
    PCollection<KV<String, String>> citiesToCountries =
        pipeline.apply(
            "Cities and Countries",
            Create.of(
                KV.of("Beijing", "China"),
                KV.of("London", "United Kingdom"),
                KV.of("San Francisco", "United States"),
                KV.of("Singapore", "Singapore"),
                KV.of("Sydney", "Australia")));

    PCollectionView<Map<String, String>> citiesToCountriesView =
        citiesToCountries.apply(View.asMap());

    PCollection<KV<String, String>> persons =
        pipeline.apply(
            "Persons",
            Create.of(
                KV.of("Henry", "Singapore"),
                KV.of("Jane", "San Francisco"),
                KV.of("Lee", "Beijing"),
                KV.of("John", "Sydney"),
                KV.of("Alfred", "London")));

    PCollection<KV<String, String>> output =
        persons.apply(
            ParDo.of(
                    new DoFn<KV<String, String>, KV<String, String>>() {
                      // Get city from person and get from city view
                      @ProcessElement
                      public void processElement(
                          @Element KV<String, String> person,
                          OutputReceiver<KV<String, String>> out,
                          ProcessContext context) {
                        Map<String, String> citiesToCountries =
                            context.sideInput(citiesToCountriesView);
                        String city = person.getValue();
                        String country = citiesToCountries.get(city);
                        if (country == null) {
                          country = "Unknown";
                        }
                        out.output(KV.of(person.getKey(), country));
                      }
                    })
                .withSideInputs(citiesToCountriesView));
    // [END main_section]

    output.apply("Log", ParDo.of(new LogOutput<>("Output: ")));

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

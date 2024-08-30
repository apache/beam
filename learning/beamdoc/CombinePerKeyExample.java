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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// beam-playground:
//   name: CombinePerKey
//   description: Demonstration of Combine.perKey transform usage.
//   multifile: false
//   default_example: false
//   context_line: 48
//   categories:
//     - Core Transforms
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers
//     - distinct

public class CombinePerKeyExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);

    // [START main_section]
    // PCollection is grouped by key and the Double values associated with each key
    // are combined into a Double.
    PCollection<KV<String, Double>> salesRecords =
        pipeline.apply(
            Create.of(
                KV.of("Apples", 2.0),
                KV.of("Apples", 3.0),
                KV.of("Apples", 5.0),
                KV.of("Oranges", 4.0),
                KV.of("Oranges", 8.0)));
    PCollection<KV<String, Double>> totalSalesPerPerson =
        salesRecords.apply(Combine.perKey(Sum.ofDoubles()));
    // [END main_section]
    // Log values
    totalSalesPerPerson.apply(
        ParDo.of(new LogOutput<>("PCollection numbers after Combine.perKey transform: ")));
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

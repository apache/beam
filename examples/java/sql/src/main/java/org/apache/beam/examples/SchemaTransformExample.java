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

// beam-playground:
//   name: Group.ByFields
//   description: Demonstration of Schema transform usage.
//   multifile: false
//   default_example: false
//   context_line: 60
//   categories:
//     - Schemas
//     - Combiners
//   complexity: BASIC
//   tags:
//     - transforms
//     - numbers

// gradle clean execute -DmainClass=org.apache.beam.examples.SchemaTransformExample
// --args="--runner=DirectRunner" -Pdirect-runner

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example that uses Schema transforms to apply multiple combiners (Sum, Min, Max) on the input
 * PCollection.
 *
 * <p>For a detailed documentation of Schemas, see <a
 * href="https://beam.apache.org/documentation/programming-guide/#schemas">
 * https://beam.apache.org/documentation/programming-guide/#schemas </a>
 */
public class SchemaTransformExample {
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline pipeline = Pipeline.create(options);
    // [START main_section]
    // define the input row schema
    Schema inputSchema = Schema.builder().addInt32Field("k").addInt32Field("n").build();
    // Create input
    PCollection<Row> input =
        pipeline
            .apply(
                Create.of(
                    Row.withSchema(inputSchema).addValues(1, 1).build(),
                    Row.withSchema(inputSchema).addValues(1, 5).build(),
                    Row.withSchema(inputSchema).addValues(2, 10).build(),
                    Row.withSchema(inputSchema).addValues(2, 20).build(),
                    Row.withSchema(inputSchema).addValues(3, 1).build()))
            .setRowSchema(inputSchema);

    PCollection<Row> result =
        input
            .apply(Select.fieldNames("n", "k"))
            .apply(
                Group.<Row>byFieldNames("k")
                    .aggregateField("n", Min.ofIntegers(), "min_n")
                    .aggregateField("n", Max.ofIntegers(), "max_n")
                    .aggregateField("n", Sum.ofIntegers(), "sum_n"));
    // [END main_section]
    // Log values
    result.apply(ParDo.of(new LogOutput<>("PCollection values after Schema transform: ")));
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

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
package org.apache.beam.sdk.extensions.yaml;

import static org.junit.Assume.assumeTrue;

import java.io.Serializable;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class YamlTransformTest implements Serializable {

  // See build.gradle for test task configuration.
  private static final String PORTABLE_RUNNER_PROPERTY_NAME = "testRunner.jobEndpoint";

  private static final Schema INPUT_SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("email", Schema.FieldType.STRING));
  private static final Row ALICE =
      Row.withSchema(INPUT_SCHEMA).addValue("alice").addValue("alice@example.com").build();
  private static final Row BOB =
      Row.withSchema(INPUT_SCHEMA).addValue("bob").addValue("bob@example.com").build();

  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(options());

  @Test
  public void simpleYamlTransform() {
    PCollection<Row> output =
        pipeline
            .apply(Create.of(ALICE, BOB))
            .setRowSchema(INPUT_SCHEMA)
            .apply(
                YamlTransform.of(
                    "{type: Filter, config: {language: python, keep: 'len(name) < 4'}}"));
    PAssert.that(output).containsInAnyOrder(BOB);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void multipleOutputYamlTransform() {
    PCollectionRowTuple output =
        pipeline
            .apply(Create.of(ALICE, BOB))
            .setRowSchema(INPUT_SCHEMA)
            .apply(
                // TODO(https://github.com/apache/beam/pull/34595): Use Partition transform.
                // This needs to wait until the above change is in a released version of Beam.
                // YamlTransform.of(
                //         "{type: Partition, "
                //             + " config: {language: python, by: 'name[0]', outputs: [a, b]}}")
                //     .withMultipleOutputs("a", "b"));
                YamlTransform.of(
                        "type: composite \n"
                            + "input: input \n"
                            + "transforms: [ \n"
                            + "   {type: Filter, name: A, input: input, "
                            + "    config: {language: python, keep: 'name[0] == \"a\"'}}, \n"
                            + "   {type: Filter, name: B, input: input, "
                            + "    config: {language: python, keep: 'name[0] == \"b\"'}} \n"
                            + "] \n"
                            + "output: {a: A, b: B} \n"
                            + "")
                    .withMultipleOutputs("a", "b"));
    PAssert.that(output.get("a")).containsInAnyOrder(ALICE);
    PAssert.that(output.get("b")).containsInAnyOrder(BOB);

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void yamlSource() {
    PCollection<Row> output =
        pipeline.apply(
            YamlTransform.source(
                "{type: Create, "
                    + " config: {elements: [{name: alice, email: alice@example.com}]}}"));
    PAssert.that(output).containsInAnyOrder(ALICE);

    // TODO: Run this on a multi-language supporting runner.
    pipeline.run().waitUntilFinish();
  }

  private static PipelineOptions options() {
    PortablePipelineOptions opts =
        PipelineOptionsFactory.create().as(PortablePipelineOptions.class);

    opts.setRunner(PortableRunner.class);
    // TODO(https://github.com/apache/beam/issues/34594):Use LOOPBACK here.
    opts.setDefaultEnvironmentType("DOCKER");
    opts.setJobEndpoint(getLocalRunnerAddress());

    return opts;
  }

  /**
   * Drives ignoring of tests via checking {@link org.junit.Assume#assumeTrue} that the {@link
   * System#getProperty} for {@link #PORTABLE_RUNNER_PROPERTY_NAME} is not null or empty.
   */
  private static String getLocalRunnerAddress() {
    String address = System.getProperty(PORTABLE_RUNNER_PROPERTY_NAME);
    assumeTrue(
        "System property: "
            + PORTABLE_RUNNER_PROPERTY_NAME
            + " is not set; start a local runner and pass "
            + "-D"
            + PORTABLE_RUNNER_PROPERTY_NAME
            + "=localhost:port",
        !Strings.isNullOrEmpty(address));
    return address;
  }
}

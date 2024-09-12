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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismRunner}. */
@RunWith(JUnit4.class)
public class PrismRunnerTest {

  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(options());

  // See build.gradle for test task configuration.
  private static final String PRISM_BUILD_TARGET_PROPERTY_NAME = "prism.buildTarget";

  @Test
  public void givenJobEndpointSet_TestPrismRunner_validateThrows() {
    TestPrismPipelineOptions options =
        PipelineOptionsFactory.create().as(TestPrismPipelineOptions.class);
    options.setRunner(TestPrismRunner.class);
    options.setJobEndpoint("endpoint");
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> TestPrismRunner.fromOptions(options));
    assertThat(error.getMessage())
        .isEqualTo("when specifying --jobEndpoint, use --runner=PortableRunner instead");
  }

  @Test
  public void givenJobEndpointSet_PrismRunner_validateThrows() {
    PrismPipelineOptions options = PipelineOptionsFactory.create().as(PrismPipelineOptions.class);
    options.setRunner(PrismRunner.class);
    options.setJobEndpoint("endpoint");
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> TestPrismRunner.fromOptions(options));
    assertThat(error.getMessage())
        .isEqualTo("when specifying --jobEndpoint, use --runner=PortableRunner instead");
  }

  @Test
  public void givenEnvironmentTypeEmpty_TestPrismRunner_defaultsToLoopback() {
    TestPrismPipelineOptions options =
        PipelineOptionsFactory.create().as(TestPrismPipelineOptions.class);
    options.setRunner(TestPrismRunner.class);
    assertThat(
            TestPrismRunner.fromOptions(options)
                .getTestPrismPipelineOptions()
                .getDefaultEnvironmentType())
        .isEqualTo(Environments.ENVIRONMENT_LOOPBACK);
  }

  @Test
  public void givenEnvironmentTypeEmpty_PrismRunner_defaultsToLoopback() {
    PrismPipelineOptions options = PipelineOptionsFactory.create().as(PrismPipelineOptions.class);
    options.setRunner(PrismRunner.class);
    assertThat(
            PrismRunner.fromOptions(options).getPrismPipelineOptions().getDefaultEnvironmentType())
        .isEqualTo(Environments.ENVIRONMENT_LOOPBACK);
  }

  @Test
  public void prismReportsPAssertFailure() {
    PAssert.that(pipeline.apply(Create.of(1, 2, 3)))
        // Purposely introduce a failed assertion.
        .containsInAnyOrder(1, 2, 3, 4);
    assertThrows(AssertionError.class, pipeline::run);
  }

  @Test
  public void windowing() {
    PCollection<KV<String, Iterable<Integer>>> got =
        pipeline
            .apply(Create.of(1, 2, 100, 101, 102, 123))
            .apply(WithTimestamps.of(t -> Instant.ofEpochSecond(t)))
            .apply(WithKeys.of("k"))
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))))
            .apply(GroupByKey.create());

    List<KV<String, Iterable<Integer>>> want =
        Arrays.asList(
            KV.of("k", Arrays.asList(1, 2)),
            KV.of("k", Arrays.asList(100, 101, 102)),
            KV.of("k", Collections.singletonList(123)));

    PAssert.that(got).containsInAnyOrder(want);

    pipeline.run();
  }

  private static TestPrismPipelineOptions options() {
    TestPrismPipelineOptions opts =
        PipelineOptionsFactory.create().as(TestPrismPipelineOptions.class);

    opts.setRunner(TestPrismRunner.class);
    opts.setPrismLocation(getLocalPrismBuildOrIgnoreTest());
    opts.setEnableWebUI(false);

    return opts;
  }

  /**
   * Drives ignoring of tests via checking {@link org.junit.Assume#assumeTrue} that the {@link
   * System#getProperty} for {@link #PRISM_BUILD_TARGET_PROPERTY_NAME} is not null or empty.
   */
  static String getLocalPrismBuildOrIgnoreTest() {
    String command = System.getProperty(PRISM_BUILD_TARGET_PROPERTY_NAME);
    assumeTrue(
        "System property: "
            + PRISM_BUILD_TARGET_PROPERTY_NAME
            + " is not set; see build.gradle for test task configuration",
        !Strings.isNullOrEmpty(command));
    return command;
  }
}

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
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PeriodicImpulse;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismRunner}. */

// TODO(https://github.com/apache/beam/issues/31793): Remove @Ignore after finalizing PrismRunner.
//    Depends on: https://github.com/apache/beam/issues/31402 and
//    https://github.com/apache/beam/issues/31792.
@Ignore
@RunWith(JUnit4.class)
public class PrismRunnerTest {
  // See build.gradle for test task configuration.
  private static final String PRISM_BUILD_TARGET_PROPERTY_NAME = "prism.buildTarget";

  @Test
  public void givenBoundedSource_runsUntilDone() {
    Pipeline pipeline = Pipeline.create(options());
    pipeline.apply(Create.of(1, 2, 3));
    PipelineResult.State state = pipeline.run().waitUntilFinish();
    assertThat(state).isEqualTo(PipelineResult.State.DONE);
  }

  @Test
  public void givenUnboundedSource_runsUntilCancel() throws IOException {
    Pipeline pipeline = Pipeline.create(options());
    pipeline.apply(PeriodicImpulse.create());
    PipelineResult result = pipeline.run();
    assertThat(result.getState()).isEqualTo(PipelineResult.State.RUNNING);
    PipelineResult.State state = result.cancel();
    assertThat(state).isEqualTo(PipelineResult.State.CANCELLED);
  }

  private static PrismPipelineOptions options() {
    PrismPipelineOptions opts = PipelineOptionsFactory.create().as(PrismPipelineOptions.class);

    opts.setRunner(PrismRunner.class);
    opts.setPrismLocation(getLocalPrismBuildOrIgnoreTest());

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

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
package org.apache.beam.runners.portability.testing;

import static org.hamcrest.MatcherAssert.assertThat;

import com.google.auto.service.AutoService;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.runners.PipelineRunnerRegistrar;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.hamcrest.Matchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link PipelineRunner} a {@link Pipeline} against a {@code JobService}. */
public class TestUniversalRunner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = LoggerFactory.getLogger(TestUniversalRunner.class);

  private final PipelineOptions options;

  private TestUniversalRunner(PipelineOptions options) {
    this.options = options;
  }

  /**
   * Constructs a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static TestUniversalRunner fromOptions(PipelineOptions options) {
    return new TestUniversalRunner(options);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    Options testOptions = options.as(Options.class);
    if (testOptions.getLocalJobServicePortFile() != null) {
      String localServicePortFilePath = testOptions.getLocalJobServicePortFile();
      try {
        testOptions.setJobEndpoint(
            "localhost:"
                + new String(
                        Files.readAllBytes(Paths.get(localServicePortFilePath)), Charsets.UTF_8)
                    .trim());
      } catch (IOException e) {
        throw new RuntimeException(
            String.format("Error reading local job service port file %s", localServicePortFilePath),
            e);
      }
    }

    PortablePipelineOptions portableOptions = options.as(PortablePipelineOptions.class);
    portableOptions.setRunner(PortableRunner.class);
    PortableRunner runner = PortableRunner.fromOptions(portableOptions);
    PipelineResult result = runner.run(pipeline);
    assertThat(
        "Pipeline did not succeed.",
        result.waitUntilFinish(),
        Matchers.is(PipelineResult.State.DONE));
    return result;
  }

  public interface Options extends TestPipelineOptions, PortablePipelineOptions {
    /**
     * A file containing the job service port, since Gradle needs to know this filename statically
     * to provide it in Beam testing options.
     *
     * <p>This option conflicts with {@link #getJobEndpoint()}. This option will override the job
     * endpoint to be localhost at the port specified in the file.
     */
    @Description("File containing local job service port.")
    String getLocalJobServicePortFile();

    void setLocalJobServicePortFile(String endpoint);
  }

  /** Register {@link Options}. */
  @AutoService(PipelineOptionsRegistrar.class)
  public static class OptionsRegistrar implements PipelineOptionsRegistrar {

    @Override
    public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
      return ImmutableList.of(Options.class);
    }
  }

  /** Registrar for the portable runner. */
  @AutoService(PipelineRunnerRegistrar.class)
  public static class RunnerRegistrar implements PipelineRunnerRegistrar {

    @Override
    public Iterable<Class<? extends PipelineRunner<?>>> getPipelineRunners() {
      return ImmutableList.of(TestUniversalRunner.class);
    }
  }
}

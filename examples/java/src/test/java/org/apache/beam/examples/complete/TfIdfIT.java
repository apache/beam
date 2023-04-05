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
package org.apache.beam.examples.complete;

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import java.util.regex.Pattern;
import org.apache.beam.examples.complete.TfIdf.Options;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for TfIdf example. */
@RunWith(JUnit4.class)
public class TfIdfIT {

  private static final String DEFAULT_INPUT = "gs://apache-beam-samples/shakespeare/";
  private static final String EXPECTED_OUTPUT_CHECKSUM = "0549d1dc8821976121771aefcb0e2297177bdb88";
  private static final Pattern DEFAULT_SHARD_TEMPLATE =
      Pattern.compile("(?x) \\S* (?<shardnum> \\d+) -of- (?<numshards> \\d+)\\.csv");

  /**
   * Options for the TfIdf Integration Test.
   *
   * <p>Define expected output file checksum to verify TfIdf pipeline result with customized input.
   */
  public interface TfIdfITOptions extends TestPipelineOptions, Options {}

  @BeforeClass
  public static void setUp() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testE2ETfIdf() throws Exception {
    TfIdfITOptions options = TestPipeline.testingPipelineOptions().as(TfIdfITOptions.class);
    options.setInput(DEFAULT_INPUT);
    options.setOutput(
        FileSystems.matchNewResource(options.getTempRoot(), true)
            .resolve(
                String.format("TfIdfIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", StandardResolveOptions.RESOLVE_FILE)
            .toString());
    TfIdf.runTfIdf(options);

    assertThat(
        new NumberedShardedFile(options.getOutput() + "*-of-*.csv", DEFAULT_SHARD_TEMPLATE),
        fileContentsHaveChecksum(EXPECTED_OUTPUT_CHECKSUM));
  }
}

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
package org.apache.beam.examples.complete.game;

import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Date;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration Tests for {@link HourlyTeamScore}. */
@RunWith(JUnit4.class)
public class HourlyTeamScoreIT {
  public static final String GAMING_DATA_CSV = "gs://apache-beam-samples/game/gaming_data1.csv";
  public static final String TEMP_STORAGE_DIR = "gs://temp-storage-for-end-to-end-tests";
  private static final String DEFAULT_OUTPUT_CHECKSUM = "f920742fd1b363d01b0a5a44c951c683ea348a47";
  private HourlyTeamScoreOptions options =
      TestPipeline.testingPipelineOptions().as(HourlyTeamScoreOptions.class);
  private static String projectId;

  public interface HourlyTeamScoreOptions extends TestPipelineOptions, HourlyTeamScore.Options {}

  @Before
  public void setupTestEnvironment() throws Exception {

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupPipelineOptions();
  }

  @Test
  public void testE2EHourlyTeamScore() throws Exception {

    HourlyTeamScore.runHourlyTeamScore(options);

    assertThat(
        new NumberedShardedFile(options.getOutput() + "*-of-*"),
        fileContentsHaveChecksum(DEFAULT_OUTPUT_CHECKSUM));
  }

  private void setupPipelineOptions() {
    options.as(GcpOptions.class).setProject(projectId);
    options.setBlockOnRun(false);
    options.setInput(GAMING_DATA_CSV);
    options.setOutput(
        FileSystems.matchNewResource(TEMP_STORAGE_DIR, true)
            .resolve(
                String.format("hourlyteamscoreIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString());
    options.setWindowDuration(10);
    options.setIsWindowed(false);
  }
}

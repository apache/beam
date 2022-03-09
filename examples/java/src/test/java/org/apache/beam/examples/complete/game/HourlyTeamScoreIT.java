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

import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HourlyTeamScore}. */
@RunWith(JUnit4.class)
public class HourlyTeamScoreIT {
  public static final String GAMING_DATA_CSV =
      "gs://apache-beam-samples/game/small/gaming_data.csv";
  public static final String TEMP_STORAGE_FOR_UPLOAD_TESTS =
      "gs://temp-storage-for-end-to-end-tests/HourlyTeamScoreIT/game/"
          + HourlyTeamScoreIT.class.getSimpleName();
  private HourlyTeamScoreOptions options =
      TestPipeline.testingPipelineOptions().as(HourlyTeamScoreOptions.class);
  private static String projectId;
  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  public interface HourlyTeamScoreOptions extends TestPipelineOptions, HourlyTeamScore.Options {}

  @Before
  public void setupTestEnvironment() throws Exception {

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupPipelineOptions();
  }

  @Test
  public void testE2EHourlyTeamScore() throws Exception {

    HourlyTeamScore.runHourlyTeamScore(options, testPipeline);
    testPipeline.run().waitUntilFinish();
  }

  @After
  public void cleanupTestEnvironment() throws Exception {}

  private void setupPipelineOptions() {
    options.as(GcpOptions.class).setProject(projectId);
    options.as(DirectOptions.class).setBlockOnRun(false);
    options.setInput(GAMING_DATA_CSV);
    options.setOutput(TEMP_STORAGE_FOR_UPLOAD_TESTS);
    options.setWindowDuration(10);
  }
}

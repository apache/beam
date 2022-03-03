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

import static org.junit.Assert.assertEquals;

import com.google.api.services.bigquery.model.*;
import com.google.api.services.pubsub.Pubsub;
import java.io.IOException;
import org.apache.beam.examples.complete.game.injector.InjectorUtils;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link LeaderBoard}. */
@RunWith(JUnit4.class)
public class LeaderBoardIT {
  public static final String LEADERBOARD_TEAM = "leaderboard_team";
  public static final String SELECT_COUNT_AS_TOTAL_QUERY =
      "SELECT count(*) as total FROM `%s.%s.%s`";
  private LeaderBoardOptions options =
      TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
  private static Pubsub pubsub;
  private static String projectId;
  private static final String TOPIC = "projects/apache-beam-testing/topics/leaderboardscores";
  private static BigqueryClient bqClient;
  private static final String OUTPUT_DATASET = "leader_board_e2e_events";
  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  public interface LeaderBoardOptions extends TestPipelineOptions, LeaderBoard.Options {}

  @Before
  public void setupTestEnvironment() throws Exception {

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupBigQuery();

    setupPubSub();

    setupPipelineOptions();
  }

  @Test
  public void testE2ELeaderBoard() throws Exception {

    LeaderBoard.runLeaderBoard(options, testPipeline);

    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(5)
            .withInitialBackoff(Duration.standardMinutes(5))
            .withMaxBackoff(Duration.standardMinutes(5));

    QueryResponse response =
        bqClient.queryWithRetries(
            String.format(SELECT_COUNT_AS_TOTAL_QUERY, projectId, OUTPUT_DATASET, LEADERBOARD_TEAM),
            projectId,
            backoffFactory);

    String res = response.getRows().get(0).getF().get(0).getV().toString();

    assertEquals("14", res);
  }

  @After
  public void cleanupTestEnvironment() throws Exception {
    bqClient.deleteDataset(projectId, OUTPUT_DATASET);
    pubsub.projects().topics().delete(TOPIC);
  }

  private void setupBigQuery() throws IOException, InterruptedException {
    bqClient = new BigqueryClient("LeaderBoardIT");
    bqClient.createNewDataset(projectId, OUTPUT_DATASET);
  }

  private void setupPubSub() throws IOException {
    pubsub = InjectorUtils.getClient();
    InjectorUtils.createTopic(pubsub, TOPIC);

    PubsubIO.Write<String> write =
        PubsubIO.writeStrings()
            .to(TOPIC)
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .withIdAttribute(projectId);

    testPipeline.apply(TextIO.read().from(options.getInput())).apply(write);
  }

  private void setupPipelineOptions() {
    options = TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
    options.as(GcpOptions.class).setProject(projectId);
    options.setDataset(OUTPUT_DATASET);
    options.setTopic(TOPIC);
    options.setStreaming(true);
    options.as(DirectOptions.class).setBlockOnRun(false);
    options.setTeamWindowDuration(1);
    options.setAllowedLateness(1);
  }
}

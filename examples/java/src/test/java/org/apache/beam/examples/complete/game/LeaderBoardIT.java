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

import com.google.api.services.bigquery.model.QueryResponse;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
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

/** Integration Tests for {@link LeaderBoard}. */
@RunWith(JUnit4.class)
public class LeaderBoardIT extends CompleteGame {
  public static final String LEADERBOARD_TEAM_TABLE = "leaderboard_team";
  public static final String SELECT_COUNT_AS_TOTAL_QUERY =
      "SELECT total_score FROM `%s.%s.%s` WHERE team LIKE (\"AzureCassowary\")";
  private final LeaderBoardOptions options =
      TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  public interface LeaderBoardOptions extends TestPipelineOptions, LeaderBoard.Options {}

  @Before
  public void setupTestEnvironment() throws Exception {
    topicPrefix = "leaderboardscores-";
    outputDataset = "leader_board_e2e_events_" + timestamp;

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupBigQuery();

    setupPubSub(testPipeline, options);

    setupPipelineOptions();
  }

  @Test
  public void testE2ELeaderBoard() throws Exception {

    LeaderBoard.runLeaderBoard(options);

    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(5)
            .withInitialBackoff(Duration.standardMinutes(5))
            .withMaxBackoff(Duration.standardMinutes(5));

    QueryResponse response =
        bqClient.queryWithRetriesUsingStandardSql(
            String.format(
                SELECT_COUNT_AS_TOTAL_QUERY, projectId, outputDataset, LEADERBOARD_TEAM_TABLE),
            projectId,
            backoffFactory);

    int res = response.getRows().size();

    assertEquals("1", Integer.toString(res));
  }

  private void setupPipelineOptions() {
    options.setProject(projectId);
    options.setDataset(outputDataset);
    options.setSubscription(subscriptionPath.getPath());
    options.setTopic(eventsTopicPath.getPath());
    options.setStreaming(false);
    options.setBlockOnRun(false);
    options.setTeamWindowDuration(1);
    options.setAllowedLateness(1);
  }

  @Override
  @After
  public void cleanupTestEnvironment() throws Exception {
    super.cleanupTestEnvironment();
  }
}

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
public class StatefulTeamScoreIT extends CompleteGame {
  public static final String LEADERBOARD_TEAM_LEADER_TABLE = "leaderboard_team_leader";
  public static final String SELECT_TOTAL_SCORE_QUERY =
      "SELECT total_score FROM `%s.%s.%s` where team like(\"AmaranthKoala\")";
  private StatefulTeamScoreOptions options =
      TestPipeline.testingPipelineOptions().as(StatefulTeamScoreIT.StatefulTeamScoreOptions.class);
  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  public interface StatefulTeamScoreOptions
      extends TestPipelineOptions, StatefulTeamScore.Options {};

  @Before
  public void setupTestEnvironment() throws Exception {
    topicPrefix = "statefulteamscores-";
    outputDataset = "stateful_team_score_e2e_" + timestamp;

    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupBigQuery();

    setupPubSub(testPipeline, options);

    setupPipelineOptions();
  }

  @Test
  public void testE2EStatefulTeamScore() throws Exception {
    StatefulTeamScore.runStatefulTeamScore(options);

    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(5)
            .withInitialBackoff(Duration.standardMinutes(10))
            .withMaxBackoff(Duration.standardMinutes(10));

    QueryResponse response =
        bqClient.queryWithRetriesUsingStandardSql(
            String.format(
                SELECT_TOTAL_SCORE_QUERY, projectId, outputDataset, LEADERBOARD_TEAM_LEADER_TABLE),
            projectId,
            backoffFactory);

    int res = response.getRows().size();

    assertEquals("25", Integer.toString(res));
  }

  private void setupPipelineOptions() {
    options.as(GcpOptions.class).setProject(projectId);
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

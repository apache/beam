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

/** Integration Tests for {@link GameStats}. */
@RunWith(JUnit4.class)
public class GameStatsIT extends CompleteGame {
  public static final String GAME_STATS_TEAM_TABLE = "game_stats_team";
  public static final String SELECT_COUNT_AS_TOTAL_QUERY =
      "SELECT total_score FROM `%s.%s.%s` where team like(\"AmaranthKoala\")";
  private GameStatsOptions options =
      TestPipeline.testingPipelineOptions().as(GameStatsIT.GameStatsOptions.class);
  @Rule public final transient TestPipeline testPipeline = TestPipeline.fromOptions(options);

  public interface GameStatsOptions extends TestPipelineOptions, GameStats.Options {};

  @Before
  public void setupTestEnvironment() throws Exception {
    TOPIC_PREFIX = "gamestats-";
    OUTPUT_DATASET = "game_stats_e2e_" + timestamp;
    PipelineOptionsFactory.register(TestPipelineOptions.class);
    projectId = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();

    setupBigQuery();

    setupPubSub(testPipeline, options);

    setupPipelineOptions();
  }

  @Test
  public void testE2EGameStats() throws Exception {

    GameStats.runGameStats(options);

    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(5)
            .withInitialBackoff(Duration.standardMinutes(10))
            .withMaxBackoff(Duration.standardMinutes(10));

    QueryResponse response =
        bqClient.queryWithRetriesUsingStandardSql(
            String.format(
                SELECT_COUNT_AS_TOTAL_QUERY, projectId, OUTPUT_DATASET, GAME_STATS_TEAM_TABLE),
            projectId,
            backoffFactory);

    int res = response.getRows().size();

    assertEquals("1", Integer.toString(res));
  }

  private void setupPipelineOptions() {
    options.as(GcpOptions.class).setProject(projectId);
    options.setDataset(OUTPUT_DATASET);
    options.setSubscription(subscriptionPath.getPath());
    options.setTopic(eventsTopicPath.getPath());
    options.setStreaming(true);
    options.setBlockOnRun(false);
    options.setTeamWindowDuration(1);
    options.setAllowedLateness(1);
    options.setFixedWindowDuration(1);
    options.setUserActivityWindowDuration(1);
    options.setSessionGap(1);
  }

  @Override
  @After
  public void cleanupTestEnvironment() throws Exception {
    super.cleanupTestEnvironment();
  }
}

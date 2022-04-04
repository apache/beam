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
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.PushConfig;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.examples.complete.game.utils.GameConstants;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.FluentBackoff;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration Tests for {@link LeaderBoard}. */
@RunWith(JUnit4.class)
public class LeaderBoardIT {
  private static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss-SSS");
  private static final String timestamp = Long.toString(System.currentTimeMillis());
  private static final String EVENTS_TOPIC_NAME = "events";
  public static final String LEADERBOARD_TEAM_TABLE = "leaderboard_team";
  public static final String CLOUD_STORAGE_CSV_FILE =
      "gs://apache-beam-samples/game/gaming_data1.csv";
  private static final Integer DEFAULT_ACK_DEADLINE_SECONDS = 120;
  public static final String SELECT_COUNT_AS_TOTAL_QUERY =
      "SELECT total_score FROM `%s.%s.%s` WHERE team LIKE (\"AzureCassowary\")";
  private LeaderBoardOptions options =
      TestPipeline.testingPipelineOptions().as(LeaderBoardOptions.class);
  private @Nullable TopicAdminClient topicAdmin = null;
  private @Nullable SubscriptionAdminClient subscriptionAdmin = null;
  private @Nullable TopicPath eventsTopicPath = null;
  private @Nullable SubscriptionPath subscriptionPath = null;
  private static String projectId;
  private static final String TOPIC_PREFIX = "leaderboardscores-";
  private static BigqueryClient bqClient;
  private static final String OUTPUT_DATASET = "leader_board_e2e_events_" + timestamp;
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

    LeaderBoard.runLeaderBoard(options);

    FluentBackoff backoffFactory =
        FluentBackoff.DEFAULT
            .withMaxRetries(5)
            .withInitialBackoff(Duration.standardMinutes(5))
            .withMaxBackoff(Duration.standardMinutes(5));

    QueryResponse response =
        bqClient.queryWithRetries(
            String.format(
                SELECT_COUNT_AS_TOTAL_QUERY, projectId, OUTPUT_DATASET, LEADERBOARD_TEAM_TABLE),
            projectId,
            backoffFactory);

    int res = response.getRows().size();

    assertEquals("1", Integer.toString(res));
  }

  @After
  public void cleanupTestEnvironment() throws Exception {
    bqClient.deleteDataset(projectId, OUTPUT_DATASET);

    if (subscriptionAdmin == null || topicAdmin == null) {
      return;
    }

    try {
      if (subscriptionPath != null) {
        subscriptionAdmin.deleteSubscription(subscriptionPath.getPath());
      }
      if (eventsTopicPath != null) {
        for (String subscriptionPath :
            topicAdmin.listTopicSubscriptions(eventsTopicPath.getPath()).iterateAll()) {
          subscriptionAdmin.deleteSubscription(subscriptionPath);
        }
        topicAdmin.deleteTopic(eventsTopicPath.getPath());
      }
    } finally {
      subscriptionAdmin.close();
      topicAdmin.close();

      subscriptionAdmin = null;
      topicAdmin = null;

      eventsTopicPath = null;
      subscriptionPath = null;
    }
  }

  private void setupBigQuery() throws IOException, InterruptedException {
    bqClient = new BigqueryClient("LeaderBoardIT");
    bqClient.createNewDataset(projectId, OUTPUT_DATASET);
  }

  private void setupPubSub() throws IOException {
    String pubsubEndpoint = PubsubOptions.targetForRootUrl("https://pubsub.googleapis.com");

    topicAdmin =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(options::getGcpCredential)
                .setEndpoint(pubsubEndpoint)
                .build());

    subscriptionAdmin =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(options::getGcpCredential)
                .setEndpoint(pubsubEndpoint)
                .build());

    PubsubClient.TopicPath eventsTopicPathTmp =
        PubsubClient.topicPathFromName(options.getProject(), createTopicName(EVENTS_TOPIC_NAME));

    topicAdmin.createTopic(eventsTopicPathTmp.getPath());

    // Set this after successful creation; it signals that the topic needs teardown
    eventsTopicPath = eventsTopicPathTmp;

    String subscriptionName =
        eventsTopicPath.getName() + "_beam_" + ThreadLocalRandom.current().nextLong();

    // Generates randomized subscription name.
    // Example:
    // 'leaderboardscores-2018-12-11-23-32-333-events-6185541326079233738_beam_3331121845501767394'
    PubsubClient.SubscriptionPath subscriptionPathTmp =
        new PubsubClient.SubscriptionPath(
            String.format("projects/%s/subscriptions/%s", options.getProject(), subscriptionName));

    subscriptionAdmin.createSubscription(
        subscriptionPathTmp.getPath(),
        eventsTopicPath.getPath(),
        PushConfig.getDefaultInstance(),
        DEFAULT_ACK_DEADLINE_SECONDS);

    subscriptionPath = subscriptionPathTmp;

    PubsubIO.Write<String> write =
        PubsubIO.writeStrings()
            .to(eventsTopicPath.getPath())
            .withTimestampAttribute(GameConstants.TIMESTAMP_ATTRIBUTE)
            .withIdAttribute(projectId);

    testPipeline.apply(TextIO.read().from(CLOUD_STORAGE_CSV_FILE)).apply(write);
    testPipeline.run();
  }

  private void setupPipelineOptions() {
    options.setProject(projectId);
    options.setDataset(OUTPUT_DATASET);
    options.setSubscription(subscriptionPath.getPath());
    options.setStreaming(false);
    options.setBlockOnRun(false);
    options.setTeamWindowDuration(1);
    options.setAllowedLateness(1);
  }

  /**
   * Generates randomized topic name.
   *
   * <p>Example: 'leaderboardscores-2018-12-11-23-32-333-events-6185541326079233738'
   */
  private static String createTopicName(String name) {
    StringBuilder topicName = new StringBuilder(TOPIC_PREFIX);

    DATETIME_FORMAT.printTo(topicName, Instant.now());

    return topicName + "-" + name + "-" + ThreadLocalRandom.current().nextLong();
  }
}

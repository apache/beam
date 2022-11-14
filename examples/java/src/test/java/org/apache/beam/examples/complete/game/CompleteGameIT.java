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
import org.apache.beam.sdk.testing.TestPipeline;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public abstract class CompleteGameIT {
  protected static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss-SSS");
  protected static String timestamp = Long.toString(System.currentTimeMillis());
  protected static String EVENTS_TOPIC_NAME = "events";
  protected static String CLOUD_STORAGE_CSV_FILE =
      "gs://apache-beam-samples/game/small/gaming_data.csv";
  protected static Integer DEFAULT_ACK_DEADLINE_SECONDS = 120;
  protected @Nullable TopicAdminClient topicAdmin = null;
  protected @Nullable SubscriptionAdminClient subscriptionAdmin = null;
  protected @Nullable TopicPath eventsTopicPath = null;
  protected @Nullable SubscriptionPath subscriptionPath = null;
  protected static String projectId;
  protected static String TOPIC_PREFIX;
  protected static BigqueryClient bqClient;
  protected static String OUTPUT_DATASET;

  protected void setupBigQuery() throws IOException, InterruptedException {
    bqClient = new BigqueryClient(projectId);
    bqClient.createNewDataset(projectId, OUTPUT_DATASET);
  }

  protected void setupPubSub(TestPipeline testPipeline, GcpOptions options)
      throws IOException {
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

    TopicPath eventsTopicPathTmp =
        PubsubClient.topicPathFromName(projectId, createTopicName(EVENTS_TOPIC_NAME));

    topicAdmin.createTopic(eventsTopicPathTmp.getPath());

    // Set this after successful creation; it signals that the topic needs teardown
    eventsTopicPath = eventsTopicPathTmp;

    String subscriptionName =
        eventsTopicPath.getName() + "_beam_" + ThreadLocalRandom.current().nextLong();

    // Generates randomized subscription name.
    // Example:
    // 'leaderboardscores-2018-12-11-23-32-333-events-6185541326079233738_beam_3331121845501767394'
    SubscriptionPath subscriptionPathTmp =
        new SubscriptionPath(
            String.format("projects/%s/subscriptions/%s", projectId, subscriptionName));

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

  protected void cleanupTestEnvironment() throws Exception {

    if (bqClient != null) {
      bqClient.deleteDataset(projectId, OUTPUT_DATASET);
    }

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

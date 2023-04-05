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
package org.apache.beam.sdk.nexmark;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubJsonClient;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.joda.time.Duration;

/** Helper for working with pubsub and gcs. */
public class PubsubHelper {
  /** Underlying pub/sub client. */
  private final PubsubClient pubsubClient;

  /** Project id. */
  private final String project;

  /** Topics we should delete on close. */
  private final List<TopicPath> createdTopics;

  /** Subscriptions we should delete on close. */
  private final List<SubscriptionPath> createdSubscriptions;

  /** How to sleep in retry loops. */
  private final Sleeper sleeper;

  /** How to backoff in retry loops. */
  private final BackOff backOff;

  private PubsubHelper(PubsubClient pubsubClient, String project) {
    this.pubsubClient = pubsubClient;
    this.project = project;
    createdTopics = new ArrayList<>();
    createdSubscriptions = new ArrayList<>();
    sleeper = Sleeper.DEFAULT;
    backOff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.standardSeconds(1))
            .withMaxRetries(3)
            .backoff();
  }

  /** Create a helper. */
  public static PubsubHelper create(PubsubOptions options) throws IOException {
    return new PubsubHelper(
        PubsubJsonClient.FACTORY.newClient(null, null, options), options.getProject());
  }

  /**
   * Create a topic from short name. Delete it if it already exists. Ensure the topic will be
   * deleted on cleanup. Return full topic name.
   */
  public TopicPath createTopic(String shortTopic) throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    while (true) {
      try {
        NexmarkUtils.console("create topic %s", topic);
        pubsubClient.createTopic(topic);
        createdTopics.add(topic);
        return topic;
      } catch (GoogleJsonResponseException ex) {
        NexmarkUtils.console("attempting to cleanup topic %s", topic);
        pubsubClient.deleteTopic(topic);
        try {
          if (!BackOffUtils.next(sleeper, backOff)) {
            NexmarkUtils.console("too many retries for creating topic %s", topic);
            throw ex;
          }
        } catch (InterruptedException in) {
          throw new IOException(in);
        }
      }
    }
  }

  /**
   * Create a topic from short name if it does not already exist. The topic will not be deleted on
   * cleanup. Return full topic name.
   */
  public TopicPath createOrReuseTopic(String shortTopic) throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    while (true) {
      try {
        NexmarkUtils.console("create topic %s", topic);
        pubsubClient.createTopic(topic);
        return topic;
      } catch (GoogleJsonResponseException ex) {
        if (topicExists(shortTopic)) {
          NexmarkUtils.console("topic %s already exists", topic);
          return topic;
        }
        try {
          if (!BackOffUtils.next(sleeper, backOff)) {
            NexmarkUtils.console("too many retries for creating/reusing topic %s", topic);
            throw ex;
          }
        } catch (InterruptedException in) {
          throw new IOException(in);
        }
      }
    }
  }

  /**
   * Check a topic corresponding to short name exists, and throw exception if not. The topic will
   * not be deleted on cleanup. Return full topic name.
   */
  public TopicPath reuseTopic(String shortTopic) throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    if (topicExists(shortTopic)) {
      NexmarkUtils.console("reusing existing topic %s", topic);
      return topic;
    }
    throw new RuntimeException("topic '" + topic + "' does not already exist");
  }

  /** Does topic corresponding to short name exist? */
  public boolean topicExists(String shortTopic) throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    return pubsubClient.listTopics(PubsubClient.projectPathFromId(project)).stream()
        .anyMatch(topic::equals);
  }

  /**
   * Create subscription from short name. Ensure the subscription will be deleted on cleanup. Return
   * full subscription name.
   */
  public SubscriptionPath createSubscription(String shortTopic, String shortSubscription)
      throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    SubscriptionPath subscription =
        PubsubClient.subscriptionPathFromName(project, shortSubscription);
    while (true) {
      try {
        NexmarkUtils.console("create subscription %s", subscription);
        pubsubClient.createSubscription(topic, subscription, 60);
        createdSubscriptions.add(subscription);
        return subscription;
      } catch (GoogleJsonResponseException ex) {
        NexmarkUtils.console("attempting to cleanup subscription %s", subscription);
        pubsubClient.deleteSubscription(subscription);
        try {
          if (!BackOffUtils.next(sleeper, backOff)) {
            NexmarkUtils.console("too many retries for creating subscription %s", subscription);
            throw ex;
          }
        } catch (InterruptedException in) {
          throw new IOException(in);
        }
      }
    }
  }

  /**
   * Check a subscription corresponding to short name exists, and throw exception if not. The
   * subscription will not be deleted on cleanup. Return full topic name.
   */
  public SubscriptionPath reuseSubscription(String shortTopic, String shortSubscription)
      throws IOException {
    SubscriptionPath subscription =
        PubsubClient.subscriptionPathFromName(project, shortSubscription);
    if (subscriptionExists(shortTopic, shortSubscription)) {
      NexmarkUtils.console("reusing existing subscription %s", subscription);
      return subscription;
    }
    throw new RuntimeException("subscription'" + subscription + "' does not already exist");
  }

  /** Does subscription corresponding to short name exist? */
  public boolean subscriptionExists(String shortTopic, String shortSubscription)
      throws IOException {
    TopicPath topic = PubsubClient.topicPathFromName(project, shortTopic);
    SubscriptionPath subscription =
        PubsubClient.subscriptionPathFromName(project, shortSubscription);
    return pubsubClient.listSubscriptions(PubsubClient.projectPathFromId(project), topic).stream()
        .anyMatch(subscription::equals);
  }

  /** Delete all the subscriptions and topics we created. */
  public void cleanup() {
    for (SubscriptionPath subscription : createdSubscriptions) {
      try {
        NexmarkUtils.console("delete subscription %s", subscription);
        pubsubClient.deleteSubscription(subscription);
      } catch (IOException ex) {
        NexmarkUtils.console("could not delete subscription %s", subscription);
      }
    }
    for (TopicPath topic : createdTopics) {
      try {
        NexmarkUtils.console("delete topic %s", topic);
        pubsubClient.deleteTopic(topic);
      } catch (IOException ex) {
        NexmarkUtils.console("could not delete topic %s", topic);
      }
    }
  }
}

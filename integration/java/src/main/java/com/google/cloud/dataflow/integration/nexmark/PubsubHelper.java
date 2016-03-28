/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.integration.nexmark;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.Transport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Helper for working with pubsub and gcs.
 */
public class PubsubHelper {
  /** Underlying pub/sub client. */
  private final Pubsub pubsubClient;

  /** Project id. */
  private final String project;

  /** Topics we should delete on close. */
  private final List<String> createdTopics;

  /** Subscriptions we should delete on close. */
  private final List<String> createdSubscriptions;

  /** How to sleep in retry loops. */
  private final Sleeper sleeper;

  /** How to backoff in retry loops. */
  private final BackOff backOff;

  private PubsubHelper(Pubsub pubsubClient, String project) {
    this.pubsubClient = pubsubClient;
    this.project = project;
    createdTopics = new ArrayList<>();
    createdSubscriptions = new ArrayList<>();
    sleeper = Sleeper.DEFAULT;
    backOff = new AttemptBoundedExponentialBackOff(3, 500);
  }

  /** Create a helper. */
  public static PubsubHelper create(NexmarkDriver.Options options) throws IOException {
    return new PubsubHelper(Transport.newPubsubClient(options).build(),
                            options.getProject());
  }

  /** Return full topic name corresponding to short topic name. */
  private String fullTopic(String shortTopic) {
    return String.format("projects/%s/topics/%s", project, shortTopic);
  }

  /** Return full subscription name corresponding to short subscription name. */
  private String fullSubscription(String shortSubscription) {
    return String.format("projects/%s/subscriptions/%s", project, shortSubscription);
  }

  /**
   * Create a topic from short name. Delete it if it already exists. Ensure the topic will be
   * deleted on cleanup. Return full topic name.
   * @throws InterruptedException
   */
  public String createTopic(String shortTopic) throws IOException, InterruptedException {
    String topic = fullTopic(shortTopic);
    while (true) {
      try {
        NexmarkUtils.console(null, "create topic %s", topic);
        pubsubClient.projects().topics().create(topic, new Topic()).execute();
        createdTopics.add(topic);
        return topic;
      } catch (GoogleJsonResponseException ex) {
        NexmarkUtils.console(null, "attempting to cleanup topic %s", topic);
        pubsubClient.projects().topics().delete(topic).execute();
        if (!BackOffUtils.next(sleeper, backOff)) {
          NexmarkUtils.console(null, "too many retries for creating topic %s", topic);
          throw ex;
        }
      }
    }
  }

  /** Create a topic from short name if it does not already exist. The topic will not be
   * deleted on cleanup. Return full topic name.
   * @throws InterruptedException */
  public String createOrReuseTopic(String shortTopic) throws IOException, InterruptedException {
    String topic = fullTopic(shortTopic);
    while (true) {
      try {
        NexmarkUtils.console(null, "create topic %s", topic);
        pubsubClient.projects().topics().create(topic, new Topic()).execute();
        return topic;
      } catch (GoogleJsonResponseException ex) {
        if (topicExists(shortTopic)) {
          NexmarkUtils.console(null, "topic %s already exists", topic);
          return topic;
        }
        if (!BackOffUtils.next(sleeper, backOff)) {
          NexmarkUtils.console(null, "too many retries for creating/reusing topic %s", topic);
          throw ex;
        }
      }
    }
  }

  /**
   * Check a topic corresponding to short name exists, and throw exception if not. The
   * topic will not be deleted on cleanup. Return full topic name.
   */
  public String reuseTopic(String shortTopic) throws IOException {
    String topic = fullTopic(shortTopic);
    if (topicExists(shortTopic)) {
      NexmarkUtils.console(null, "reusing existing topic %s", topic);
      return topic;
    }
    throw new RuntimeException("topic '" + topic + "' does not already exist");
  }

  /** Does topic corresponding to short name exist? */
  public boolean topicExists(String shortTopic) throws IOException {
    String topic = fullTopic(shortTopic);
    List<Topic> existingTopics =
        pubsubClient.projects().topics().list("projects/" + project).execute().getTopics();
    if (existingTopics != null) {
      for (Topic existingTopic : existingTopics) {
        if (existingTopic.getName().equals(topic)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Create subscription from short name. Ensure the subscription will be deleted
   * on cleanup. Return full subscription name.
   * @throws InterruptedException
   */
  public String createSubscription(String shortTopic, String shortSubscription)
      throws IOException, InterruptedException {
    String topic = fullTopic(shortTopic);
    String subscription = fullSubscription(shortSubscription);
    while (true) {
      try {
        NexmarkUtils.console(null, "create subscription %s", subscription);
        pubsubClient.projects()
            .subscriptions()
            .create(subscription, new Subscription().setTopic(topic).setAckDeadlineSeconds(60))
            .execute();
        createdSubscriptions.add(subscription);
        return subscription;
      } catch (GoogleJsonResponseException ex) {
        NexmarkUtils.console(null, "attempting to cleanup subscription %s", subscription);
        pubsubClient.projects().subscriptions().delete(subscription).execute();
        if (!BackOffUtils.next(sleeper, backOff)) {
          NexmarkUtils.console(null, "too many retries for creating subscription %s", subscription);
          throw ex;
        }
      }
    }
  }

  /**
   * Check a subscription corresponding to short name exists, and throw exception if not. The
   * subscription will not be deleted on cleanup. Return full topic name.
   */
  public String reuseSubscription(String shortTopic, String shortSubscription) throws IOException {
    String subscription = fullSubscription(shortSubscription);
    if (subscriptionExists(shortTopic, shortSubscription)) {
      NexmarkUtils.console(null, "reusing existing subscription %s", subscription);
      return subscription;
    }
    throw new RuntimeException("subscription'" + subscription + "' does not already exist");
  }

  /** Does subscription corresponding to short name exist? */
  public boolean subscriptionExists(String shortTopic, String shortSubscription)
      throws IOException {
    String topic = fullTopic(shortTopic);
    String subscription = fullSubscription(shortSubscription);
    List<Subscription> existingSubscriptions =
        pubsubClient.projects()
            .subscriptions()
            .list("projects/" + project)
            .execute()
            .getSubscriptions();
    if (existingSubscriptions != null) {
      for (Subscription existingSubscription : existingSubscriptions) {
        if (existingSubscription.getTopic().equals(topic)
            && existingSubscription.getName().equals(subscription)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Delete all the subscriptions and topics we created. */
  public void cleanup() {
    for (String subscription : createdSubscriptions) {
      try {
        NexmarkUtils.console(null, "delete subscription %s", subscription);
        pubsubClient.projects().subscriptions().delete(subscription).execute();
      } catch (IOException ex) {
        NexmarkUtils.console(null, "could not delete subscription %s", subscription);
      }
    }
    for (String topic : createdTopics) {
      try {
        NexmarkUtils.console(null, "delete topic %s", topic);
        pubsubClient.projects().topics().delete(topic).execute();
      } catch (IOException ex) {
        NexmarkUtils.console(null, "could not delete topic %s", topic);
      }
    }
  }
}

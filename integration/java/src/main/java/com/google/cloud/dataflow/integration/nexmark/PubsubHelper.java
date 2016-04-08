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

package com.google.cloud.dataflow.integration.nexmark;

import com.google.cloud.dataflow.sdk.io.PubsubClient;
import com.google.cloud.dataflow.sdk.io.PubsubGrpcClient;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Helper for working with pubsub.
 */
public class PubsubHelper implements AutoCloseable {
  /**
   * Underlying pub/sub client.
   */
  private final PubsubClient pubsubClient;

  /**
   * Project id.
   */
  private final String project;

  /**
   * Topics we should delete on close.
   */
  private final List<String> createdTopics;

  /**
   * Subscriptions we should delete on close.
   */
  private final List<String> createdSubscriptions;

  private PubsubHelper(PubsubClient pubsubClient, String project) {
    this.pubsubClient = pubsubClient;
    this.project = project;
    createdTopics = new ArrayList<>();
    createdSubscriptions = new ArrayList<>();
  }

  /**
   * Create a helper.
   */
  public static PubsubHelper create(GcpOptions options) {
    try {
      return new PubsubHelper(PubsubGrpcClient.newClient(null, null, options),
          options.getProject());
    } catch (IOException e) {
      throw new RuntimeException("Unable to create Pubsub client: ", e);
    }
  }

  /**
   * Return full topic name corresponding to short topic name.
   */
  private String fullTopic(String shortTopic) {
    return String.format("projects/%s/topics/%s", project, shortTopic);
  }

  /**
   * Return full subscription name corresponding to short subscription name.
   */
  private String fullSubscription(String shortSubscription) {
    return String.format("projects/%s/subscriptions/%s", project, shortSubscription);
  }

  /**
   * Create a topic from short name. Delete it if it already exists. Ensure the topic will be
   * deleted on cleanup. Return full topic name.
   */
  public String createTopic(String shortTopic) {
    String topic = fullTopic(shortTopic);
    try {
      if (topicExists(topic)) {
        NexmarkUtils.console("attempting to cleanup topic %s", topic);
        pubsubClient.deleteTopic(topic);
      }
      NexmarkUtils.console("create topic %s", topic);
      pubsubClient.createTopic(topic);
      createdTopics.add(topic);
      return topic;
    } catch (IOException e) {
      throw new RuntimeException("Unable to create Pubsub topic " + topic + ": ", e);
    }
  }

  /**
   * Create a topic from short name if it does not already exist. The topic will not be
   * deleted on cleanup. Return full topic name.
   */
  public String createOrReuseTopic(String shortTopic) {
    String topic = fullTopic(shortTopic);
    try {
      if (topicExists(topic)) {
        NexmarkUtils.console("topic %s already exists", topic);
        return topic;
      }
      NexmarkUtils.console("create topic %s", topic);
      pubsubClient.createTopic(topic);
      return topic;
    } catch (IOException e) {
      throw new RuntimeException("Unable to create or reuse Pubsub topic " + topic + ": ", e);
    }
  }

  /**
   * Check a topic corresponding to short name exists, and throw exception if not. The
   * topic will not be deleted on cleanup. Return full topic name.
   */
  public String reuseTopic(String shortTopic) {
    String topic = fullTopic(shortTopic);
    if (topicExists(shortTopic)) {
      NexmarkUtils.console("reusing existing topic %s", topic);
      return topic;
    }
    throw new RuntimeException("topic '" + topic + "' does not already exist");
  }

  /**
   * Does topic corresponding to short name exist?
   */
  public boolean topicExists(String shortTopic) {
    String topic = fullTopic(shortTopic);
    try {
      Collection<String> existingTopics = pubsubClient.listTopics("projects/" + project);
      return existingTopics.contains(topic);
    } catch (IOException e) {
      throw new RuntimeException("Unable to check Pubsub topic " + topic + ": ", e);
    }
  }

  /**
   * Create subscription from short name. Delete subscription if it already exists. Ensure the
   * subscription will be deleted on cleanup. Return full subscription name.
   */
  public String createSubscription(String shortTopic, String shortSubscription) {
    String topic = fullTopic(shortTopic);
    String subscription = fullSubscription(shortSubscription);
    try {
      if (subscriptionExists(shortTopic, shortSubscription)) {
        NexmarkUtils.console("attempting to cleanup subscription %s", subscription);
        pubsubClient.deleteSubscription(subscription);
      }
      NexmarkUtils.console("create subscription %s", subscription);
      pubsubClient.createSubscription(subscription, topic, 60);
      createdSubscriptions.add(subscription);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create Pubsub subscription " + subscription + ": ", e);
    }
    return subscription;
  }

  /**
   * Check a subscription corresponding to short name exists, and throw exception if not. The
   * subscription will not be deleted on cleanup. Return full topic name.
   */
  public String reuseSubscription(String shortTopic, String shortSubscription) {
    String subscription = fullSubscription(shortSubscription);
    if (subscriptionExists(shortTopic, shortSubscription)) {
      NexmarkUtils.console("reusing existing subscription %s", subscription);
      return subscription;
    }
    throw new RuntimeException("subscription'" + subscription + "' does not already exist");
  }

  /**
   * Does subscription corresponding to short name exist?
   */
  public boolean subscriptionExists(String shortTopic, String shortSubscription) {
    String topic = fullTopic(shortTopic);
    String subscription = fullSubscription(shortSubscription);
    try {
      Collection<String> existingSubscriptions =
          pubsubClient.listSubscriptions("projects/" + project, topic);
      return existingSubscriptions.contains(subscription);
    } catch (IOException e) {
      throw new RuntimeException("Unable to check Pubsub subscription" + subscription + ": ", e);
    }
  }

  /**
   * Delete all the subscriptions and topics we created.
   */
  @Override
  public void close() {
    for (String subscription : createdSubscriptions) {
      try {
        NexmarkUtils.console("delete subscription %s", subscription);
        pubsubClient.deleteSubscription(subscription);
      } catch (IOException ex) {
        NexmarkUtils.console("could not delete subscription %s", subscription);
      }
    }
    for (String topic : createdTopics) {
      try {
        NexmarkUtils.console("delete topic %s", topic);
        pubsubClient.deleteTopic(topic);
      } catch (IOException ex) {
        NexmarkUtils.console("could not delete topic %s", topic);
      }
    }
  }
}

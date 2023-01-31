/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.pubsub;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.util.Map;

/** Interface for managing Pub/Sub resources in integration tests. */
public interface PubsubResourceManager {

  /**
   * Creates a topic with the given name on Pub/Sub.
   *
   * @param topicName Topic name to create. The underlying implementation may not use the topic name
   *     directly, and can add a prefix or a suffix to identify specific executions.
   * @return The instance of the TopicName that was just created.
   */
  TopicName createTopic(String topicName);

  /**
   * Creates a subscription at the specific topic, with a given name.
   *
   * @param topicName Topic Name reference to add the subscription.
   * @param subscriptionName Name of the subscription to use. Note that the underlying
   *     implementation may not use the subscription name literally, and can use a prefix or a
   *     suffix to identify specific executions.
   * @return The instance of the SubscriptionName that was just created.
   */
  SubscriptionName createSubscription(TopicName topicName, String subscriptionName);

  /**
   * Publishes a message with the given data to the publisher context's topic.
   *
   * @param topic Reference to the topic to send the message.
   * @param attributes Attributes to send with the message.
   * @param data Byte data to send.
   * @return The message id that was generated.
   */
  String publish(TopicName topic, Map<String, String> attributes, ByteString data)
      throws PubsubResourceManagerException;

  /**
   * Pulls messages from the given subscription.
   *
   * @param subscriptionName Name of the subscription to use.
   * @return The message id that was generated.
   */
  PullResponse pull(SubscriptionName subscriptionName, int maxMessages);

  /** Delete any topics or subscriptions created by this manager. */
  void cleanupAll();
}

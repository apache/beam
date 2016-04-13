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

package com.google.cloud.dataflow.sdk.io;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/**
 * A helper interface for talking to Pubsub via an underlying transport.
 */
public interface PubsubClient extends AutoCloseable {
  /**
   * Path representing a cloud project id.
   */
  class ProjectPath implements Serializable {
    private final String path;

    public ProjectPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ProjectPath that = (ProjectPath) o;

      return path.equals(that.path);

    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }

    public static ProjectPath fromId(String projectId) {
      return new ProjectPath(String.format("projects/%s", projectId));
    }
  }

  /**
   * Path representing a Pubsub subscription.
   */
  class SubscriptionPath implements Serializable {
    private final String path;

    public SubscriptionPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public String getV1Beta1Path() {
      String[] splits = path.split("/");
      Preconditions.checkState(splits.length == 4);
      return String.format("/subscriptions/%s/%s", splits[1], splits[3]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubscriptionPath that = (SubscriptionPath) o;
      return path.equals(that.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }

    public static SubscriptionPath fromName(String projectId, String subscriptionName) {
      return new SubscriptionPath(String.format("projects/%s/subscriptions/%s",
          projectId, subscriptionName));
    }
  }

  /**
   * Path representing a Pubsub topic.
   */
  class TopicPath implements Serializable {
    private final String path;

    public TopicPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public String getV1Beta1Path() {
      String[] splits = path.split("/");
      Preconditions.checkState(splits.length == 4);
      return String.format("/topics/%s/%s", splits[1], splits[3]);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicPath topicPath = (TopicPath) o;
      return path.equals(topicPath.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }

    public static TopicPath fromName(String projectId, String topicName) {
      return new TopicPath(String.format("projects/%s/topics/%s", projectId, topicName));
    }
  }

  /**
   * A message to be sent to Pubsub.
   */
  class OutgoingMessage {
    /**
     * Underlying (encoded) element.
     */
    public final byte[] elementBytes;

    /**
     * Timestamp for element (ms since epoch).
     */
    public final long timestampMsSinceEpoch;

    public OutgoingMessage(byte[] elementBytes, long timestampMsSinceEpoch) {
      this.elementBytes = elementBytes;
      this.timestampMsSinceEpoch = timestampMsSinceEpoch;
    }
  }

  /**
   * A message received from Pubsub.
   */
  class IncomingMessage {
    /**
     * Underlying (encoded) element.
     */
    public final byte[] elementBytes;

    /**
     * Timestamp for element (ms since epoch). Either Pubsub's processing time,
     * or the custom timestamp associated with the message.
     */
    public final long timestampMsSinceEpoch;

    /**
     * Timestamp (in system time) at which we requested the message (ms since epoch).
     */
    public final long requestTimeMsSinceEpoch;

    /**
     * Id to pass back to Pubsub to acknowledge receipt of this message.
     */
    public final String ackId;

    /**
     * Id to pass to the runner to distinguish this message from all others.
     */
    public final byte[] recordId;

    public IncomingMessage(
        byte[] elementBytes,
        long timestampMsSinceEpoch,
        long requestTimeMsSinceEpoch,
        String ackId,
        byte[] recordId) {
      this.elementBytes = elementBytes;
      this.timestampMsSinceEpoch = timestampMsSinceEpoch;
      this.requestTimeMsSinceEpoch = requestTimeMsSinceEpoch;
      this.ackId = ackId;
      this.recordId = recordId;
    }
  }

  /**
   * Gracefully close the underlying transport.
   */
  @Override
  void close();


  /**
   * Publish {@code outgoingMessages} to Pubsub {@code topic}. Return number of messages
   * published.
   *
   * @throws IOException
   */
  int publish(TopicPath topic, Iterable<OutgoingMessage> outgoingMessages) throws IOException;

  /**
   * Request the next batch of up to {@code batchSize} messages from {@code subscription}.
   * Return the received messages, or empty collection if none were available. Does not
   * wait for messages to arrive. Returned messages will record heir request time
   * as {@code requestTimeMsSinceEpoch}.
   *
   * @throws IOException
   */
  Collection<IncomingMessage> pull(
      long requestTimeMsSinceEpoch, SubscriptionPath subscription, int batchSize)
      throws IOException;

  /**
   * Acknowldege messages from {@code subscription} with {@code ackIds}.
   *
   * @throws IOException
   */
  void acknowledge(SubscriptionPath subscription, Iterable<String> ackIds) throws IOException;

  /**
   * Modify the ack deadline for messages from {@code subscription} with {@code ackIds} to
   * be {@code deadlineSeconds} from now.
   *
   * @throws IOException
   */
  void modifyAckDeadline(
      SubscriptionPath subscription, Iterable<String> ackIds,
      int deadlineSeconds)
      throws IOException;

  /**
   * Create {@code topic}.
   *
   * @throws IOException
   */
  void createTopic(TopicPath topic) throws IOException;

  /*
   * Delete {@code topic}.
   *
   * @throws IOException
   */
  void deleteTopic(TopicPath topic) throws IOException;

  /**
   * Return a list of topics for {@code project}.
   *
   * @throws IOException
   */
  Collection<TopicPath> listTopics(ProjectPath project) throws IOException;

  /**
   * Create {@code subscription} to {@code topic}.
   *
   * @throws IOException
   */
  void createSubscription(
      TopicPath topic, SubscriptionPath subscription,
      int ackDeadlineSeconds) throws IOException;

  /**
   * Delete {@code subscription}.
   *
   * @throws IOException
   */
  void deleteSubscription(SubscriptionPath subscription) throws IOException;

  /**
   * Return a list of subscriptions for {@code topic} in {@code project}.
   *
   * @throws IOException
   */
  Collection<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException;
}

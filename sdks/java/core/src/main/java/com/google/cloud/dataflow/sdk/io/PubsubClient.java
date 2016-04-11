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

import java.io.IOException;
import java.util.Collection;

/**
 * A helper interface for talking to pub/sub via an underlying transport.
 */
public interface PubsubClient extends AutoCloseable {
  /**
   * Gracefully close the underlying transport.
   */
  @Override
  void close();

  /**
   * A message to be sent to pub/sub.
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
   * A message received from pub/sub.
   */
  class IncomingMessage {
    /**
     * Underlying (encoded) element.
     */
    public final byte[] elementBytes;

    /**
     * Timestamp for element (ms since epoch). Either pub/sub's processing time,
     * or the custom timestamp associated with the message.
     */
    public final long timestampMsSinceEpoch;

    /**
     * Timestamp (in system time) at which we requested the message (ms since epoch).
     */
    public final long requestTimeMsSinceEpoch;

    /**
     * Id to pass back to pub/sub to acknowledge receipt of this message.
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
   * Publish {@code outgoingMessages} to pub/sub {@code topic}. Return number of messages
   * published.
   *
   * @throws IOException
   */
  int publish(String topic, Iterable<OutgoingMessage> outgoingMessages) throws IOException;

  /**
   * Request the next batch of up to {@code batchSize} messages from {@code subscription}.
   * Return the received messages, or empty collection if none were available. Does not
   * wait for messages to arrive. Returned messages will record heir request time
   * as {@code requestTimeMsSinceEpoch}.
   *
   * @throws IOException
   */
  Collection<IncomingMessage> pull(
      long requestTimeMsSinceEpoch, String subscription, int
      batchSize) throws IOException;

  /**
   * Acknowldege messages from {@code subscription} with {@code ackIds}.
   *
   * @throws IOException
   */
  void acknowledge(String subscription, Iterable<String> ackIds) throws IOException;

  /**
   * Modify the ack deadline for messages from {@code subscription} with {@code ackIds} to
   * be {@code deadlineSeconds} from now.
   *
   * @throws IOException
   */
  void modifyAckDeadline(String subscription, Iterable<String> ackIds, int deadlineSeconds)
      throws IOException;

  /**
   * Create {@code topic}.
   *
   * @throws IOException
   */
  void createTopic(String topic) throws IOException;

  /*
   * Delete {@code topic}.
   *
   * @throws IOException
   */
  void deleteTopic(String topic) throws IOException;

  /**
   * Return a list of topics for {@code project}.
   *
   * @throws IOException
   */
  Collection<String> listTopics(String project) throws IOException;

  /**
   * Create {@code subscription} to {@code topic}.
   *
   * @throws IOException
   */
  void createSubscription(String topic, String subscription, int ackDeadlineSeconds) throws
      IOException;

  /**
   * Delete {@code subscription}.
   *
   * @throws IOException
   */
  void deleteSubscription(String subscription) throws IOException;

  /**
   * Return a list of subscriptions for {@code topic} in {@code project}.
   *
   * @throws IOException
   */
  Collection<String> listSubscriptions(String project, String topic) throws IOException;
}

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

package org.apache.beam.sdk.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.options.PubsubOptions;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * A (partial) implementation of {@link PubsubClient} for use by unit tests. Only suitable for
 * testing {@link #publish}, {@link #pull}, {@link #acknowledge} and {@link #modifyAckDeadline}
 * methods.
 */
public class PubsubTestClient extends PubsubClient {
  public static PubsubClientFactory createFactoryForPublish(
      final TopicPath expectedTopic,
      final Set<OutgoingMessage> expectedOutgoingMessages) {
    return new PubsubClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampLabel, @Nullable String idLabel, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient(expectedTopic, null, 0, expectedOutgoingMessages, null);
      }

      @Override
      public String getKind() {
        return "PublishTest";
      }
    };
  }

  public static PubsubClientFactory createFactoryForPull(
      @Nullable final SubscriptionPath expectedSubscription,
      final int ackTimeoutSec,
      @Nullable final List<IncomingMessage> expectedIncomingMessages) {
    return new PubsubClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampLabel, @Nullable String idLabel, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient(null, expectedSubscription, ackTimeoutSec,
                                    null, expectedIncomingMessages);
      }

      @Override
      public String getKind() {
        return "PullTest";
      }
    };
  }

  /**
   * Only publish calls for this topic are allowed.
   */
  @Nullable
  private TopicPath expectedTopic;
  /**
   * Only pull calls for this subscription are allowed.
   */
  @Nullable
  private SubscriptionPath expectedSubscription;

  /**
   * Timeout to simulate.
   */
  private int ackTimeoutSec;

  /**
   * Messages yet to seen in a {@link #publish} call.
   */
  @Nullable
  private Set<OutgoingMessage> remainingExpectedOutgoingMessages;

  /**
   * Messages waiting to be received by a {@link #pull} call.
   */
  @Nullable
  private List<IncomingMessage> remainingPendingIncomingMessages;

  /**
   * Messages which have been returned from a {@link #pull} call and
   * not yet ACKed by an {@link #acknowledge} call.
   */
  private Map<String, IncomingMessage> pendingAckIncommingMessages;

  /**
   * When above messages are due to have their ACK deadlines expire.
   */
  private Map<String, Long> ackDeadline;

  /**
   * Current time.
   */
  private long nowMsSinceEpoch;

  @VisibleForTesting
  PubsubTestClient(
      @Nullable TopicPath expectedTopic,
      @Nullable SubscriptionPath expectedSubscription,
      int ackTimeoutSec,
      @Nullable Set<OutgoingMessage> expectedOutgoingMessages,
      @Nullable List<IncomingMessage> expectedIncomingMessages) {
    this.expectedTopic = expectedTopic;
    this.expectedSubscription = expectedSubscription;
    this.ackTimeoutSec = ackTimeoutSec;

    this.remainingExpectedOutgoingMessages = expectedOutgoingMessages;
    this.remainingPendingIncomingMessages = expectedIncomingMessages;

    this.pendingAckIncommingMessages = new HashMap<>();
    this.ackDeadline = new HashMap<>();
    this.nowMsSinceEpoch = Long.MIN_VALUE;
  }

  /**
   * Advance wall-clock time to {@code newNowMsSinceEpoch}. This will simulate Pubsub expiring
   * outstanding ACKs.
   */
  public void advanceTo(long newNowMsSinceEpoch) {
    checkArgument(newNowMsSinceEpoch >= nowMsSinceEpoch,
                  "Cannot advance time backwards from %d to %d", nowMsSinceEpoch,
                  newNowMsSinceEpoch);
    nowMsSinceEpoch = newNowMsSinceEpoch;
    // Any messages who's ACKs timed out are available for re-pulling.
    Iterator<Map.Entry<String, Long>> deadlineItr = ackDeadline.entrySet().iterator();
    while (deadlineItr.hasNext()) {
      Map.Entry<String, Long> entry = deadlineItr.next();
      if (entry.getValue() <= nowMsSinceEpoch) {
        remainingPendingIncomingMessages.add(pendingAckIncommingMessages.remove(entry.getKey()));
        deadlineItr.remove();
      }
    }
  }

  @Override
  public void close() {
    if (remainingExpectedOutgoingMessages != null) {
      checkState(this.remainingExpectedOutgoingMessages.isEmpty(),
                 "Failed to pull %d messages", this.remainingExpectedOutgoingMessages.size());
      remainingExpectedOutgoingMessages = null;
    }
    if (remainingPendingIncomingMessages != null) {
      checkState(remainingPendingIncomingMessages.isEmpty(),
                 "Failed to publish %d messages", remainingPendingIncomingMessages.size());
      checkState(pendingAckIncommingMessages.isEmpty(),
                 "Failed to ACK %d messages", pendingAckIncommingMessages.size());
      checkState(ackDeadline.isEmpty(),
                 "Failed to ACK %d messages", ackDeadline.size());
      remainingPendingIncomingMessages = null;
      pendingAckIncommingMessages = null;
      ackDeadline = null;
    }
  }

  @Override
  public int publish(
      TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    checkNotNull(expectedTopic, "Missing expected topic");
    checkNotNull(remainingExpectedOutgoingMessages, "Missing expected outgoing messages");
    checkState(topic.equals(expectedTopic), "Topic %s does not match expected %s", topic,
               expectedTopic);
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      checkState(remainingExpectedOutgoingMessages.remove(outgoingMessage),
                 "Unexpeced outgoing message %s", outgoingMessage);
    }
    return outgoingMessages.size();
  }

  @Override
  public List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch, SubscriptionPath subscription, int batchSize,
      boolean returnImmediately) throws IOException {
    checkState(requestTimeMsSinceEpoch == nowMsSinceEpoch,
               "Simulated time %d does not match requset time %d", nowMsSinceEpoch,
               requestTimeMsSinceEpoch);
    checkNotNull(expectedSubscription, "Missing expected subscription");
    checkNotNull(remainingPendingIncomingMessages, "Missing expected incoming messages");
    checkState(subscription.equals(expectedSubscription),
               "Subscription %s does not match expected %s", subscription, expectedSubscription);
    checkState(returnImmediately, "PubsubTestClient only supports returning immediately");

    List<IncomingMessage> incomingMessages = new ArrayList<>();
    Iterator<IncomingMessage> pendItr = remainingPendingIncomingMessages.iterator();
    while (pendItr.hasNext()) {
      IncomingMessage incomingMessage = pendItr.next();
      pendItr.remove();
      IncomingMessage incomingMessageWithRequestTime =
          incomingMessage.withRequestTime(requestTimeMsSinceEpoch);
      incomingMessages.add(incomingMessageWithRequestTime);
      pendingAckIncommingMessages.put(incomingMessageWithRequestTime.ackId,
                                      incomingMessageWithRequestTime);
      ackDeadline.put(incomingMessageWithRequestTime.ackId,
                      requestTimeMsSinceEpoch + ackTimeoutSec * 1000);
      if (incomingMessages.size() >= batchSize) {
        break;
      }
    }
    return incomingMessages;
  }

  @Override
  public void acknowledge(
      SubscriptionPath subscription,
      List<String> ackIds) throws IOException {
    checkNotNull(expectedSubscription, "Missing expected subscription");
    checkNotNull(remainingPendingIncomingMessages, "Missing expected incoming messages");
    checkState(subscription.equals(expectedSubscription),
               "Subscription %s does not match expected %s", subscription, expectedSubscription);

    for (String ackId : ackIds) {
      checkState(ackDeadline.remove(ackId) != null,
                 "No message with ACK id %s is outstanding", ackId);
      checkState(pendingAckIncommingMessages.remove(ackId) != null,
                 "No message with ACK id %s is outstanding", ackId);
    }
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    checkNotNull(expectedSubscription, "Missing expected subscription");
    checkNotNull(remainingPendingIncomingMessages, "Missing expected incoming messages");
    checkState(subscription.equals(expectedSubscription),
               "Subscription %s does not match expected %s", subscription, expectedSubscription);

    for (String ackId : ackIds) {
      checkState(ackDeadline.remove(ackId) != null,
                 "No message with ACK id %s is outstanding", ackId);
      checkState(pendingAckIncommingMessages.containsKey(ackId),
                 "No message with ACK id %s is outstanding", ackId);
      ackDeadline.put(ackId, nowMsSinceEpoch + deadlineSeconds * 1000);
    }
  }

  @Override
  public void createTopic(TopicPath topic) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTopic(TopicPath topic) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<TopicPath> listTopics(ProjectPath project) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createSubscription(
      TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSubscription(SubscriptionPath subscription) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<SubscriptionPath> listSubscriptions(
      ProjectPath project, TopicPath topic) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int ackDeadlineSeconds(SubscriptionPath subscription) throws IOException {
    return ackTimeoutSec;
  }
}

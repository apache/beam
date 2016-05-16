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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.options.PubsubOptions;

import com.google.api.client.util.Clock;
import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.ArrayList;
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
        return new PubsubTestClient(checkNotNull(expectedTopic),
                                    checkNotNull(expectedOutgoingMessages), null, null, 0,
                                    null, null, null);
      }

      @Override
      public String getKind() {
        return "PublishTest";
      }
    };
  }

  public static PubsubClientFactory createFactoryForPull(
      final Clock clock,
      final SubscriptionPath expectedSubscription,
      final int ackTimeoutSec,
      final List<IncomingMessage> expectedIncomingMessages,
      final Map<String, IncomingMessage> pendingAckIncomingMessages,
      final Map<String, Long> ackDeadline) {
    return new PubsubClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampLabel, @Nullable String idLabel, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient(null, null, checkNotNull(clock),
                                    checkNotNull(expectedSubscription),
                                    ackTimeoutSec,
                                    checkNotNull(expectedIncomingMessages),
                                    checkNotNull(pendingAckIncomingMessages),
                                    checkNotNull(ackDeadline));
      }

      @Override
      public String getKind() {
        return "PullTest";
      }
    };
  }

  /**
   * Publish mode only: Only publish calls for this topic are allowed.
   */
  @Nullable
  private TopicPath expectedTopic;

  /**
   * Publish mode only: Messages yet to seen in a {@link #publish} call.
   */
  @Nullable
  private Set<OutgoingMessage> remainingExpectedOutgoingMessages;

  /**
   * Pull mode only: Clock from which to get current time.
   */
  @Nullable
  private Clock clock;

  /**
   * Pull mode only: Only pull calls for this subscription are allowed.
   */
  @Nullable
  private SubscriptionPath expectedSubscription;

  /**
   * Pull mode only: Timeout to simulate.
   */
  private int ackTimeoutSec;

  /**
   * Pull mode only: Messages waiting to be received by a {@link #pull} call.
   */
  @Nullable
  private List<IncomingMessage> remainingPendingIncomingMessages;

  /**
   * Pull mode only: Messages which have been returned from a {@link #pull} call and
   * not yet ACKed by an {@link #acknowledge} call.
   */
  @Nullable
  private Map<String, IncomingMessage> pendingAckIncomingMessages;

  /**
   * Pull mode only: When above messages are due to have their ACK deadlines expire.
   */
  @Nullable
  private Map<String, Long> ackDeadline;

  @VisibleForTesting
  PubsubTestClient(
      @Nullable TopicPath expectedTopic,
      @Nullable Set<OutgoingMessage> expectedOutgoingMessages,
      @Nullable Clock clock,
      @Nullable SubscriptionPath expectedSubscription,
      int ackTimeoutSec,
      @Nullable List<IncomingMessage> expectedIncomingMessages,
      @Nullable Map<String, IncomingMessage> pendingAckIncomingMessages,
      @Nullable Map<String, Long> ackDeadline) {
    this.expectedTopic = expectedTopic;
    this.remainingExpectedOutgoingMessages = expectedOutgoingMessages;
    this.clock = clock;
    this.expectedSubscription = expectedSubscription;
    this.ackTimeoutSec = ackTimeoutSec;
    this.remainingPendingIncomingMessages = expectedIncomingMessages;
    this.pendingAckIncomingMessages = pendingAckIncomingMessages;
    this.ackDeadline = ackDeadline;
  }

  /**
   * Return true if in pull mode, false if in publish mode.
   */
  private boolean inPullMode() {
    return expectedSubscription != null;
  }

  /**
   * For subscription mode only:
   * Track progression of time according to {@link #clock}. This will simulate Pubsub expiring
   * outstanding ACKs.
   */
  public void advance() {
    checkState(inPullMode(), "Can only advance in pull mode");
    // Any messages who's ACKs timed out are available for re-pulling.
    Iterator<Map.Entry<String, Long>> deadlineItr = ackDeadline.entrySet().iterator();
    while (deadlineItr.hasNext()) {
      Map.Entry<String, Long> entry = deadlineItr.next();
      if (entry.getValue() <= clock.currentTimeMillis()) {
        remainingPendingIncomingMessages.add(pendingAckIncomingMessages.remove(entry.getKey()));
        deadlineItr.remove();
      }
    }
  }

  @Override
  public void close() {
    if (remainingExpectedOutgoingMessages != null) {
      checkState(this.remainingExpectedOutgoingMessages.isEmpty(),
                 "Failed to pull %s messages", this.remainingExpectedOutgoingMessages.size());
      remainingExpectedOutgoingMessages = null;
    }
    if (remainingPendingIncomingMessages != null) {
      checkState(remainingPendingIncomingMessages.isEmpty(),
                 "Failed to publish %s messages", remainingPendingIncomingMessages.size());
      checkState(pendingAckIncomingMessages.isEmpty(),
                 "Failed to ACK %s messages", pendingAckIncomingMessages.size());
      checkState(ackDeadline.isEmpty(),
                 "Failed to ACK %s messages", ackDeadline.size());
      remainingPendingIncomingMessages = null;
      pendingAckIncomingMessages = null;
      ackDeadline = null;
    }
  }

  @Override
  public int publish(
      TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    checkState(!inPullMode(), "Can only publish in publish mode");
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
    checkState(inPullMode(), "Can only pull in pull mode");
    long now = clock.currentTimeMillis();
    checkState(requestTimeMsSinceEpoch == now,
               "Simulated time %s does not match request time %s", now, requestTimeMsSinceEpoch);
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
      pendingAckIncomingMessages.put(incomingMessageWithRequestTime.ackId,
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
    checkState(inPullMode(), "Can only acknowledge in pull mode");
    checkState(subscription.equals(expectedSubscription),
               "Subscription %s does not match expected %s", subscription, expectedSubscription);

    for (String ackId : ackIds) {
      checkState(ackDeadline.remove(ackId) != null,
                 "No message with ACK id %s is outstanding", ackId);
      checkState(pendingAckIncomingMessages.remove(ackId) != null,
                 "No message with ACK id %s is outstanding", ackId);
    }
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    checkState(inPullMode(), "Can only modify ack deadline in pull mode");
    checkState(subscription.equals(expectedSubscription),
               "Subscription %s does not match expected %s", subscription, expectedSubscription);

    for (String ackId : ackIds) {
      if (deadlineSeconds > 0) {
        checkState(ackDeadline.remove(ackId) != null,
                   "No message with ACK id %s is outstanding", ackId);
        checkState(pendingAckIncomingMessages.containsKey(ackId),
                   "No message with ACK id %s is outstanding", ackId);
        ackDeadline.put(ackId, clock.currentTimeMillis() + deadlineSeconds * 1000);
      } else {
        checkState(ackDeadline.remove(ackId) != null,
                   "No message with ACK id %s is outstanding", ackId);
        IncomingMessage message = pendingAckIncomingMessages.remove(ackId);
        checkState(message != null, "No message with ACK id %s is outstanding", ackId);
        remainingPendingIncomingMessages.add(message);
      }
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

  @Override
  public boolean isEOF() {
    checkState(inPullMode(), "Can only check EOF in pull mode");
    return remainingPendingIncomingMessages.isEmpty();
  }
}

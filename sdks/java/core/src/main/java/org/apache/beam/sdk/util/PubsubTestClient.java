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

import org.apache.beam.sdk.options.PubsubOptions;

import com.google.common.base.Preconditions;

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
 * testing {@link #publish}, {@link #pull}, {@link #acknowledge} and {@link #modifyAckDeadline}.
 */
public class PubsubTestClient extends PubsubClient {
  public static PubsubClientFactory createFactory(
      final TopicPath expectedTopic,
      final Set<OutgoingMessage> remainingExpectedOutgoingMessages,
      final SubscriptionPath expectedSubscription,
      final List<IncomingMessage> remainingPendingIncomingMessages,
      final long ackTimeoutMs) {
    return new PubsubClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampLabel, @Nullable String idLabel, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient(expectedTopic, remainingExpectedOutgoingMessages,
                                    expectedSubscription, remainingPendingIncomingMessages,
                                    ackTimeoutMs);
      }
    };
  }


  private TopicPath expectedTopic;
  private Set<OutgoingMessage> remainingExpectedOutgoingMessages;

  private SubscriptionPath expectedSubscription;
  private List<IncomingMessage> remainingPendingIncomingMessages;

  private long ackTimeoutMs;
  private Map<String, IncomingMessage> pendingAckIncommingMessages;
  private Map<String, Long> ackDeadline;

  private long currentTimeMsSinceEpoch;

  private PubsubTestClient(
      TopicPath expectedTopic,
      Set<OutgoingMessage> remainingExpectedOutgoingMessages,
      SubscriptionPath expectedSubscription,
      List<IncomingMessage> remainingPendingIncomingMessages,
      long ackTimeoutMs) {
    this.expectedTopic = expectedTopic;
    this.remainingExpectedOutgoingMessages = remainingExpectedOutgoingMessages;
    this.expectedSubscription = expectedSubscription;
    this.remainingPendingIncomingMessages = remainingPendingIncomingMessages;
    this.ackTimeoutMs = ackTimeoutMs;

    this.pendingAckIncommingMessages = new HashMap<>();
    this.ackDeadline = new HashMap<>();
    this.currentTimeMsSinceEpoch = 0;
  }

  @Override
  public void close() {
    if (remainingExpectedOutgoingMessages != null) {
      Preconditions.checkState(remainingExpectedOutgoingMessages.isEmpty());
    }
    if (remainingPendingIncomingMessages != null) {
      Preconditions.checkState(remainingPendingIncomingMessages.isEmpty());
      Preconditions.checkState(pendingAckIncommingMessages.isEmpty());
      Preconditions.checkState(ackDeadline.isEmpty());
    }
  }

  @Override
  public int publish(
      TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    Preconditions.checkState(topic.equals(expectedTopic));
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      Preconditions.checkState(remainingExpectedOutgoingMessages.remove(outgoingMessage));
    }
    return outgoingMessages.size();
  }

  private void timeout() {
    // Any messages who's acks timed out are available for re-pulling.
    Iterator<Map.Entry<String, Long>> deadlineItr = ackDeadline.entrySet().iterator();
    while (deadlineItr.hasNext()) {
      Map.Entry<String, Long> entry = deadlineItr.next();
      if (entry.getValue() <= currentTimeMsSinceEpoch) {
        remainingPendingIncomingMessages.add(pendingAckIncommingMessages.remove(entry.getKey()));
        deadlineItr.remove();
      }
    }
  }


  @Override
  public List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch, SubscriptionPath subscription, int batchSize,
      boolean returnImmediately) throws IOException {
    Preconditions.checkState(subscription.equals(expectedSubscription));
    Preconditions.checkState(returnImmediately);

    timeout();

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
      ackDeadline.put(incomingMessageWithRequestTime.ackId, requestTimeMsSinceEpoch + ackTimeoutMs);
      if (incomingMessages.size() >= batchSize) {
        break;
      }
    }
    return incomingMessages;
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, List<String> ackIds) throws IOException {
    Preconditions.checkState(subscription.equals(expectedSubscription));

    timeout();

    for (String ackId : ackIds) {
      Preconditions.checkState(ackDeadline.remove(ackId) != null);
      Preconditions.checkState(pendingAckIncommingMessages.remove(ackId) != null);
    }
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    Preconditions.checkState(subscription.equals(expectedSubscription));

    timeout();

    for (String ackId : ackIds) {
      Preconditions.checkState(ackDeadline.remove(ackId) != null);
      ackDeadline.put(ackId, currentTimeMsSinceEpoch + deadlineSeconds * 1000);
      Preconditions.checkState(pendingAckIncommingMessages.containsKey(ackId));
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
}

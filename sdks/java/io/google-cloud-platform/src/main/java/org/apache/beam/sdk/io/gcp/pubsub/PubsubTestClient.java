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
package org.apache.beam.sdk.io.gcp.pubsub;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A (partial) implementation of {@link PubsubClient} for use by unit tests. Only suitable for
 * testing {@link #publish}, {@link #pull}, {@link #acknowledge} and {@link #modifyAckDeadline}
 * methods. Relies on statics to mimic the Pubsub service, though we try to hide that.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubTestClient extends PubsubClient implements Serializable {
  /**
   * Mimic the state of the simulated Pubsub 'service'.
   *
   * <p>Note that the {@link PubsubTestClientFactory} is serialized/deserialized even when running
   * test pipelines. Meanwhile it is valid for multiple {@link PubsubTestClient}s to be created from
   * the same client factory and run in parallel. Thus we can't enforce aliasing of the following
   * data structures over all clients and must resort to a static.
   */
  private static class State {
    /** True if has been primed for a test but not yet validated. */
    boolean isActive;

    boolean isPublish;

    /** Publish mode only: Only publish calls for this topic are allowed. */
    @Nullable TopicPath expectedTopic;

    /** Publish mode only: Messages yet to seen in a {@link #publish} call. */
    @Nullable Set<OutgoingMessage> remainingExpectedOutgoingMessages;

    /**
     * Publish mode only: Messages which should throw when first sent to simulate transient publish
     * failure.
     */
    @Nullable Set<OutgoingMessage> remainingFailingOutgoingMessages;

    /** Pull mode only: Clock from which to get current time. */
    @Nullable Clock clock;

    /** Pull mode only: Only pull calls for this subscription are allowed. */
    @Nullable SubscriptionPath expectedSubscription;

    /** Pull mode only: Timeout to simulate. */
    int ackTimeoutSec;

    /** Pull mode only: Messages waiting to be received by a {@link #pull} call. */
    @Nullable List<IncomingMessage> remainingPendingIncomingMessages;

    /**
     * Pull mode only: Messages which have been returned from a {@link #pull} call and not yet ACKed
     * by an {@link #acknowledge} call.
     */
    @Nullable Map<String, IncomingMessage> pendingAckIncomingMessages;

    /** Pull mode only: When above messages are due to have their ACK deadlines expire. */
    @Nullable Map<String, Long> ackDeadline;

    /** The Pub/Sub schema resource path. */
    @Nullable SchemaPath expectedSchemaPath;

    /** Expected Pub/sub mapped Beam Schema. */
    @Nullable Schema expectedSchema;
  }

  private static final State STATE = new State();

  /** Closing the factory will validate all expected messages were processed. */
  public interface PubsubTestClientFactory extends PubsubClientFactory, Closeable, Serializable {
    default <T> PubsubIO.Read<T> setClock(PubsubIO.Read<T> readTransform, Clock clock) {
      return readTransform.withClock(clock);
    }
  }

  /**
   * Return a factory for testing publishers. Only one factory may be in-flight at a time. The
   * factory must be closed when the test is complete, at which point final validation will occur.
   */
  public static PubsubTestClientFactory createFactoryForPublish(
      final @Nullable TopicPath expectedTopic,
      final Iterable<OutgoingMessage> expectedOutgoingMessages,
      final Iterable<OutgoingMessage> failingOutgoingMessages) {
    activate(
        () -> setPublishState(expectedTopic, expectedOutgoingMessages, failingOutgoingMessages));
    return new PubsubTestClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute,
          @Nullable String idAttribute,
          PubsubOptions options,
          @Nullable String rootUrlOverride)
          throws IOException {
        return newClient(timestampAttribute, idAttribute, options);
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient();
      }

      @Override
      public String getKind() {
        return "PublishTest";
      }

      @Override
      public void close() {
        deactivate(PubsubTestClient::performFinalPublishStateChecks);
      }
    };
  }

  /**
   * Return a factory for testing subscribers. Only one factory may be in-flight at a time. The
   * factory must be closed when the test in complete
   */
  public static PubsubTestClientFactory createFactoryForPull(
      final Clock clock,
      final SubscriptionPath expectedSubscription,
      final int ackTimeoutSec,
      final Iterable<IncomingMessage> expectedIncomingMessages) {
    activate(
        () -> setPullState(expectedSubscription, clock, ackTimeoutSec, expectedIncomingMessages));
    return new PubsubTestClientFactory() {
      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute,
          @Nullable String idAttribute,
          PubsubOptions options,
          @Nullable String rootUrlOverride)
          throws IOException {
        return newClient(timestampAttribute, idAttribute, options);
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient();
      }

      @Override
      public String getKind() {
        return "PullTest";
      }

      @Override
      public void close() {
        deactivate(PubsubTestClient::performFinalPullStateChecks);
      }
    };
  }

  /**
   * Returns a factory for a test that is expected to both publish and pull messages over the course
   * of the test.
   */
  public static PubsubTestClientFactory createFactoryForPullAndPublish(
      final SubscriptionPath pullSubscription,
      final TopicPath publishTopicPath,
      final Clock pullClock,
      final int pullAckTimeoutSec,
      final Iterable<IncomingMessage> expectedIncomingMessages,
      final Iterable<OutgoingMessage> expectedOutgoingMessages,
      final Iterable<OutgoingMessage> failingOutgoingMessages) {
    activate(
        () -> {
          setPublishState(publishTopicPath, expectedOutgoingMessages, failingOutgoingMessages);
          setPullState(pullSubscription, pullClock, pullAckTimeoutSec, expectedIncomingMessages);
        });
    return new PubsubTestClientFactory() {
      @Override
      public void close() throws IOException {
        deactivate(
            () -> {
              performFinalPublishStateChecks();
              performFinalPullStateChecks();
            });
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute,
          @Nullable String idAttribute,
          PubsubOptions options,
          @Nullable String rootUrlOverride)
          throws IOException {
        return newClient(timestampAttribute, idAttribute, options);
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient();
      }

      @Override
      public String getKind() {
        return "PublishAndPullTest";
      }
    };
  }

  public static PubsubTestClientFactory createFactoryForGetSchema(
      TopicPath expectedTopic,
      @Nullable SchemaPath expectedSchemaPath,
      @Nullable Schema expectedSchema) {
    return new PubsubTestClientFactory() {
      @Override
      public void close() {
        deactivate(() -> {});
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute,
          @Nullable String idAttribute,
          PubsubOptions options,
          @Nullable String rootUrlOverride) {
        activate(
            () -> {
              setSchemaState(expectedTopic, expectedSchemaPath, expectedSchema);
            });
        return new PubsubTestClient();
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
          throws IOException {
        return newClient(timestampAttribute, idAttribute, options, null);
      }

      @Override
      public String getKind() {
        return "GetSchemaTest";
      }
    };
  }

  /**
   * Activates {@link PubsubTestClientFactory} state for the test. This can only be called once per
   * test.
   *
   * <p>It is not necessary to set {@code STATE.isActive}. That will be set regardless.
   *
   * @param setStateValues a {@link Runnable} that sets all the desired values.
   */
  private static void activate(Runnable setStateValues) {
    synchronized (STATE) {
      checkState(!STATE.isActive, "Test still in flight");
      setStateValues.run();
      STATE.isActive = true;
    }
  }

  /**
   * Deactivates {@link PubsubTestClientFactory} state for use in other tests. This can only be
   * called once per test.
   *
   * <p>It is not necessary to check or set {@code STATE.isActivate}. That will be handled by this
   * method.
   *
   * @param runFinalChecks a {@link Runnable} to handle any final state checking before marking
   *     {@code STATE} as no longer active
   */
  private static void deactivate(Runnable runFinalChecks) {
    synchronized (STATE) {
      checkState(STATE.isActive, "No test still in flight");
      runFinalChecks.run();
      STATE.remainingExpectedOutgoingMessages = null;
      STATE.remainingPendingIncomingMessages = null;
      STATE.pendingAckIncomingMessages = null;
      STATE.ackDeadline = null;
      STATE.isActive = false;
    }
  }

  /** Handles setting {@code STATE} values for a publishing client. */
  private static void setPublishState(
      final @Nullable TopicPath expectedTopic,
      final Iterable<OutgoingMessage> expectedOutgoingMessages,
      final Iterable<OutgoingMessage> failingOutgoingMessages) {
    STATE.isPublish = true;
    STATE.expectedTopic = expectedTopic;
    STATE.remainingExpectedOutgoingMessages = Sets.newHashSet(expectedOutgoingMessages);
    STATE.remainingFailingOutgoingMessages = Sets.newHashSet(failingOutgoingMessages);
  }

  /** Handles setting {@code STATE} values for a pulling client. */
  private static void setPullState(
      final SubscriptionPath expectedSubscription,
      final Clock clock,
      final int ackTimeoutSec,
      final Iterable<IncomingMessage> expectedIncomingMessages) {
    STATE.clock = clock;
    STATE.expectedSubscription = expectedSubscription;
    STATE.ackTimeoutSec = ackTimeoutSec;
    STATE.remainingPendingIncomingMessages = Lists.newArrayList(expectedIncomingMessages);
    STATE.pendingAckIncomingMessages = new HashMap<>();
    STATE.ackDeadline = new HashMap<>();
  }

  private static void setSchemaState(
      TopicPath expectedTopic,
      @Nullable SchemaPath expectedSchemaPath,
      @Nullable Schema expectedSchema) {
    STATE.expectedTopic = expectedTopic;
    STATE.expectedSchemaPath = expectedSchemaPath;
    STATE.expectedSchema = expectedSchema;
  }

  /** Handles verifying {@code STATE} at end of publish test. */
  private static void performFinalPublishStateChecks() {
    checkState(STATE.isActive, "No test still in flight");
    checkState(
        STATE.remainingExpectedOutgoingMessages.isEmpty(),
        "Still waiting for %s messages to be published",
        STATE.remainingExpectedOutgoingMessages.size());
  }

  /** Handles verifying {@code STATE} at end of pull test. */
  private static void performFinalPullStateChecks() {
    checkState(
        STATE.remainingPendingIncomingMessages.isEmpty(),
        "Still waiting for %s messages to be pulled",
        STATE.remainingPendingIncomingMessages.size());
    checkState(
        STATE.pendingAckIncomingMessages.isEmpty(),
        "Still waiting for %s messages to be ACKed",
        STATE.pendingAckIncomingMessages.size());
    checkState(
        STATE.ackDeadline.isEmpty(),
        "Still waiting for %s messages to be ACKed",
        STATE.ackDeadline.size());
  }

  public static PubsubTestClientFactory createFactoryForCreateSubscription() {
    return new PubsubTestClientFactory() {
      int numCalls = 0;

      @Override
      public void close() throws IOException {
        checkState(
            numCalls == 1, "Expected exactly one subscription to be created, got %s", numCalls);
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute,
          @Nullable String idAttribute,
          PubsubOptions options,
          @Nullable String rootUrlOverride)
          throws IOException {
        return newClient(timestampAttribute, idAttribute, options);
      }

      @Override
      public PubsubClient newClient(
          @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
          throws IOException {
        return new PubsubTestClient() {
          @Override
          public void createSubscription(
              TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds)
              throws IOException {
            checkState(numCalls == 0, "Expected at most one subscription to be created");
            numCalls++;
          }
        };
      }

      @Override
      public String getKind() {
        return "CreateSubscriptionTest";
      }
    };
  }

  /** Return true if in pull mode. */
  private boolean inPullMode() {
    checkState(STATE.isActive, "No test is active");
    return STATE.expectedSubscription != null;
  }

  /** Return true if in publish mode. */
  private boolean inPublishMode() {
    checkState(STATE.isActive, "No test is active");
    return STATE.isPublish;
  }

  /**
   * For subscription mode only: Track progression of time according to the {@link Clock} passed .
   * This will simulate Pubsub expiring outstanding ACKs.
   */
  public void advance() {
    synchronized (STATE) {
      checkState(inPullMode(), "Can only advance in pull mode");
      // Any messages who's ACKs timed out are available for re-pulling.
      Iterator<Map.Entry<String, Long>> deadlineItr = STATE.ackDeadline.entrySet().iterator();
      while (deadlineItr.hasNext()) {
        Map.Entry<String, Long> entry = deadlineItr.next();
        if (entry.getValue() <= STATE.clock.currentTimeMillis()) {
          STATE.remainingPendingIncomingMessages.add(
              STATE.pendingAckIncomingMessages.remove(entry.getKey()));
          deadlineItr.remove();
        }
      }
    }
  }

  @Override
  public void close() {}

  @Override
  public int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    synchronized (STATE) {
      checkState(inPublishMode(), "Can only publish in publish mode");
      boolean isDynamic = STATE.expectedTopic == null;
      if (!isDynamic) {
        checkState(
            topic.equals(STATE.expectedTopic),
            "Topic %s does not match expected %s",
            topic,
            STATE.expectedTopic);
      }
      for (OutgoingMessage outgoingMessage : outgoingMessages) {
        if (isDynamic) {
          checkState(outgoingMessage.topic().equals(topic.getPath()));
        } else {
          checkState(outgoingMessage.topic() == null);
        }
        if (STATE.remainingFailingOutgoingMessages.remove(outgoingMessage)) {
          throw new RuntimeException("Simulating failure for " + outgoingMessage);
        }
        checkState(
            STATE.remainingExpectedOutgoingMessages.remove(outgoingMessage),
            "Unexpected outgoing message %s",
            outgoingMessage);
      }
      return outgoingMessages.size();
    }
  }

  @Override
  public List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch,
      SubscriptionPath subscription,
      int batchSize,
      boolean returnImmediately)
      throws IOException {
    synchronized (STATE) {
      checkState(inPullMode(), "Can only pull in pull mode");
      long now = STATE.clock.currentTimeMillis();
      checkState(
          requestTimeMsSinceEpoch == now,
          "Simulated time %s does not match request time %s",
          now,
          requestTimeMsSinceEpoch);
      checkState(
          subscription.equals(STATE.expectedSubscription),
          "Subscription %s does not match expected %s",
          subscription,
          STATE.expectedSubscription);
      checkState(returnImmediately, "Pull only supported if returning immediately");

      List<IncomingMessage> incomingMessages = new ArrayList<>();
      Iterator<IncomingMessage> pendItr = STATE.remainingPendingIncomingMessages.iterator();
      while (pendItr.hasNext()) {
        IncomingMessage incomingMessage = pendItr.next();
        pendItr.remove();
        IncomingMessage incomingMessageWithRequestTime =
            IncomingMessage.of(
                incomingMessage.message(),
                incomingMessage.timestampMsSinceEpoch(),
                requestTimeMsSinceEpoch,
                incomingMessage.ackId(),
                incomingMessage.recordId());
        incomingMessages.add(incomingMessageWithRequestTime);
        STATE.pendingAckIncomingMessages.put(
            incomingMessageWithRequestTime.ackId(), incomingMessageWithRequestTime);
        STATE.ackDeadline.put(
            incomingMessageWithRequestTime.ackId(),
            requestTimeMsSinceEpoch + STATE.ackTimeoutSec * 1000);
        if (incomingMessages.size() >= batchSize) {
          break;
        }
      }
      return incomingMessages;
    }
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, List<String> ackIds) throws IOException {
    synchronized (STATE) {
      checkState(inPullMode(), "Can only acknowledge in pull mode");
      checkState(
          subscription.equals(STATE.expectedSubscription),
          "Subscription %s does not match expected %s",
          subscription,
          STATE.expectedSubscription);

      for (String ackId : ackIds) {
        checkState(
            STATE.ackDeadline.remove(ackId) != null,
            "No message with ACK id %s is waiting for an ACK",
            ackId);
        checkState(
            STATE.pendingAckIncomingMessages.remove(ackId) != null,
            "No message with ACK id %s is waiting for an ACK",
            ackId);
      }
    }
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    synchronized (STATE) {
      checkState(inPullMode(), "Can only modify ack deadline in pull mode");
      checkState(
          subscription.equals(STATE.expectedSubscription),
          "Subscription %s does not match expected %s",
          subscription,
          STATE.expectedSubscription);

      for (String ackId : ackIds) {
        if (deadlineSeconds > 0) {
          checkState(
              STATE.ackDeadline.remove(ackId) != null,
              "No message with ACK id %s is waiting for an ACK",
              ackId);
          checkState(
              STATE.pendingAckIncomingMessages.containsKey(ackId),
              "No message with ACK id %s is waiting for an ACK",
              ackId);
          STATE.ackDeadline.put(ackId, STATE.clock.currentTimeMillis() + deadlineSeconds * 1000);
        } else {
          checkState(
              STATE.ackDeadline.remove(ackId) != null,
              "No message with ACK id %s is waiting for an ACK",
              ackId);
          IncomingMessage message = STATE.pendingAckIncomingMessages.remove(ackId);
          checkState(message != null, "No message with ACK id %s is waiting for an ACK", ackId);
          STATE.remainingPendingIncomingMessages.add(message);
        }
      }
    }
  }

  @Override
  public void createTopic(TopicPath topic) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTopic(TopicPath topic, SchemaPath schema) throws IOException {
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
  public List<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int ackDeadlineSeconds(SubscriptionPath subscription) throws IOException {
    synchronized (STATE) {
      return STATE.ackTimeoutSec;
    }
  }

  @Override
  public boolean isEOF() {
    synchronized (STATE) {
      checkState(inPullMode(), "Can only check EOF in pull mode");
      return STATE.remainingPendingIncomingMessages.isEmpty();
    }
  }

  @Override
  public void createSchema(
      SchemaPath schemaPath, String schemaContent, com.google.pubsub.v1.Schema.Type type)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Delete {@link SchemaPath}. */
  @Override
  public void deleteSchema(SchemaPath schemaPath) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaPath getSchemaPath(TopicPath topicPath) throws IOException {
    return STATE.expectedSchemaPath;
  }

  @Override
  public Schema getSchema(SchemaPath schemaPath) throws IOException {
    return STATE.expectedSchema;
  }
}

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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.projectPathFromPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.IncomingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Seconds;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Test rule which creates a new topic and subscription with randomized names and exposes the APIs
 * to work with them.
 *
 * <p>Deletes topic and subscription on shutdown.
 */
public class TestPubsub implements TestRule {
  private static final DateTimeFormatter DATETIME_FORMAT =
      DateTimeFormat.forPattern("YYYY-MM-dd-HH-mm-ss-SSS");
  private static final String EVENTS_TOPIC_NAME = "events";
  private static final String TOPIC_PREFIX = "integ-test-";
  private static final String NO_ID_ATTRIBUTE = null;
  private static final String NO_TIMESTAMP_ATTRIBUTE = null;
  private static final Integer DEFAULT_ACK_DEADLINE_SECONDS = 60;

  private final TestPubsubOptions pipelineOptions;

  private @Nullable PubsubClient pubsub = null;
  private @Nullable TopicPath eventsTopicPath = null;
  private @Nullable SubscriptionPath subscriptionPath = null;

  /**
   * Creates an instance of this rule.
   *
   * <p>Loads GCP configuration from {@link TestPipelineOptions}.
   */
  public static TestPubsub create() {
    TestPubsubOptions options = TestPipeline.testingPipelineOptions().as(TestPubsubOptions.class);
    return new TestPubsub(options);
  }

  private TestPubsub(TestPubsubOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (TestPubsub.this.pubsub != null) {
          throw new AssertionError(
              "Pubsub client was not shutdown in previous test. "
                  + "Topic path is'"
                  + eventsTopicPath
                  + "'. "
                  + "Current test: "
                  + description.getDisplayName());
        }

        try {
          initializePubsub(description);
          base.evaluate();
        } finally {
          tearDown();
        }
      }
    };
  }

  private void initializePubsub(Description description) throws IOException {
    pubsub =
        PubsubGrpcClient.FACTORY.newClient(
            NO_TIMESTAMP_ATTRIBUTE, NO_ID_ATTRIBUTE, pipelineOptions);
    TopicPath eventsTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, EVENTS_TOPIC_NAME));

    pubsub.createTopic(eventsTopicPathTmp);

    eventsTopicPath = eventsTopicPathTmp;
    subscriptionPath =
        pubsub.createRandomSubscription(
            projectPathFromPath(String.format("projects/%s", pipelineOptions.getProject())),
            topicPath(),
            DEFAULT_ACK_DEADLINE_SECONDS);
  }

  private void tearDown() throws IOException {
    if (pubsub == null) {
      return;
    }

    try {
      if (subscriptionPath != null) {
        pubsub.deleteSubscription(subscriptionPath);
      }
      if (eventsTopicPath != null) {
        pubsub.deleteTopic(eventsTopicPath);
      }
    } finally {
      pubsub.close();
      pubsub = null;
      eventsTopicPath = null;
      subscriptionPath = null;
    }
  }

  /**
   * Generates randomized topic name.
   *
   * <p>Example: 'TestClassName-testMethodName-2018-12-11-23-32-333-&lt;random-long&gt;'
   */
  static String createTopicName(Description description, String name) throws IOException {
    StringBuilder topicName = new StringBuilder(TOPIC_PREFIX);

    if (description.getClassName() != null) {
      try {
        topicName.append(Class.forName(description.getClassName()).getSimpleName()).append("-");
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }

    if (description.getMethodName() != null) {
      // Remove braces (which are illegal in pubsub naming restrictions) in dynamic method names
      // when using parameterized tests.
      topicName.append(description.getMethodName().replaceAll("[\\[\\]]", "")).append("-");
    }

    DATETIME_FORMAT.printTo(topicName, Instant.now());

    return topicName.toString()
        + "-"
        + name
        + "-"
        + String.valueOf(ThreadLocalRandom.current().nextLong());
  }

  /** Topic path where events will be published to. */
  public TopicPath topicPath() {
    return eventsTopicPath;
  }

  /** Subscription path used to listen for messages on {@link #topicPath()}. */
  public SubscriptionPath subscriptionPath() {
    return subscriptionPath;
  }

  private List<SubscriptionPath> listSubscriptions(ProjectPath projectPath, TopicPath topicPath)
      throws IOException {
    return pubsub.listSubscriptions(projectPath, topicPath).stream()
        .filter((path) -> !path.equals(subscriptionPath))
        .collect(ImmutableList.toImmutableList());
  }

  /** Publish messages to {@link #topicPath()}. */
  public void publish(List<PubsubMessage> messages) throws IOException {
    List<PubsubClient.OutgoingMessage> outgoingMessages =
        messages.stream().map(this::toOutgoingMessage).collect(toList());
    pubsub.publish(eventsTopicPath, outgoingMessages);
  }

  /** Pull up to 100 messages from {@link #subscriptionPath()}. */
  public List<PubsubMessage> pull() throws IOException {
    return pull(100);
  }

  /** Pull up to {@code maxBatchSize} messages from {@link #subscriptionPath()}. */
  public List<PubsubMessage> pull(int maxBatchSize) throws IOException {
    List<PubsubClient.IncomingMessage> messages =
        pubsub.pull(0, subscriptionPath, maxBatchSize, true);
    if (!messages.isEmpty()) {
      pubsub.acknowledge(
          subscriptionPath,
          messages.stream().map(IncomingMessage::ackId).collect(ImmutableList.toImmutableList()));
    }

    return messages.stream()
        .map(
            msg ->
                new PubsubMessage(
                    msg.message().getData().toByteArray(),
                    msg.message().getAttributesMap(),
                    msg.recordId()))
        .collect(ImmutableList.toImmutableList());
  }

  /**
   * Repeatedly pull messages from {@link #subscriptionPath()}, returns after receiving {@code n}
   * messages or after waiting for {@code timeoutDuration}.
   */
  public List<PubsubMessage> waitForNMessages(int n, Duration timeoutDuration)
      throws IOException, InterruptedException {
    List<PubsubMessage> receivedMessages = new ArrayList<>(n);

    DateTime startTime = new DateTime();
    int timeoutSeconds = timeoutDuration.toStandardSeconds().getSeconds();

    receivedMessages.addAll(pull(n - receivedMessages.size()));

    while (receivedMessages.size() < n
        && Seconds.secondsBetween(startTime, new DateTime()).getSeconds() < timeoutSeconds) {
      Thread.sleep(1000);
      receivedMessages.addAll(pull(n - receivedMessages.size()));
    }

    return receivedMessages;
  }

  /**
   * Repeatedly pull messages from {@link #subscriptionPath()} until receiving one for each matcher
   * (or timeout is reached), then assert that the received messages match the expectations.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * testTopic
   *   .assertThatTopicEventuallyReceives(
   *     hasProperty("payload", equalTo("hello".getBytes(StandardCharsets.US_ASCII))),
   *     hasProperty("payload", equalTo("world".getBytes(StandardCharsets.US_ASCII))))
   *   .waitForUpTo(Duration.standardSeconds(20));
   * </pre>
   *
   */
  public PollingAssertion assertThatTopicEventuallyReceives(Matcher<PubsubMessage>... matchers) {
    return timeoutDuration ->
        assertThat(
            waitForNMessages(matchers.length, timeoutDuration), containsInAnyOrder(matchers));
  }

  public interface PollingAssertion {
    void waitForUpTo(Duration timeoutDuration) throws IOException, InterruptedException;
  }

  /**
   * Check if topics exist.
   *
   * @param project GCP project identifier.
   * @param timeoutDuration Joda duration that sets a period of time before checking times out.
   */
  public void checkIfAnySubscriptionExists(String project, Duration timeoutDuration)
      throws InterruptedException, IllegalArgumentException, IOException, TimeoutException {
    if (timeoutDuration.getMillis() <= 0) {
      throw new IllegalArgumentException(String.format("timeoutDuration should be greater than 0"));
    }

    DateTime startTime = new DateTime();
    int sizeOfSubscriptionList = 0;
    while (sizeOfSubscriptionList == 0
        && Seconds.secondsBetween(new DateTime(), startTime).getSeconds()
            < timeoutDuration.toStandardSeconds().getSeconds()) {
      // Sleep 1 sec
      Thread.sleep(1000);
      sizeOfSubscriptionList =
          listSubscriptions(projectPathFromPath(String.format("projects/%s", project)), topicPath())
              .size();
    }

    if (sizeOfSubscriptionList > 0) {
      return;
    } else {
      throw new TimeoutException("Timed out when checking if topics exist for " + topicPath());
    }
  }

  private PubsubClient.OutgoingMessage toOutgoingMessage(PubsubMessage message) {
    return PubsubClient.OutgoingMessage.of(
        com.google.pubsub.v1.PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(message.getPayload()))
            .putAllAttributes(message.getAttributeMap())
            .build(),
        DateTime.now().getMillis(),
        null);
  }
}

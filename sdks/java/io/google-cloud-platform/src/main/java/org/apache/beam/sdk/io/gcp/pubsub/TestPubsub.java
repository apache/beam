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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PushConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams;
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
  private static final Integer DEFAULT_ACK_DEADLINE_SECONDS = 60;

  private final TestPubsubOptions pipelineOptions;
  private final String pubsubEndpoint;
  private final boolean isLocalhost;

  private @Nullable TopicAdminClient topicAdmin = null;
  private @Nullable SubscriptionAdminClient subscriptionAdmin = null;
  private @Nullable TopicPath eventsTopicPath = null;
  private @Nullable SubscriptionPath subscriptionPath = null;
  private @Nullable ManagedChannel channel = null;
  private @Nullable TransportChannelProvider channelProvider = null;

  /**
   * Creates an instance of this rule using options provided by {@link
   * TestPipeline#testingPipelineOptions()}.
   *
   * <p>Loads GCP configuration from {@link TestPipelineOptions}.
   */
  public static TestPubsub create() {
    return fromOptions(TestPipeline.testingPipelineOptions());
  }

  /**
   * Creates an instance of this rule using provided options.
   *
   * <p>Loads GCP configuration from {@link TestPipelineOptions}.
   */
  public static TestPubsub fromOptions(PipelineOptions options) {
    return new TestPubsub(options.as(TestPubsubOptions.class));
  }

  private TestPubsub(TestPubsubOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
    this.pubsubEndpoint = PubsubOptions.targetForRootUrl(this.pipelineOptions.getPubsubRootUrl());
    this.isLocalhost = this.pubsubEndpoint.startsWith("localhost");
  }

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        if (TestPubsub.this.topicAdmin != null || TestPubsub.this.subscriptionAdmin != null) {
          throw new AssertionError(
              "Pubsub client was not shutdown after previous test. "
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
    if (isLocalhost) {
      channel = ManagedChannelBuilder.forTarget(pubsubEndpoint).usePlaintext().build();
    } else {
      channel = ManagedChannelBuilder.forTarget(pubsubEndpoint).useTransportSecurity().build();
    }
    TransportChannelProvider chanProvider =
        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    channelProvider = chanProvider;
    TopicAdminClient topAdmin =
        TopicAdminClient.create(
            TopicAdminSettings.newBuilder()
                .setCredentialsProvider(pipelineOptions::getGcpCredential)
                .setTransportChannelProvider(chanProvider)
                .setEndpoint(pubsubEndpoint)
                .build());
    topicAdmin = topAdmin;
    SubscriptionAdminClient subAdmin =
        SubscriptionAdminClient.create(
            SubscriptionAdminSettings.newBuilder()
                .setCredentialsProvider(pipelineOptions::getGcpCredential)
                .setTransportChannelProvider(chanProvider)
                .setEndpoint(pubsubEndpoint)
                .build());
    subscriptionAdmin = subAdmin;
    TopicPath eventsTopicPathTmp =
        PubsubClient.topicPathFromName(
            pipelineOptions.getProject(), createTopicName(description, EVENTS_TOPIC_NAME));
    topAdmin.createTopic(eventsTopicPathTmp.getPath());

    // Set this after successful creation; it signals that the topic needs teardown
    eventsTopicPath = eventsTopicPathTmp;

    String subscriptionName =
        topicPath().getName() + "_beam_" + ThreadLocalRandom.current().nextLong();
    SubscriptionPath subscriptionPathTmp =
        new SubscriptionPath(
            String.format(
                "projects/%s/subscriptions/%s", pipelineOptions.getProject(), subscriptionName));

    subAdmin.createSubscription(
        subscriptionPathTmp.getPath(),
        topicPath().getPath(),
        PushConfig.getDefaultInstance(),
        DEFAULT_ACK_DEADLINE_SECONDS);

    subscriptionPath = subscriptionPathTmp;
  }

  private void tearDown() {
    SubscriptionAdminClient subAdmin = subscriptionAdmin;
    TopicAdminClient topAdmin = topicAdmin;
    ManagedChannel chan = channel;
    if (subAdmin == null || topAdmin == null || chan == null) {
      return;
    }

    try {
      SubscriptionPath subPath = subscriptionPath;
      if (subPath != null) {
        subAdmin.deleteSubscription(subPath.getPath());
      }
      TopicPath topPath = eventsTopicPath;
      if (topPath != null) {
        for (String subscriptionPath :
            topAdmin.listTopicSubscriptions(topPath.getPath()).iterateAll()) {
          subAdmin.deleteSubscription(subscriptionPath);
        }
        topAdmin.deleteTopic(topPath.getPath());
      }
    } finally {
      subAdmin.close();
      topAdmin.close();
      chan.shutdown();

      subscriptionAdmin = null;
      topicAdmin = null;
      channelProvider = null;
      channel = null;

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
    return Preconditions.checkNotNull(eventsTopicPath, "eventsTopicPath is not initialized");
  }

  /** Subscription path used to listen for messages on {@link #topicPath()}. */
  public SubscriptionPath subscriptionPath() {
    return Preconditions.checkNotNull(subscriptionPath, "subscriptionPath is not initialized");
  }

  private List<String> listSubscriptions(TopicPath topicPath) {
    TopicAdminClient topAdmin =
        Preconditions.checkNotNull(topicAdmin, "topicAdmin is not initialized");
    SubscriptionPath subPath =
        Preconditions.checkNotNull(subscriptionPath, "subscriptionPath is not initialized");
    // Exclude subscriptionPath, the subscription that we created
    return Streams.stream(topAdmin.listTopicSubscriptions(topicPath.getPath()).iterateAll())
        .filter((path) -> !path.equals(subPath.getPath()))
        .collect(Collectors.toList());
  }

  /** Publish messages to {@link #topicPath()}. */
  public void publish(List<PubsubMessage> messages) {
    TopicPath topPath =
        Preconditions.checkNotNull(eventsTopicPath, "eventsTopicPath is not initialized");
    TransportChannelProvider chanProvider =
        Preconditions.checkNotNull(channelProvider, "channelProvider is not initialized");
    Publisher eventPublisher;
    try {
      eventPublisher =
          Publisher.newBuilder(topPath.getPath())
              .setCredentialsProvider(pipelineOptions::getGcpCredential)
              .setChannelProvider(chanProvider)
              .setEndpoint(pubsubEndpoint)
              .build();
    } catch (IOException e) {
      throw new RuntimeException("Error creating event publisher", e);
    }

    List<ApiFuture<String>> futures =
        messages.stream()
            .map(
                (message) -> {
                  com.google.pubsub.v1.PubsubMessage.Builder builder =
                      com.google.pubsub.v1.PubsubMessage.newBuilder()
                          .setData(ByteString.copyFrom(message.getPayload()));
                  Map<String, String> attributeMap = message.getAttributeMap();
                  if (attributeMap != null) {
                    builder.putAllAttributes(attributeMap);
                  }
                  return eventPublisher.publish(builder.build());
                })
            .collect(Collectors.toList());

    try {
      ApiFutures.allAsList(futures).get();
    } catch (ExecutionException e) {
      throw new RuntimeException("Error publishing a test message", e);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted while waiting for messages to publish", e);
    }

    eventPublisher.shutdown();
  }

  /**
   * Repeatedly pull messages from {@link #subscriptionPath()}, returns after receiving {@code n}
   * messages or after waiting for {@code timeoutDuration}.
   */
  public List<PubsubMessage> waitForNMessages(int n, Duration timeoutDuration)
      throws IOException, InterruptedException {
    SubscriptionPath subPath =
        Preconditions.checkNotNull(subscriptionPath, "subscriptionPath is not initialized");
    TransportChannelProvider chanProvider =
        Preconditions.checkNotNull(channelProvider, "channelProvider is not initialized");

    BlockingQueue<com.google.pubsub.v1.PubsubMessage> receivedMessages =
        new LinkedBlockingDeque<>(n);

    MessageReceiver receiver =
        (com.google.pubsub.v1.PubsubMessage message, AckReplyConsumer replyConsumer) -> {
          if (receivedMessages.offer(message)) {
            replyConsumer.ack();
          } else {
            replyConsumer.nack();
          }
        };

    Subscriber subscriber =
        Subscriber.newBuilder(subPath.getPath(), receiver)
            .setCredentialsProvider(pipelineOptions::getGcpCredential)
            .setChannelProvider(chanProvider)
            .setEndpoint(pubsubEndpoint)
            .build();
    subscriber.startAsync();

    DateTime startTime = new DateTime();
    int timeoutSeconds = timeoutDuration.toStandardSeconds().getSeconds();
    while (receivedMessages.size() < n
        && Seconds.secondsBetween(startTime, new DateTime()).getSeconds() < timeoutSeconds) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ignored) {
        // Ignore interrupt and continue polling.
      }
    }

    subscriber.stopAsync();
    subscriber.awaitTerminated();

    return receivedMessages.stream()
        .map(
            (message) ->
                new PubsubMessage(
                    message.getData().toByteArray(),
                    message.getAttributesMap(),
                    message.getMessageId()))
        .collect(Collectors.toList());
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
   * }</pre>
   *
   * @param matchers Matchers to assert on the received messages.
   */
  @SafeVarargs
  public final PollingAssertion assertThatTopicEventuallyReceives(
      Matcher<PubsubMessage>... matchers) {
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
   * @deprecated Use {@link #assertSubscriptionEventuallyCreated}.
   */
  @Deprecated
  public void checkIfAnySubscriptionExists(String project, Duration timeoutDuration)
      throws InterruptedException, IllegalArgumentException, IOException, TimeoutException {
    try {
      assertSubscriptionEventuallyCreated(project, timeoutDuration);
    } catch (AssertionError e) {
      throw new TimeoutException(e.getMessage());
    }
  }

  /**
   * Block until a subscription is created for this test topic in the specified project. Throws
   * {@link AssertionError} if {@code timeoutDuration} is reached before a subscription is created.
   *
   * @param project GCP project identifier.
   * @param timeoutDuration Joda duration before timeout occurs.
   */
  public void assertSubscriptionEventuallyCreated(String project, Duration timeoutDuration)
      throws InterruptedException, IllegalArgumentException, IOException {
    if (timeoutDuration.getMillis() <= 0) {
      throw new IllegalArgumentException(String.format("timeoutDuration should be greater than 0"));
    }

    DateTime startTime = new DateTime();
    int sizeOfSubscriptionList = 0;
    while (sizeOfSubscriptionList == 0
        && Seconds.secondsBetween(startTime, new DateTime()).getSeconds()
            < timeoutDuration.toStandardSeconds().getSeconds()) {
      // Sleep 1 sec
      Thread.sleep(1000);
      sizeOfSubscriptionList = Iterables.size(listSubscriptions(topicPath()));
    }

    if (sizeOfSubscriptionList > 0) {
      return;
    } else {
      throw new AssertionError("Timed out before subscription created for " + topicPath());
    }
  }
}

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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auth.Credentials;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.GetSubscriptionRequest;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A helper class for talking to Pubsub via grpc. */
public class PubsubGrpcClient extends PubsubClient {
  private static final int LIST_BATCH_SIZE = 1000;

  private static final int DEFAULT_TIMEOUT_S = 60;

  private static ManagedChannel channelForRootUrl(String urlString) throws IOException {
    URL url;
    try {
      url = new URL(urlString);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(
          String.format("Could not parse pubsub root url \"%s\"", urlString), e);
    }

    int port = url.getPort();

    if (port < 0) {
      switch (url.getProtocol()) {
        case "https":
          port = 443;
          break;
        case "http":
          port = 80;
          break;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "Could not determine port for pubsub root url \"%s\". You must either specify the port or use the protocol \"https\" or \"http\"",
                  urlString));
      }
    }

    return NettyChannelBuilder.forAddress(url.getHost(), port)
        .negotiationType(NegotiationType.TLS)
        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
        .build();
  }

  private static class PubsubGrpcClientFactory implements PubsubClientFactory {
    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
        throws IOException {
      return new PubsubGrpcClient(
          timestampAttribute,
          idAttribute,
          DEFAULT_TIMEOUT_S,
          channelForRootUrl(options.getPubsubRootUrl()),
          options.getGcpCredential());
    }

    @Override
    public String getKind() {
      return "Grpc";
    }
  }

  /** Factory for creating Pubsub clients using gRCP transport. */
  public static final PubsubClientFactory FACTORY = new PubsubGrpcClientFactory();

  /** Timeout for grpc calls (in s). */
  private final int timeoutSec;

  /** Underlying netty channel, or {@literal null} if closed. */
  private @Nullable ManagedChannel publisherChannel;

  /** Credentials determined from options and environment. */
  private final Credentials credentials;

  /**
   * Attribute to use for custom timestamps, or {@literal null} if should use Pubsub publish time
   * instead.
   */
  private final @Nullable String timestampAttribute;

  /** Attribute to use for custom ids, or {@literal null} if should use Pubsub provided ids. */
  private final @Nullable String idAttribute;

  /** Cached stubs, or null if not cached. */
  private PublisherGrpc.@Nullable PublisherBlockingStub cachedPublisherStub;

  private SubscriberGrpc.SubscriberBlockingStub cachedSubscriberStub;

  @VisibleForTesting
  PubsubGrpcClient(
      @Nullable String timestampAttribute,
      @Nullable String idAttribute,
      int timeoutSec,
      ManagedChannel publisherChannel,
      Credentials credentials) {
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.timeoutSec = timeoutSec;
    this.publisherChannel = publisherChannel;
    this.credentials = credentials;
  }

  /** Gracefully close the underlying netty channel. */
  @Override
  public void close() {
    if (publisherChannel == null) {
      // Already closed.
      return;
    }
    // Can gc the underlying stubs.
    cachedPublisherStub = null;
    cachedSubscriberStub = null;
    // Mark the client as having been closed before going further
    // in case we have an exception from the channel.
    ManagedChannel publisherChannel = this.publisherChannel;
    this.publisherChannel = null;
    // Gracefully shutdown the channel.
    publisherChannel.shutdown();
    try {
      publisherChannel.awaitTermination(timeoutSec, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignore.
      Thread.currentThread().interrupt();
    }
  }

  /** Return channel with interceptor for returning credentials. */
  private Channel newChannel() throws IOException {
    checkState(publisherChannel != null, "PubsubGrpcClient has been closed");
    ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(credentials, Executors.newSingleThreadExecutor());
    return ClientInterceptors.intercept(publisherChannel, interceptor);
  }

  /** Return a stub for making a publish request with a timeout. */
  private PublisherBlockingStub publisherStub() throws IOException {
    if (cachedPublisherStub == null) {
      cachedPublisherStub = PublisherGrpc.newBlockingStub(newChannel());
    }
    return cachedPublisherStub.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS);
  }

  /** Return a stub for making a subscribe request with a timeout. */
  private SubscriberBlockingStub subscriberStub() throws IOException {
    if (cachedSubscriberStub == null) {
      cachedSubscriberStub = SubscriberGrpc.newBlockingStub(newChannel());
    }
    return cachedSubscriberStub.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS);
  }

  @Override
  public int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    PublishRequest.Builder request = PublishRequest.newBuilder().setTopic(topic.getPath());
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      PubsubMessage.Builder message = outgoingMessage.message().toBuilder();

      if (timestampAttribute != null) {
        message.putAttributes(
            timestampAttribute, String.valueOf(outgoingMessage.timestampMsSinceEpoch()));
      }

      if (idAttribute != null && !Strings.isNullOrEmpty(outgoingMessage.recordId())) {
        message.putAttributes(idAttribute, outgoingMessage.recordId());
      }

      request.addMessages(message);
    }

    PublishResponse response = publisherStub().publish(request.build());
    return response.getMessageIdsCount();
  }

  @Override
  public List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch,
      SubscriptionPath subscription,
      int batchSize,
      boolean returnImmediately)
      throws IOException {
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(subscription.getPath())
            .setReturnImmediately(returnImmediately)
            .setMaxMessages(batchSize)
            .build();
    PullResponse response = subscriberStub().pull(request);
    if (response.getReceivedMessagesCount() == 0) {
      return ImmutableList.of();
    }
    List<IncomingMessage> incomingMessages = new ArrayList<>(response.getReceivedMessagesCount());
    for (ReceivedMessage message : response.getReceivedMessagesList()) {
      PubsubMessage pubsubMessage = message.getMessage();
      @Nullable Map<String, String> attributes = pubsubMessage.getAttributes();

      // Timestamp.
      String pubsubTimestampString = null;
      Timestamp timestampProto = pubsubMessage.getPublishTime();
      if (timestampProto != null) {
        pubsubTimestampString =
            String.valueOf(timestampProto.getSeconds() + timestampProto.getNanos() / 1000L);
      }
      long timestampMsSinceEpoch =
          extractTimestamp(timestampAttribute, pubsubTimestampString, attributes);

      // Ack id.
      String ackId = message.getAckId();
      checkState(!Strings.isNullOrEmpty(ackId));

      // Record id, if any.
      @Nullable String recordId = null;
      if (idAttribute != null && attributes != null) {
        recordId = attributes.get(idAttribute);
      }
      if (Strings.isNullOrEmpty(recordId)) {
        // Fall back to the Pubsub provided message id.
        recordId = pubsubMessage.getMessageId();
      }

      incomingMessages.add(
          IncomingMessage.of(
              pubsubMessage, timestampMsSinceEpoch, requestTimeMsSinceEpoch, ackId, recordId));
    }
    return incomingMessages;
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, List<String> ackIds) throws IOException {
    AcknowledgeRequest request =
        AcknowledgeRequest.newBuilder()
            .setSubscription(subscription.getPath())
            .addAllAckIds(ackIds)
            .build();
    subscriberStub().acknowledge(request); // ignore Empty result.
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    ModifyAckDeadlineRequest request =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(subscription.getPath())
            .addAllAckIds(ackIds)
            .setAckDeadlineSeconds(deadlineSeconds)
            .build();
    subscriberStub().modifyAckDeadline(request); // ignore Empty result.
  }

  @Override
  public void createTopic(TopicPath topic) throws IOException {
    Topic request = Topic.newBuilder().setName(topic.getPath()).build();
    publisherStub().createTopic(request); // ignore Topic result.
  }

  @Override
  public void deleteTopic(TopicPath topic) throws IOException {
    DeleteTopicRequest request = DeleteTopicRequest.newBuilder().setTopic(topic.getPath()).build();
    publisherStub().deleteTopic(request); // ignore Empty result.
  }

  @Override
  public List<TopicPath> listTopics(ProjectPath project) throws IOException {
    ListTopicsRequest.Builder request =
        ListTopicsRequest.newBuilder().setProject(project.getPath()).setPageSize(LIST_BATCH_SIZE);
    ListTopicsResponse response = publisherStub().listTopics(request.build());
    if (response.getTopicsCount() == 0) {
      return ImmutableList.of();
    }
    List<TopicPath> topics = new ArrayList<>(response.getTopicsCount());
    while (true) {
      for (Topic topic : response.getTopicsList()) {
        topics.add(topicPathFromPath(topic.getName()));
      }
      if (response.getNextPageToken().isEmpty()) {
        break;
      }
      request.setPageToken(response.getNextPageToken());
      response = publisherStub().listTopics(request.build());
    }
    return topics;
  }

  @Override
  public void createSubscription(
      TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds) throws IOException {
    Subscription request =
        Subscription.newBuilder()
            .setTopic(topic.getPath())
            .setName(subscription.getPath())
            .setAckDeadlineSeconds(ackDeadlineSeconds)
            .build();
    subscriberStub().createSubscription(request); // ignore Subscription result.
  }

  @Override
  public void deleteSubscription(SubscriptionPath subscription) throws IOException {
    DeleteSubscriptionRequest request =
        DeleteSubscriptionRequest.newBuilder().setSubscription(subscription.getPath()).build();
    subscriberStub().deleteSubscription(request); // ignore Empty result.
  }

  @Override
  public List<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException {
    ListSubscriptionsRequest.Builder request =
        ListSubscriptionsRequest.newBuilder()
            .setProject(project.getPath())
            .setPageSize(LIST_BATCH_SIZE);
    ListSubscriptionsResponse response = subscriberStub().listSubscriptions(request.build());
    if (response.getSubscriptionsCount() == 0) {
      return ImmutableList.of();
    }
    List<SubscriptionPath> subscriptions = new ArrayList<>(response.getSubscriptionsCount());
    while (true) {
      for (Subscription subscription : response.getSubscriptionsList()) {
        if (subscription.getTopic().equals(topic.getPath())) {
          subscriptions.add(subscriptionPathFromPath(subscription.getName()));
        }
      }
      if (response.getNextPageToken().isEmpty()) {
        break;
      }
      request.setPageToken(response.getNextPageToken());
      response = subscriberStub().listSubscriptions(request.build());
    }
    return subscriptions;
  }

  @Override
  public int ackDeadlineSeconds(SubscriptionPath subscription) throws IOException {
    GetSubscriptionRequest request =
        GetSubscriptionRequest.newBuilder().setSubscription(subscription.getPath()).build();
    Subscription response = subscriberStub().getSubscription(request);
    return response.getAckDeadlineSeconds();
  }

  @Override
  public boolean isEOF() {
    return false;
  }
}

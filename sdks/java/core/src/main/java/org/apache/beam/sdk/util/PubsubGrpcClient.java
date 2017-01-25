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

import static com.google.common.base.Preconditions.checkState;

import com.google.auth.Credentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PubsubOptions;

/**
 * A helper class for talking to Pubsub via grpc.
 *
 * <p>CAUTION: Currently uses the application default credentials and does not respect any
 * credentials-related arguments in {@link GcpOptions}.
 */
public class PubsubGrpcClient extends PubsubClient {
  private static final String PUBSUB_ADDRESS = "pubsub.googleapis.com";
  private static final int PUBSUB_PORT = 443;
  private static final int LIST_BATCH_SIZE = 1000;

  private static final int DEFAULT_TIMEOUT_S = 15;

  private static class PubsubGrpcClientFactory implements PubsubClientFactory {
    @Override
    public PubsubClient newClient(
        @Nullable String timestampLabel, @Nullable String idLabel, PubsubOptions options)
        throws IOException {
      ManagedChannel channel = NettyChannelBuilder
          .forAddress(PUBSUB_ADDRESS, PUBSUB_PORT)
          .negotiationType(NegotiationType.TLS)
          .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
          .build();

      return new PubsubGrpcClient(timestampLabel,
                                  idLabel,
                                  DEFAULT_TIMEOUT_S,
                                  channel,
                                  options.getGcpCredential());
    }

    @Override
    public String getKind() {
      return "Grpc";
    }
  }

  /**
   * Factory for creating Pubsub clients using gRCP transport.
   */
  public static final PubsubClientFactory FACTORY = new PubsubGrpcClientFactory();

  /**
   * Timeout for grpc calls (in s).
   */
  private final int timeoutSec;

  /**
   * Underlying netty channel, or {@literal null} if closed.
   */
  @Nullable
  private ManagedChannel publisherChannel;

  /**
   * Credentials determined from options and environment.
   */
  private final Credentials credentials;

  /**
   * Label to use for custom timestamps, or {@literal null} if should use Pubsub publish time
   * instead.
   */
  @Nullable
  private final String timestampLabel;

  /**
   * Label to use for custom ids, or {@literal null} if should use Pubsub provided ids.
   */
  @Nullable
  private final String idLabel;


  /**
   * Cached stubs, or null if not cached.
   */
  @Nullable
  private PublisherGrpc.PublisherBlockingStub cachedPublisherStub;
  private SubscriberGrpc.SubscriberBlockingStub cachedSubscriberStub;

  @VisibleForTesting
  PubsubGrpcClient(
      @Nullable String timestampLabel,
      @Nullable String idLabel,
      int timeoutSec,
      ManagedChannel publisherChannel,
      Credentials credentials) {
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.timeoutSec = timeoutSec;
    this.publisherChannel = publisherChannel;
    this.credentials = credentials;
  }

  /**
   * Gracefully close the underlying netty channel.
   */
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

  /**
   * Return channel with interceptor for returning credentials.
   */
  private Channel newChannel() throws IOException {
    checkState(publisherChannel != null, "PubsubGrpcClient has been closed");
    ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(credentials, Executors.newSingleThreadExecutor());
    return ClientInterceptors.intercept(publisherChannel, interceptor);
  }

  /**
   * Return a stub for making a publish request with a timeout.
   */
  private PublisherBlockingStub publisherStub() throws IOException {
    if (cachedPublisherStub == null) {
      cachedPublisherStub = PublisherGrpc.newBlockingStub(newChannel());
    }
    return cachedPublisherStub.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS);
  }

  /**
   * Return a stub for making a subscribe request with a timeout.
   */
  private SubscriberBlockingStub subscriberStub() throws IOException {
    if (cachedSubscriberStub == null) {
      cachedSubscriberStub = SubscriberGrpc.newBlockingStub(newChannel());
    }
    return cachedSubscriberStub.withDeadlineAfter(timeoutSec, TimeUnit.SECONDS);
  }

  @Override
  public int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages)
      throws IOException {
    PublishRequest.Builder request = PublishRequest.newBuilder()
                                                   .setTopic(topic.getPath());
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      PubsubMessage.Builder message =
          PubsubMessage.newBuilder()
                       .setData(ByteString.copyFrom(outgoingMessage.elementBytes));

      if (outgoingMessage.attributes != null) {
        message.putAllAttributes(outgoingMessage.attributes);
      }

      if (timestampLabel != null) {
        message.getMutableAttributes()
               .put(timestampLabel, String.valueOf(outgoingMessage.timestampMsSinceEpoch));
      }

      if (idLabel != null && !Strings.isNullOrEmpty(outgoingMessage.recordId)) {
        message.getMutableAttributes().put(idLabel, outgoingMessage.recordId);
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
      boolean returnImmediately) throws IOException {
    PullRequest request = PullRequest.newBuilder()
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

      // Payload.
      byte[] elementBytes = pubsubMessage.getData().toByteArray();

      // Timestamp.
      String pubsubTimestampString = null;
      Timestamp timestampProto = pubsubMessage.getPublishTime();
      if (timestampProto != null) {
        pubsubTimestampString = String.valueOf(timestampProto.getSeconds()
                                               + timestampProto.getNanos() / 1000L);
      }
      long timestampMsSinceEpoch =
          extractTimestamp(timestampLabel, pubsubTimestampString, attributes);

      // Ack id.
      String ackId = message.getAckId();
      checkState(!Strings.isNullOrEmpty(ackId));

      // Record id, if any.
      @Nullable String recordId = null;
      if (idLabel != null && attributes != null) {
        recordId = attributes.get(idLabel);
      }
      if (Strings.isNullOrEmpty(recordId)) {
        // Fall back to the Pubsub provided message id.
        recordId = pubsubMessage.getMessageId();
      }

      incomingMessages.add(new IncomingMessage(elementBytes, attributes, timestampMsSinceEpoch,
                                               requestTimeMsSinceEpoch, ackId, recordId));
    }
    return incomingMessages;
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, List<String> ackIds)
      throws IOException {
    AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
                                                   .setSubscription(subscription.getPath())
                                                   .addAllAckIds(ackIds)
                                                   .build();
    subscriberStub().acknowledge(request); // ignore Empty result.
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds)
      throws IOException {
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
    Topic request = Topic.newBuilder()
                         .setName(topic.getPath())
                         .build();
    publisherStub().createTopic(request); // ignore Topic result.
  }

  @Override
  public void deleteTopic(TopicPath topic) throws IOException {
    DeleteTopicRequest request = DeleteTopicRequest.newBuilder()
                                                   .setTopic(topic.getPath())
                                                   .build();
    publisherStub().deleteTopic(request); // ignore Empty result.
  }

  @Override
  public List<TopicPath> listTopics(ProjectPath project) throws IOException {
    ListTopicsRequest.Builder request =
        ListTopicsRequest.newBuilder()
                         .setProject(project.getPath())
                         .setPageSize(LIST_BATCH_SIZE);
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
      TopicPath topic, SubscriptionPath subscription,
      int ackDeadlineSeconds) throws IOException {
    Subscription request = Subscription.newBuilder()
                                       .setTopic(topic.getPath())
                                       .setName(subscription.getPath())
                                       .setAckDeadlineSeconds(ackDeadlineSeconds)
                                       .build();
    subscriberStub().createSubscription(request); // ignore Subscription result.
  }

  @Override
  public void deleteSubscription(SubscriptionPath subscription) throws IOException {
    DeleteSubscriptionRequest request =
        DeleteSubscriptionRequest.newBuilder()
                                 .setSubscription(subscription.getPath())
                                 .build();
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
        GetSubscriptionRequest.newBuilder()
                              .setSubscription(subscription.getPath())
                              .build();
    Subscription response = subscriberStub().getSubscription(request);
    return response.getAckDeadlineSeconds();
  }

  @Override
  public boolean isEOF() {
    return false;
  }
}

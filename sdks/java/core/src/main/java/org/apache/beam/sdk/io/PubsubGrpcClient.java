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

package org.apache.beam.sdk.io;

import org.apache.beam.sdk.options.GcpOptions;

import com.google.api.client.util.DateTime;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.ListSubscriptionsRequest;
import com.google.pubsub.v1.ListSubscriptionsResponse;
import com.google.pubsub.v1.ListTopicsRequest;
import com.google.pubsub.v1.ListTopicsResponse;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriberGrpc;
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

/**
 * A helper class for talking to Pubsub via grpc.
 */
public class PubsubGrpcClient implements PubsubClient {
  private static final String PUBSUB_ADDRESS = "pubsub.googleapis.com";
  private static final int PUBSUB_PORT = 443;
  private static final List<String> PUBSUB_SCOPES =
      Collections.singletonList("https://www.googleapis.com/auth/pubsub");
  private static final int LIST_BATCH_SIZE = 1000;

  /**
   * Timeout for grpc calls (in s).
   */
  private static final int TIMEOUT_S = 15;

  /**
   * Underlying netty channel, or {@literal null} if closed.
   */
  @Nullable
  private ManagedChannel publisherChannel;

  /**
   * Credentials determined from options and environment.
   */
  private final GoogleCredentials credentials;

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

  private PubsubGrpcClient(
      @Nullable String timestampLabel, @Nullable String idLabel,
      ManagedChannel publisherChannel, GoogleCredentials credentials) {
    this.timestampLabel = timestampLabel;
    this.idLabel = idLabel;
    this.publisherChannel = publisherChannel;
    this.credentials = credentials;
  }

  /**
   * Construct a new Pubsub grpc client. It should be closed via {@link #close} in order
   * to ensure tidy cleanup of underlying netty resources. (Or use the try-with-resources
   * construct since this class is {@link AutoCloseable}). If non-{@literal null}, use
   * {@code timestampLabel} and {@code idLabel} to store custom timestamps/ids within
   * message metadata.
   */
  public static PubsubGrpcClient newClient(
      @Nullable String timestampLabel, @Nullable String idLabel,
      GcpOptions options) throws IOException {
    ManagedChannel channel = NettyChannelBuilder
        .forAddress(PUBSUB_ADDRESS, PUBSUB_PORT)
        .negotiationType(NegotiationType.TLS)
        .sslContext(GrpcSslContexts.forClient().ciphers(null).build())
        .build();
    // TODO: GcpOptions needs to support building com.google.auth.oauth2.Credentials from the
    // various command line options. It currently only supports the older
    // com.google.api.client.auth.oauth2.Credentials.
    GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    return new PubsubGrpcClient(timestampLabel, idLabel, channel, credentials);
  }

  /**
   * Gracefully close the underlying netty channel.
   */
  @Override
  public void close() {
    Preconditions.checkState(publisherChannel != null, "Client has already been closed");
    publisherChannel.shutdown();
    try {
      publisherChannel.awaitTermination(TIMEOUT_S, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      // Ignore.
      Thread.currentThread().interrupt();
    }
    publisherChannel = null;
    cachedPublisherStub = null;
    cachedSubscriberStub = null;
  }

  /**
   * Return channel with interceptor for returning credentials.
   */
  private Channel newChannel() throws IOException {
    Preconditions.checkState(publisherChannel != null, "PubsubGrpcClient has been closed");
    ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(credentials, Executors.newSingleThreadExecutor());
    return ClientInterceptors.intercept(publisherChannel, interceptor);
  }

  /**
   * Return a stub for making a publish request with a timeout.
   */
  private PublisherGrpc.PublisherBlockingStub publisherStub() throws IOException {
    if (cachedPublisherStub == null) {
      cachedPublisherStub = PublisherGrpc.newBlockingStub(newChannel());
    }
    return cachedPublisherStub.withDeadlineAfter(TIMEOUT_S, TimeUnit.SECONDS);
  }

  /**
   * Return a stub for making a subscribe request with a timeout.
   */
  private SubscriberGrpc.SubscriberBlockingStub subscriberStub() throws IOException {
    if (cachedSubscriberStub == null) {
      cachedSubscriberStub = SubscriberGrpc.newBlockingStub(newChannel());
    }
    return cachedSubscriberStub.withDeadlineAfter(TIMEOUT_S, TimeUnit.SECONDS);
  }

  @Override
  public int publish(TopicPath topic, Iterable<OutgoingMessage> outgoingMessages)
      throws IOException {
    PublishRequest.Builder request = PublishRequest.newBuilder()
                                                   .setTopic(topic.getPath());
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      PubsubMessage.Builder message =
          PubsubMessage.newBuilder()
                       .setData(ByteString.copyFrom(outgoingMessage.elementBytes));

      if (timestampLabel != null) {
        message.getMutableAttributes()
               .put(timestampLabel, String.valueOf(outgoingMessage.timestampMsSinceEpoch));
      }

      if (idLabel != null) {
        message.getMutableAttributes()
               .put(idLabel,
                   Hashing.murmur3_128().hashBytes(outgoingMessage.elementBytes).toString());
      }

      request.addMessages(message);
    }

    PublishResponse response = publisherStub().publish(request.build());
    return response.getMessageIdsCount();
  }

  @Override
  public Collection<IncomingMessage> pull(
      long requestTimeMsSinceEpoch,
      SubscriptionPath subscription,
      int batchSize) throws IOException {
    PullRequest request = PullRequest.newBuilder()
                                     .setSubscription(subscription.getPath())
                                     .setReturnImmediately(true)
                                     .setMaxMessages(batchSize)
                                     .build();
    PullResponse response = subscriberStub().pull(request);
    if (response.getReceivedMessagesCount() == 0) {
      return ImmutableList.of();
    }
    List<IncomingMessage> incomingMessages = new ArrayList<>(response.getReceivedMessagesCount());
    for (ReceivedMessage message : response.getReceivedMessagesList()) {
      PubsubMessage pubsubMessage = message.getMessage();
      Map<String, String> attributes = pubsubMessage.getAttributes();

      // Payload.
      byte[] elementBytes = pubsubMessage.getData().toByteArray();

      // Timestamp.
      // Start with Pubsub processing time.
      Timestamp timestampProto = pubsubMessage.getPublishTime();
      long timestampMsSinceEpoch = timestampProto.getSeconds() + timestampProto.getNanos() / 1000L;
      if (timestampLabel != null && attributes != null) {
        String timestampString = attributes.get(timestampLabel);
        if (timestampString != null && !timestampString.isEmpty()) {
          try {
            // Try parsing as milliseconds since epoch. Note there is no way to parse a
            // string in RFC 3339 format here.
            // Expected IllegalArgumentException if parsing fails; we use that to fall back
            // to RFC 3339.
            timestampMsSinceEpoch = Long.parseLong(timestampString);
          } catch (IllegalArgumentException e1) {
            try {
              // Try parsing as RFC3339 string. DateTime.parseRfc3339 will throw an
              // IllegalArgumentException if parsing fails, and the caller should handle.
              timestampMsSinceEpoch = DateTime.parseRfc3339(timestampString).getValue();
            } catch (IllegalArgumentException e2) {
              // Fallback to Pubsub processing time.
            }
          }
        }
        // else: fallback to Pubsub processing time.
      }
      // else: fallback to Pubsub processing time.

      // Ack id.
      String ackId = message.getAckId();
      Preconditions.checkState(ackId != null && !ackId.isEmpty());

      // Record id, if any.
      @Nullable byte[] recordId = null;
      if (idLabel != null && attributes != null) {
        String recordIdString = attributes.get(idLabel);
        if (recordIdString != null && !recordIdString.isEmpty()) {
          recordId = recordIdString.getBytes();
        }
      }
      if (recordId == null) {
        recordId = pubsubMessage.getMessageId().getBytes();
      }

      incomingMessages.add(new IncomingMessage(elementBytes, timestampMsSinceEpoch,
          requestTimeMsSinceEpoch, ackId, recordId));
    }
    return incomingMessages;
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, Iterable<String> ackIds)
      throws IOException {
    AcknowledgeRequest request = AcknowledgeRequest.newBuilder()
                                                   .setSubscription(subscription.getPath())
                                                   .addAllAckIds(ackIds)
                                                   .build();
    subscriberStub().acknowledge(request); // ignore Empty result.
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, Iterable<String> ackIds, int
      deadlineSeconds)
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
  public Collection<TopicPath> listTopics(ProjectPath project) throws IOException {
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
        topics.add(new TopicPath(topic.getName()));
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
  public Collection<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
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
          subscriptions.add(new SubscriptionPath(subscription.getName()));
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
}

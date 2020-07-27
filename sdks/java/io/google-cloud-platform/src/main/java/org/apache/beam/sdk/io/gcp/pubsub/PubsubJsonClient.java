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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.Pubsub.Builder;
import com.google.api.services.pubsub.Pubsub.Projects.Subscriptions;
import com.google.api.services.pubsub.Pubsub.Projects.Topics;
import com.google.api.services.pubsub.model.AcknowledgeRequest;
import com.google.api.services.pubsub.model.ListSubscriptionsResponse;
import com.google.api.services.pubsub.model.ListTopicsResponse;
import com.google.api.services.pubsub.model.ModifyAckDeadlineRequest;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PublishResponse;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.PullRequest;
import com.google.api.services.pubsub.model.PullResponse;
import com.google.api.services.pubsub.model.ReceivedMessage;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A Pubsub client using JSON transport. */
public class PubsubJsonClient extends PubsubClient {

  private static class PubsubJsonClientFactory implements PubsubClientFactory {
    private static HttpRequestInitializer chainHttpRequestInitializer(
        Credentials credential, HttpRequestInitializer httpRequestInitializer) {
      if (credential == null) {
        return httpRequestInitializer;
      } else {
        return new ChainingHttpRequestInitializer(
            new HttpCredentialsAdapter(credential), httpRequestInitializer);
      }
    }

    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
        throws IOException {
      Pubsub pubsub =
          new Builder(
                  Transport.getTransport(),
                  Transport.getJsonFactory(),
                  chainHttpRequestInitializer(
                      options.getGcpCredential(),
                      // Do not log 404. It clutters the output and is possibly even required by the
                      // caller.
                      new RetryHttpRequestInitializer(ImmutableList.of(404))))
              .setRootUrl(options.getPubsubRootUrl())
              .setApplicationName(options.getAppName())
              .setGoogleClientRequestInitializer(options.getGoogleApiTrace())
              .build();
      return new PubsubJsonClient(timestampAttribute, idAttribute, pubsub);
    }

    @Override
    public String getKind() {
      return "Json";
    }
  }

  /** Factory for creating Pubsub clients using Json transport. */
  public static final PubsubClientFactory FACTORY = new PubsubJsonClientFactory();

  /**
   * Attribute to use for custom timestamps, or {@literal null} if should use Pubsub publish time
   * instead.
   */
  private final @Nullable String timestampAttribute;

  /** Attribute to use for custom ids, or {@literal null} if should use Pubsub provided ids. */
  private final @Nullable String idAttribute;

  /** Underlying JSON transport. */
  private Pubsub pubsub;

  @VisibleForTesting
  PubsubJsonClient(
      @Nullable String timestampAttribute, @Nullable String idAttribute, Pubsub pubsub) {
    this.timestampAttribute = timestampAttribute;
    this.idAttribute = idAttribute;
    this.pubsub = pubsub;
  }

  @Override
  public void close() {
    // Nothing to close.
  }

  @Override
  public int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages) throws IOException {
    List<PubsubMessage> pubsubMessages = new ArrayList<>(outgoingMessages.size());
    for (OutgoingMessage outgoingMessage : outgoingMessages) {
      PubsubMessage pubsubMessage =
          new PubsubMessage().encodeData(outgoingMessage.message().getData().toByteArray());
      pubsubMessage.setAttributes(getMessageAttributes(outgoingMessage));
      if (!outgoingMessage.message().getOrderingKey().isEmpty()) {
        pubsubMessage.put("orderingKey", outgoingMessage.message().getOrderingKey());
      }
      pubsubMessages.add(pubsubMessage);
    }
    PublishRequest request = new PublishRequest().setMessages(pubsubMessages);
    PublishResponse response =
        pubsub.projects().topics().publish(topic.getPath(), request).execute();
    return response.getMessageIds().size();
  }

  private Map<String, String> getMessageAttributes(OutgoingMessage outgoingMessage) {
    Map<String, String> attributes = null;
    if (outgoingMessage.message().getAttributesMap() == null) {
      attributes = new TreeMap<>();
    } else {
      attributes = new TreeMap<>(outgoingMessage.message().getAttributesMap());
    }
    if (timestampAttribute != null) {
      attributes.put(timestampAttribute, String.valueOf(outgoingMessage.timestampMsSinceEpoch()));
    }
    if (idAttribute != null && !Strings.isNullOrEmpty(outgoingMessage.recordId())) {
      attributes.put(idAttribute, outgoingMessage.recordId());
    }
    return attributes;
  }

  @Override
  public List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch,
      SubscriptionPath subscription,
      int batchSize,
      boolean returnImmediately)
      throws IOException {
    PullRequest request =
        new PullRequest().setReturnImmediately(returnImmediately).setMaxMessages(batchSize);
    PullResponse response =
        pubsub.projects().subscriptions().pull(subscription.getPath(), request).execute();
    if (response.getReceivedMessages() == null || response.getReceivedMessages().isEmpty()) {
      return ImmutableList.of();
    }
    List<IncomingMessage> incomingMessages = new ArrayList<>(response.getReceivedMessages().size());
    for (ReceivedMessage message : response.getReceivedMessages()) {
      PubsubMessage pubsubMessage = message.getMessage();
      Map<String, String> attributes;
      if (pubsubMessage.getAttributes() != null) {
        attributes = pubsubMessage.getAttributes();
      } else {
        attributes = new HashMap<>();
      }

      // Payload.
      byte[] elementBytes = pubsubMessage.getData() == null ? null : pubsubMessage.decodeData();
      if (elementBytes == null) {
        elementBytes = new byte[0];
      }

      // Timestamp.
      long timestampMsSinceEpoch =
          extractTimestamp(timestampAttribute, message.getMessage().getPublishTime(), attributes);

      // Ack id.
      String ackId = message.getAckId();
      checkState(!Strings.isNullOrEmpty(ackId));

      // Record id, if any.
      @Nullable String recordId = null;
      if (idAttribute != null) {
        recordId = attributes.get(idAttribute);
      }
      if (Strings.isNullOrEmpty(recordId)) {
        // Fall back to the Pubsub provided message id.
        recordId = pubsubMessage.getMessageId();
      }

      com.google.pubsub.v1.PubsubMessage.Builder protoMessage =
          com.google.pubsub.v1.PubsubMessage.newBuilder();
      protoMessage.setData(ByteString.copyFrom(elementBytes));
      protoMessage.putAllAttributes(attributes);
      protoMessage.setOrderingKey(
          (String) pubsubMessage.getUnknownKeys().getOrDefault("orderingKey", ""));
      incomingMessages.add(
          IncomingMessage.of(
              protoMessage.build(),
              timestampMsSinceEpoch,
              requestTimeMsSinceEpoch,
              ackId,
              recordId));
    }

    return incomingMessages;
  }

  @Override
  public void acknowledge(SubscriptionPath subscription, List<String> ackIds) throws IOException {
    AcknowledgeRequest request = new AcknowledgeRequest().setAckIds(ackIds);
    pubsub
        .projects()
        .subscriptions()
        .acknowledge(subscription.getPath(), request)
        .execute(); // ignore Empty result.
  }

  @Override
  public void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException {
    ModifyAckDeadlineRequest request =
        new ModifyAckDeadlineRequest().setAckIds(ackIds).setAckDeadlineSeconds(deadlineSeconds);
    pubsub
        .projects()
        .subscriptions()
        .modifyAckDeadline(subscription.getPath(), request)
        .execute(); // ignore Empty result.
  }

  @Override
  public void createTopic(TopicPath topic) throws IOException {
    pubsub
        .projects()
        .topics()
        .create(topic.getPath(), new Topic())
        .execute(); // ignore Topic result.
  }

  @Override
  public void deleteTopic(TopicPath topic) throws IOException {
    pubsub.projects().topics().delete(topic.getPath()).execute(); // ignore Empty result.
  }

  @Override
  public List<TopicPath> listTopics(ProjectPath project) throws IOException {
    Topics.List request = pubsub.projects().topics().list(project.getPath());
    ListTopicsResponse response = request.execute();
    if (response.getTopics() == null || response.getTopics().isEmpty()) {
      return ImmutableList.of();
    }
    List<TopicPath> topics = new ArrayList<>(response.getTopics().size());
    while (true) {
      for (Topic topic : response.getTopics()) {
        topics.add(topicPathFromPath(topic.getName()));
      }
      if (Strings.isNullOrEmpty(response.getNextPageToken())) {
        break;
      }
      request.setPageToken(response.getNextPageToken());
      response = request.execute();
    }
    return topics;
  }

  @Override
  public void createSubscription(
      TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds) throws IOException {
    Subscription request =
        new Subscription().setTopic(topic.getPath()).setAckDeadlineSeconds(ackDeadlineSeconds);
    pubsub
        .projects()
        .subscriptions()
        .create(subscription.getPath(), request)
        .execute(); // ignore Subscription result.
  }

  @Override
  public void deleteSubscription(SubscriptionPath subscription) throws IOException {
    pubsub
        .projects()
        .subscriptions()
        .delete(subscription.getPath())
        .execute(); // ignore Empty result.
  }

  @Override
  public List<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException {
    Subscriptions.List request = pubsub.projects().subscriptions().list(project.getPath());
    ListSubscriptionsResponse response = request.execute();
    if (response.getSubscriptions() == null || response.getSubscriptions().isEmpty()) {
      return ImmutableList.of();
    }
    List<SubscriptionPath> subscriptions = new ArrayList<>(response.getSubscriptions().size());
    while (true) {
      for (Subscription subscription : response.getSubscriptions()) {
        if (subscription.getTopic().equals(topic.getPath())) {
          subscriptions.add(subscriptionPathFromPath(subscription.getName()));
        }
      }
      if (Strings.isNullOrEmpty(response.getNextPageToken())) {
        break;
      }
      request.setPageToken(response.getNextPageToken());
      response = request.execute();
    }
    return subscriptions;
  }

  @Override
  public int ackDeadlineSeconds(SubscriptionPath subscription) throws IOException {
    Subscription response = pubsub.projects().subscriptions().get(subscription.getPath()).execute();
    return response.getAckDeadlineSeconds();
  }

  @Override
  public boolean isEOF() {
    return false;
  }
}

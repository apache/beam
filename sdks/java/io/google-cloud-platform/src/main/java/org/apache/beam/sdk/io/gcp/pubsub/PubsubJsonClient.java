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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.pubsub.Pubsub;
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
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

      return newClient(timestampAttribute, idAttribute, options, null);
    }

    @Override
    public PubsubClient newClient(
        @Nullable String timestampAttribute,
        @Nullable String idAttribute,
        PubsubOptions options,
        @Nullable String rootUrlOverride)
        throws IOException {
      Pubsub pubsub =
          new Pubsub.Builder(
                  Transport.getTransport(),
                  Transport.getJsonFactory(),
                  chainHttpRequestInitializer(
                      options.getGcpCredential(),
                      // Do not log 404. It clutters the output and is possibly even required by the
                      // caller.
                      new RetryHttpRequestInitializer(ImmutableList.of(404))))
              .setRootUrl(MoreObjects.firstNonNull(rootUrlOverride, options.getPubsubRootUrl()))
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
          new PubsubMessage().encodeData(outgoingMessage.getMessage().getData().toByteArray());
      pubsubMessage.setAttributes(getMessageAttributes(outgoingMessage));
      if (!outgoingMessage.getMessage().getOrderingKey().isEmpty()) {
        pubsubMessage.setOrderingKey(outgoingMessage.getMessage().getOrderingKey());
      }

      // N.B. publishTime and messageId are intentionally not set on the message that is published
      pubsubMessages.add(pubsubMessage);
    }
    PublishRequest request = new PublishRequest().setMessages(pubsubMessages);
    PublishResponse response =
        pubsub.projects().topics().publish(topic.getPath(), request).execute();
    return response.getMessageIds().size();
  }

  private Map<String, String> getMessageAttributes(OutgoingMessage outgoingMessage) {
    Map<String, String> attributes = new TreeMap<>(outgoingMessage.getMessage().getAttributesMap());
    if (timestampAttribute != null) {
      attributes.put(
          timestampAttribute, String.valueOf(outgoingMessage.getTimestampMsSinceEpoch()));
    }
    String recordId = outgoingMessage.recordId();
    if (idAttribute != null && recordId != null && !recordId.isEmpty()) {
      attributes.put(idAttribute, recordId);
    }
    return attributes;
  }

  @Override
  @SuppressWarnings("ProtoFieldNullComparison")
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
    List<ReceivedMessage> receivedMessages = response.getReceivedMessages();
    if (receivedMessages == null || receivedMessages.isEmpty()) {
      return ImmutableList.of();
    }
    List<IncomingMessage> incomingMessages = new ArrayList<>(receivedMessages.size());
    for (ReceivedMessage message : receivedMessages) {
      PubsubMessage pubsubMessage = message.getMessage();
      if (pubsubMessage == null) {
        continue;
      }
      Map<String, String> attributes = pubsubMessage.getAttributes();
      if (attributes == null) {
        attributes = new HashMap<>();
      }

      // Payload.
      String dataStr = pubsubMessage.getData();
      byte[] elementBytes = (dataStr == null) ? new byte[0] : pubsubMessage.decodeData();

      // Timestamp.
      long timestampMsSinceEpoch;
      if (timestampAttribute == null || timestampAttribute.isEmpty()) {
        String publishTime = pubsubMessage.getPublishTime();
        if (publishTime == null) {
          throw new IllegalStateException("Received message missing publishTime");
        }
        timestampMsSinceEpoch = parseTimestampAsMsSinceEpoch(publishTime);
      } else {
        timestampMsSinceEpoch = extractTimestampAttribute(timestampAttribute, attributes);
      }

      // Ack id.
      String ackId = message.getAckId();
      if (ackId == null || ackId.isEmpty()) {
        throw new IllegalStateException("Received message missing ackId");
      }

      // Record id, if any.
      @Nullable String recordId = null;
      if (idAttribute != null) {
        recordId = attributes.get(idAttribute);
      }
      if (recordId == null || recordId.isEmpty()) {
        // Fall back to the Pubsub provided message id.
        recordId = checkStateNotNull(pubsubMessage.getMessageId(), "Message ID is missing");
      }

      com.google.pubsub.v1.PubsubMessage.Builder protoMessage =
          com.google.pubsub.v1.PubsubMessage.newBuilder();
      protoMessage.setData(ByteString.copyFrom(elementBytes));
      protoMessage.putAllAttributes(attributes);
      // {@link PubsubMessage} uses `null` or empty string to represent no ordering key.
      // {@link com.google.pubsub.v1.PubsubMessage} does not track string field presence and uses
      // empty string as a default.
      String orderingKey = pubsubMessage.getOrderingKey();
      if (orderingKey != null) {
        protoMessage.setOrderingKey(orderingKey);
      }
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
  public void createTopic(TopicPath topic, SchemaPath schema) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteTopic(TopicPath topic) throws IOException {
    pubsub.projects().topics().delete(topic.getPath()).execute(); // ignore Empty result.
  }

  @Override
  public List<TopicPath> listTopics(ProjectPath project) throws IOException {
    Topics.List request = pubsub.projects().topics().list(project.getPath());
    ListTopicsResponse response = request.execute();
    List<Topic> topicsList = response.getTopics();
    if (topicsList == null || topicsList.isEmpty()) {
      return ImmutableList.of();
    }
    List<TopicPath> topics = new ArrayList<>(topicsList.size());
    while (true) {
      for (Topic topic : topicsList) {
        topics.add(topicPathFromPath(topic.getName()));
      }
      String nextPageToken = response.getNextPageToken();
      if (nextPageToken == null || nextPageToken.isEmpty()) {
        break;
      }
      request.setPageToken(nextPageToken);
      response = request.execute();
      topicsList = response.getTopics();
      if (topicsList == null) {
        break;
      }
    }
    return topics;
  }

  @Override
  public boolean isTopicExists(TopicPath topic) throws IOException {
    try {
      pubsub.projects().topics().get(topic.getPath()).execute();
      return true;
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        return false;
      }
      throw e;
    }
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
    List<Subscription> subscriptionsList = response.getSubscriptions();
    if (subscriptionsList == null || subscriptionsList.isEmpty()) {
      return ImmutableList.of();
    }
    List<SubscriptionPath> subscriptions = new ArrayList<>(subscriptionsList.size());
    while (true) {
      for (Subscription subscription : subscriptionsList) {
        String subTopic = subscription.getTopic();
        if (subTopic != null && subTopic.equals(topic.getPath())) {
          subscriptions.add(subscriptionPathFromPath(subscription.getName()));
        }
      }
      String nextPageToken = response.getNextPageToken();
      if (nextPageToken == null || nextPageToken.isEmpty()) {
        break;
      }
      request.setPageToken(nextPageToken);
      response = request.execute();
      subscriptionsList = response.getSubscriptions();
      if (subscriptionsList == null) {
        break;
      }
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

  /** Create {@link com.google.api.services.pubsub.model.Schema} from Schema definition content. */
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

  /** Return {@link SchemaPath} from {@link TopicPath} if exists. */
  @Override
  public @Nullable SchemaPath getSchemaPath(TopicPath topicPath) throws IOException {
    Topic topic = pubsub.projects().topics().get(topicPath.getPath()).execute();
    com.google.api.services.pubsub.model.SchemaSettings schemaSettings = topic.getSchemaSettings();
    if (schemaSettings == null) {
      return null;
    }
    String schemaPath = schemaSettings.getSchema();
    if (schemaPath == null || schemaPath.equals(SchemaPath.DELETED_SCHEMA_PATH)) {
      return null;
    }
    return PubsubClient.schemaPathFromPath(schemaPath);
  }

  /** Return a Beam {@link Schema} from the Pub/Sub schema resource, if exists. */
  @Override
  public Schema getSchema(SchemaPath schemaPath) throws IOException {
    com.google.api.services.pubsub.model.Schema pubsubSchema =
        pubsub.projects().schemas().get(schemaPath.getPath()).execute();
    return fromPubsubSchema(pubsubSchema);
  }
}

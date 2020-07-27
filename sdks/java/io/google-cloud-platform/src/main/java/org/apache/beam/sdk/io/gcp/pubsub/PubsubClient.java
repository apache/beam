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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.DateTime;
import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An (abstract) helper class for talking to Pubsub via an underlying transport. */
public abstract class PubsubClient implements Closeable {
  /** Factory for creating clients. */
  public interface PubsubClientFactory extends Serializable {
    /**
     * Construct a new Pubsub client. It should be closed via {@link #close} in order to ensure tidy
     * cleanup of underlying netty resources (or use the try-with-resources construct). Uses {@code
     * options} to derive pubsub endpoints and application credentials. If non-{@literal null}, use
     * {@code timestampAttribute} and {@code idAttribute} to store custom timestamps/ids within
     * message metadata.
     */
    PubsubClient newClient(
        @Nullable String timestampAttribute, @Nullable String idAttribute, PubsubOptions options)
        throws IOException;

    /** Return the display name for this factory. Eg "Json", "gRPC". */
    String getKind();
  }

  /**
   * Return timestamp as ms-since-unix-epoch corresponding to {@code timestamp}. Return {@literal
   * null} if no timestamp could be found. Throw {@link IllegalArgumentException} if timestamp
   * cannot be recognized.
   */
  private static @Nullable Long asMsSinceEpoch(@Nullable String timestamp) {
    if (Strings.isNullOrEmpty(timestamp)) {
      return null;
    }
    try {
      // Try parsing as milliseconds since epoch. Note there is no way to parse a
      // string in RFC 3339 format here.
      // Expected IllegalArgumentException if parsing fails; we use that to fall back
      // to RFC 3339.
      return Long.parseLong(timestamp);
    } catch (IllegalArgumentException e1) {
      // Try parsing as RFC3339 string. DateTime.parseRfc3339 will throw an
      // IllegalArgumentException if parsing fails, and the caller should handle.
      return DateTime.parseRfc3339(timestamp).getValue();
    }
  }

  /**
   * Return the timestamp (in ms since unix epoch) to use for a Pubsub message with {@code
   * attributes} and {@code pubsubTimestamp}.
   *
   * <p>If {@code timestampAttribute} is non-{@literal null} then the message attributes must
   * contain that attribute, and the value of that attribute will be taken as the timestamp.
   * Otherwise the timestamp will be taken from the Pubsub publish timestamp {@code
   * pubsubTimestamp}.
   *
   * @throws IllegalArgumentException if the timestamp cannot be recognized as a ms-since-unix-epoch
   *     or RFC3339 time.
   */
  protected static long extractTimestamp(
      @Nullable String timestampAttribute,
      @Nullable String pubsubTimestamp,
      @Nullable Map<String, String> attributes) {
    Long timestampMsSinceEpoch;
    if (Strings.isNullOrEmpty(timestampAttribute)) {
      timestampMsSinceEpoch = asMsSinceEpoch(pubsubTimestamp);
      checkArgument(
          timestampMsSinceEpoch != null,
          "Cannot interpret PubSub publish timestamp: %s",
          pubsubTimestamp);
    } else {
      String value = attributes == null ? null : attributes.get(timestampAttribute);
      checkArgument(
          value != null,
          "PubSub message is missing a value for timestamp attribute %s",
          timestampAttribute);
      timestampMsSinceEpoch = asMsSinceEpoch(value);
      checkArgument(
          timestampMsSinceEpoch != null,
          "Cannot interpret value of attribute %s as timestamp: %s",
          timestampAttribute,
          value);
    }
    return timestampMsSinceEpoch;
  }

  /** Path representing a cloud project id. */
  public static class ProjectPath implements Serializable {
    private final String projectId;

    /**
     * Creates a {@link ProjectPath} from a {@link String} representation, which must be of the form
     * {@code "projects/" + projectId}.
     */
    ProjectPath(String path) {
      List<String> splits = Splitter.on('/').splitToList(path);
      checkArgument(
          splits.size() == 2 && "projects".equals(splits.get(0)),
          "Malformed project path \"%s\": must be of the form \"projects/\" + <project id>",
          path);
      this.projectId = splits.get(1);
    }

    public String getPath() {
      return String.format("projects/%s", projectId);
    }

    public String getId() {
      return projectId;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ProjectPath that = (ProjectPath) o;

      return projectId.equals(that.projectId);
    }

    @Override
    public int hashCode() {
      return projectId.hashCode();
    }

    @Override
    public String toString() {
      return getPath();
    }
  }

  public static ProjectPath projectPathFromPath(String path) {
    return new ProjectPath(path);
  }

  public static ProjectPath projectPathFromId(String projectId) {
    return new ProjectPath(String.format("projects/%s", projectId));
  }

  /** Path representing a Pubsub subscription. */
  public static class SubscriptionPath implements Serializable {
    private final String projectId;
    private final String subscriptionName;

    SubscriptionPath(String path) {
      List<String> splits = Splitter.on('/').splitToList(path);
      checkState(
          splits.size() == 4
              && "projects".equals(splits.get(0))
              && "subscriptions".equals(splits.get(2)),
          "Malformed subscription path %s: "
              + "must be of the form \"projects/\" + <project id> + \"subscriptions\"",
          path);
      this.projectId = splits.get(1);
      this.subscriptionName = splits.get(3);
    }

    public String getPath() {
      return String.format("projects/%s/subscriptions/%s", projectId, subscriptionName);
    }

    public String getName() {
      return subscriptionName;
    }

    public String getV1Beta1Path() {
      return String.format("/subscriptions/%s/%s", projectId, subscriptionName);
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SubscriptionPath that = (SubscriptionPath) o;
      return this.subscriptionName.equals(that.subscriptionName)
          && this.projectId.equals(that.projectId);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(projectId, subscriptionName);
    }

    @Override
    public String toString() {
      return getPath();
    }
  }

  public static SubscriptionPath subscriptionPathFromPath(String path) {
    return new SubscriptionPath(path);
  }

  public static SubscriptionPath subscriptionPathFromName(
      String projectId, String subscriptionName) {
    return new SubscriptionPath(
        String.format("projects/%s/subscriptions/%s", projectId, subscriptionName));
  }

  /** Path representing a Pubsub topic. */
  public static class TopicPath implements Serializable {
    private final String path;

    TopicPath(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }

    public String getName() {
      List<String> splits = Splitter.on('/').splitToList(path);

      checkState(splits.size() == 4, "Malformed topic path %s", path);
      return splits.get(3);
    }

    public String getV1Beta1Path() {
      List<String> splits = Splitter.on('/').splitToList(path);
      checkState(splits.size() == 4, "Malformed topic path %s", path);
      return String.format("/topics/%s/%s", splits.get(1), splits.get(3));
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TopicPath topicPath = (TopicPath) o;
      return path.equals(topicPath.path);
    }

    @Override
    public int hashCode() {
      return path.hashCode();
    }

    @Override
    public String toString() {
      return path;
    }
  }

  public static TopicPath topicPathFromPath(String path) {
    return new TopicPath(path);
  }

  public static TopicPath topicPathFromName(String projectId, String topicName) {
    return new TopicPath(String.format("projects/%s/topics/%s", projectId, topicName));
  }

  /**
   * A message to be sent to Pubsub.
   *
   * <p>NOTE: This class is {@link Serializable} only to support the {@link PubsubTestClient}. Java
   * serialization is never used for non-test clients.
   */
  @AutoValue
  public abstract static class OutgoingMessage implements Serializable {

    /** Underlying Message. May not have publish timestamp set. */
    public abstract PubsubMessage message();

    /** Timestamp for element (ms since epoch). */
    public abstract long timestampMsSinceEpoch();

    /**
     * If using an id attribute, the record id to associate with this record's metadata so the
     * receiver can reject duplicates. Otherwise {@literal null}.
     */
    public abstract @Nullable String recordId();

    public static OutgoingMessage of(
        PubsubMessage message, long timestampMsSinceEpoch, @Nullable String recordId) {
      return new AutoValue_PubsubClient_OutgoingMessage(message, timestampMsSinceEpoch, recordId);
    }

    public static OutgoingMessage of(
        org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage message,
        long timestampMsSinceEpoch,
        @Nullable String recordId) {
      PubsubMessage.Builder builder =
          PubsubMessage.newBuilder().setData(ByteString.copyFrom(message.getPayload()));
      if (message.getAttributeMap() != null) {
        builder.putAllAttributes(message.getAttributeMap());
      }
      return of(builder.build(), timestampMsSinceEpoch, recordId);
    }
  }

  /**
   * A message received from Pubsub.
   *
   * <p>NOTE: This class is {@link Serializable} only to support the {@link PubsubTestClient}. Java
   * serialization is never used for non-test clients.
   */
  @AutoValue
  abstract static class IncomingMessage implements Serializable {

    /** Underlying Message. */
    public abstract PubsubMessage message();

    /**
     * Timestamp for element (ms since epoch). Either Pubsub's processing time, or the custom
     * timestamp associated with the message.
     */
    public abstract long timestampMsSinceEpoch();

    /** Timestamp (in system time) at which we requested the message (ms since epoch). */
    public abstract long requestTimeMsSinceEpoch();

    /** Id to pass back to Pubsub to acknowledge receipt of this message. */
    public abstract String ackId();

    /** Id to pass to the runner to distinguish this message from all others. */
    public abstract String recordId();

    public static IncomingMessage of(
        PubsubMessage message,
        long timestampMsSinceEpoch,
        long requestTimeMsSinceEpoch,
        String ackId,
        String recordId) {
      return new AutoValue_PubsubClient_IncomingMessage(
          message, timestampMsSinceEpoch, requestTimeMsSinceEpoch, ackId, recordId);
    }
  }

  /**
   * Publish {@code outgoingMessages} to Pubsub {@code topic}. Return number of messages published.
   */
  public abstract int publish(TopicPath topic, List<OutgoingMessage> outgoingMessages)
      throws IOException;

  /**
   * Request the next batch of up to {@code batchSize} messages from {@code subscription}. Return
   * the received messages, or empty collection if none were available. Does not wait for messages
   * to arrive if {@code returnImmediately} is {@literal true}. Returned messages will record their
   * request time as {@code requestTimeMsSinceEpoch}.
   */
  public abstract List<IncomingMessage> pull(
      long requestTimeMsSinceEpoch,
      SubscriptionPath subscription,
      int batchSize,
      boolean returnImmediately)
      throws IOException;

  /** Acknowldege messages from {@code subscription} with {@code ackIds}. */
  public abstract void acknowledge(SubscriptionPath subscription, List<String> ackIds)
      throws IOException;

  /**
   * Modify the ack deadline for messages from {@code subscription} with {@code ackIds} to be {@code
   * deadlineSeconds} from now.
   */
  public abstract void modifyAckDeadline(
      SubscriptionPath subscription, List<String> ackIds, int deadlineSeconds) throws IOException;

  /** Create {@code topic}. */
  public abstract void createTopic(TopicPath topic) throws IOException;

  /*
   * Delete {@code topic}.
   */
  public abstract void deleteTopic(TopicPath topic) throws IOException;

  /** Return a list of topics for {@code project}. */
  public abstract List<TopicPath> listTopics(ProjectPath project) throws IOException;

  /** Create {@code subscription} to {@code topic}. */
  public abstract void createSubscription(
      TopicPath topic, SubscriptionPath subscription, int ackDeadlineSeconds) throws IOException;

  /**
   * Create a random subscription for {@code topic}. Return the {@link SubscriptionPath}. It is the
   * responsibility of the caller to later delete the subscription.
   */
  public SubscriptionPath createRandomSubscription(
      ProjectPath project, TopicPath topic, int ackDeadlineSeconds) throws IOException {
    // Create a randomized subscription derived from the topic name.
    String subscriptionName = topic.getName() + "_beam_" + ThreadLocalRandom.current().nextLong();
    SubscriptionPath subscription =
        PubsubClient.subscriptionPathFromName(project.getId(), subscriptionName);
    createSubscription(topic, subscription, ackDeadlineSeconds);
    return subscription;
  }

  /** Delete {@code subscription}. */
  public abstract void deleteSubscription(SubscriptionPath subscription) throws IOException;

  /** Return a list of subscriptions for {@code topic} in {@code project}. */
  public abstract List<SubscriptionPath> listSubscriptions(ProjectPath project, TopicPath topic)
      throws IOException;

  /** Return the ack deadline, in seconds, for {@code subscription}. */
  public abstract int ackDeadlineSeconds(SubscriptionPath subscription) throws IOException;

  /**
   * Return {@literal true} if {@link #pull} will always return empty list. Actual clients will
   * return {@literal false}. Test clients may return {@literal true} to signal that all expected
   * messages have been pulled and the test may complete.
   */
  public abstract boolean isEOF();
}

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

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.client.util.Clock;
import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.naming.SizeLimitExceededException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoDomain;
import org.apache.beam.sdk.extensions.protobuf.ProtoDynamicMessageSchema;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithFailures;
import org.apache.beam.sdk.transforms.WithFailures.Result;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.ThrowingBadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.DefaultErrorHandler;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.EncodableThrowable;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read and Write {@link PTransform}s for Cloud Pub/Sub streams. These transforms create and consume
 * unbounded {@link PCollection PCollections}.
 *
 * <h3>Using local emulator</h3>
 *
 * <p>In order to use local emulator for Pubsub you should use {@code
 * PubsubOptions#setPubsubRootUrl(String)} method to set host and port of your local emulator.
 *
 * <h3>Permissions</h3>
 *
 * <p>Permission requirements depend on the {@link PipelineRunner} that is used to execute the Beam
 * pipeline. Please refer to the documentation of corresponding {@link PipelineRunner
 * PipelineRunners} for more details.
 *
 * <h3>Updates to the I/O connector code</h3>
 *
 * For any significant updates to this I/O connector, please consider involving corresponding code
 * reviewers mentioned <a
 * href="https://github.com/apache/beam/blob/master/sdks/java/io/google-cloud-platform/OWNERS">
 * here</a>.
 *
 * <h3>Example PubsubIO read usage</h3>
 *
 * <pre>{@code
 * // Read from a specific topic; a subscription will be created at pipeline start time.
 * PCollection<PubsubMessage> messages = PubsubIO.readMessages().fromTopic(topic);
 *
 * // Read from a subscription.
 * PCollection<PubsubMessage> messages = PubsubIO.readMessages().fromSubscription(subscription);
 *
 * // Read messages including attributes. All PubSub attributes will be included in the PubsubMessage.
 * PCollection<PubsubMessage> messages = PubsubIO.readMessagesWithAttributes().fromTopic(topic);
 *
 * // Examples of reading different types from PubSub.
 * PCollection<String> strings = PubsubIO.readStrings().fromTopic(topic);
 * PCollection<MyProto> protos = PubsubIO.readProtos(MyProto.class).fromTopic(topic);
 * PCollection<MyType> avros = PubsubIO.readAvros(MyType.class).fromTopic(topic);
 *
 * }</pre>
 *
 * <h3>Example PubsubIO write usage</h3>
 *
 * Data can be written to a single topic or to a dynamic set of topics. In order to write to a
 * single topic, the {@link PubsubIO.Write#to(String)} method can be used. For example:
 *
 * <pre>{@code
 * avros.apply(PubsubIO.writeAvros(MyType.class).to(topic));
 * protos.apply(PubsubIO.writeProtos(MyProto.class).to(topic));
 * strings.apply(PubsubIO.writeStrings().to(topic));
 * }</pre>
 *
 * Dynamic topic destinations can be accomplished by specifying a function to extract the topic from
 * the record using the {@link PubsubIO.Write#to(SerializableFunction)} method. For example:
 *
 * <pre>{@code
 * avros.apply(PubsubIO.writeAvros(MyType.class).
 *      to((ValueInSingleWindow<Event> quote) -> {
 *               String country = quote.getCountry();
 *               return "projects/myproject/topics/events_" + country;
 *              });
 * }</pre>
 *
 * Dynamic topics can also be specified by writing {@link PubsubMessage} objects containing the
 * topic and writing using the {@link PubsubIO#writeMessagesDynamic()} method. For example:
 *
 * <pre>{@code
 * events.apply(MapElements.into(new TypeDescriptor<PubsubMessage>() {})
 *                         .via(e -> new PubsubMessage(
 *                             e.toByteString(), Collections.emptyMap()).withTopic(e.getCountry())))
 * .apply(PubsubIO.writeMessagesDynamic());
 * }</pre>
 *
 * <h3>Custom timestamps</h3>
 *
 * All messages read from PubSub have a stable publish timestamp that is independent of when the
 * message is read from the PubSub topic. By default, the publish time is used as the timestamp for
 * all messages read and the watermark is based on that. If there is a different logical timestamp
 * to be used, that timestamp must be published in a PubSub attribute and specified using {@link
 * PubsubIO.Read#withTimestampAttribute}. See the Javadoc for that method for the timestamp format.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class PubsubIO {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubIO.class);

  /** Factory for creating pubsub client to manage transport. */
  private static final PubsubClient.PubsubClientFactory FACTORY = PubsubJsonClient.FACTORY;

  /**
   * Project IDs must contain 6-63 lowercase letters, digits, or dashes. IDs must start with a
   * letter and may not end with a dash. This regex isn't exact - this allows for patterns that
   * would be rejected by the service, but this is sufficient for basic parsing of table references.
   */
  private static final Pattern PROJECT_ID_REGEXP =
      Pattern.compile("[a-z][-a-z0-9:.]{4,61}[a-z0-9]");

  private static final Pattern SUBSCRIPTION_REGEXP =
      Pattern.compile("projects/([^/]+)/subscriptions/(.+)");

  private static final Pattern TOPIC_REGEXP = Pattern.compile("projects/([^/]+)/topics/(.+)");

  private static final Pattern V1BETA1_SUBSCRIPTION_REGEXP =
      Pattern.compile("/subscriptions/([^/]+)/(.+)");

  private static final Pattern V1BETA1_TOPIC_REGEXP = Pattern.compile("/topics/([^/]+)/(.+)");

  private static final Pattern PUBSUB_NAME_REGEXP = Pattern.compile("[a-zA-Z][-._~%+a-zA-Z0-9]+");

  static final int PUBSUB_MESSAGE_MAX_TOTAL_SIZE = 10_000_000;

  private static final int PUBSUB_NAME_MIN_LENGTH = 3;
  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

  public static final String ENABLE_CUSTOM_PUBSUB_SINK = "enable_custom_pubsub_sink";
  public static final String ENABLE_CUSTOM_PUBSUB_SOURCE = "enable_custom_pubsub_source";

  private static void validateProjectName(String project) {
    Matcher match = PROJECT_ID_REGEXP.matcher(project);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Illegal project name specified in Pubsub subscription: " + project);
    }
  }

  private static void validatePubsubName(String name) {
    if (name.length() < PUBSUB_NAME_MIN_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub object name is shorter than 3 characters: " + name);
    }
    if (name.length() > PUBSUB_NAME_MAX_LENGTH) {
      throw new IllegalArgumentException(
          "Pubsub object name is longer than 255 characters: " + name);
    }

    if (name.startsWith("goog")) {
      throw new IllegalArgumentException("Pubsub object name cannot start with goog: " + name);
    }

    Matcher match = PUBSUB_NAME_REGEXP.matcher(name);
    if (!match.matches()) {
      throw new IllegalArgumentException(
          "Illegal Pubsub object name specified: "
              + name
              + " Please see Javadoc for naming rules.");
    }
  }

  /** Populate common {@link DisplayData} between Pubsub source and sink. */
  private static void populateCommonDisplayData(
      DisplayData.Builder builder,
      String timestampAttribute,
      String idAttribute,
      ValueProvider<PubsubTopic> topic) {
    builder
        .addIfNotNull(
            DisplayData.item("timestampAttribute", timestampAttribute)
                .withLabel("Timestamp Attribute"))
        .addIfNotNull(DisplayData.item("idAttribute", idAttribute).withLabel("ID Attribute"))
        .addIfNotNull(DisplayData.item("topic", topic).withLabel("Pubsub Topic"));
  }

  /** Class representing a Cloud Pub/Sub Subscription. */
  public static class PubsubSubscription implements Serializable {

    private enum Type {
      NORMAL,
      FAKE
    }

    private final PubsubSubscription.Type type;
    private final String project;
    private final String subscription;

    private PubsubSubscription(PubsubSubscription.Type type, String project, String subscription) {
      this.type = type;
      this.project = project;
      this.subscription = subscription;
    }

    /**
     * Creates a class representing a Pub/Sub subscription from the specified subscription path.
     *
     * <p>Cloud Pub/Sub subscription names should be of the form {@code
     * projects/<project>/subscriptions/<subscription>}, where {@code <project>} is the name of the
     * project the subscription belongs to. The {@code <subscription>} component must comply with
     * the following requirements:
     *
     * <ul>
     *   <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and
     *       periods ('.').
     *   <li>Must be between 3 and 255 characters.
     *   <li>Must begin with a letter.
     *   <li>Must end with a letter or a number.
     *   <li>Cannot begin with {@code 'goog'} prefix.
     * </ul>
     */
    public static PubsubSubscription fromPath(String path) {
      if (path.startsWith(SUBSCRIPTION_RANDOM_TEST_PREFIX)
          || path.startsWith(SUBSCRIPTION_STARTING_SIGNAL)) {
        return new PubsubSubscription(PubsubSubscription.Type.FAKE, "", path);
      }

      String projectName, subscriptionName;

      Matcher v1beta1Match = V1BETA1_SUBSCRIPTION_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn(
            "Saw subscription in v1beta1 format. Subscriptions should be in the format "
                + "projects/<project_id>/subscriptions/<subscription_name>");
        projectName = v1beta1Match.group(1);
        subscriptionName = v1beta1Match.group(2);
      } else {
        Matcher match = SUBSCRIPTION_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub subscription is not in "
                  + "projects/<project_id>/subscriptions/<subscription_name> format: "
                  + path);
        }
        projectName = match.group(1);
        subscriptionName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(subscriptionName);
      return new PubsubSubscription(PubsubSubscription.Type.NORMAL, projectName, subscriptionName);
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * v1beta1 API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == PubsubSubscription.Type.NORMAL) {
        return "/subscriptions/" + project + "/" + subscription;
      } else {
        return subscription;
      }
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * v1beta2 API.
     *
     * @deprecated the v1beta2 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta2Path() {
      if (type == PubsubSubscription.Type.NORMAL) {
        return "projects/" + project + "/subscriptions/" + subscription;
      } else {
        return subscription;
      }
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * API.
     */
    public String asPath() {
      if (type == PubsubSubscription.Type.NORMAL) {
        return "projects/" + project + "/subscriptions/" + subscription;
      } else {
        return subscription;
      }
    }

    @Override
    public String toString() {
      return asPath();
    }
  }

  /** Used to build a {@link ValueProvider} for {@link SubscriptionPath}. */
  private static class SubscriptionPathTranslator
      implements SerializableFunction<PubsubSubscription, SubscriptionPath> {

    @Override
    public SubscriptionPath apply(PubsubSubscription from) {
      return PubsubClient.subscriptionPathFromName(from.project, from.subscription);
    }
  }

  /** Used to build a {@link ValueProvider} for {@link TopicPath}. */
  private static class TopicPathTranslator implements SerializableFunction<PubsubTopic, TopicPath> {

    @Override
    public TopicPath apply(PubsubTopic from) {
      return PubsubClient.topicPathFromName(from.project, from.topic);
    }
  }

  /** Class representing a Cloud Pub/Sub Topic. */
  public static class PubsubTopic implements Serializable {
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof PubsubTopic)) {
        return false;
      }
      PubsubTopic that = (PubsubTopic) o;
      return type == that.type && project.equals(that.project) && topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, project, topic);
    }

    private enum Type {
      NORMAL,
      FAKE
    }

    private final PubsubTopic.Type type;
    private final String project;
    private final String topic;

    private PubsubTopic(PubsubTopic.Type type, String project, String topic) {
      this.type = type;
      this.project = project;
      this.topic = topic;
    }

    /**
     * Creates a class representing a Cloud Pub/Sub topic from the specified topic path.
     *
     * <p>Cloud Pub/Sub topic names should be of the form {@code /topics/<project>/<topic>}, where
     * {@code <project>} is the name of the publishing project. The {@code <topic>} component must
     * comply with the following requirements:
     *
     * <ul>
     *   <li>Can only contain lowercase letters, numbers, dashes ('-'), underscores ('_') and
     *       periods ('.').
     *   <li>Must be between 3 and 255 characters.
     *   <li>Must begin with a letter.
     *   <li>Must end with a letter or a number.
     *   <li>Cannot begin with 'goog' prefix.
     * </ul>
     */
    public static PubsubTopic fromPath(String path) {
      if (path.equals(TOPIC_DEV_NULL_TEST_NAME)) {
        return new PubsubTopic(PubsubTopic.Type.FAKE, "", path);
      }

      String projectName, topicName;

      Matcher v1beta1Match = V1BETA1_TOPIC_REGEXP.matcher(path);
      if (v1beta1Match.matches()) {
        LOG.warn(
            "Saw topic in v1beta1 format.  Topics should be in the format "
                + "projects/<project_id>/topics/<topic_name>");
        projectName = v1beta1Match.group(1);
        topicName = v1beta1Match.group(2);
      } else {
        Matcher match = TOPIC_REGEXP.matcher(path);
        if (!match.matches()) {
          throw new IllegalArgumentException(
              "Pubsub topic is not in projects/<project_id>/topics/<topic_name> format: " + path);
        }
        projectName = match.group(1);
        topicName = match.group(2);
      }

      validateProjectName(projectName);
      validatePubsubName(topicName);
      return new PubsubTopic(PubsubTopic.Type.NORMAL, projectName, topicName);
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub v1beta1
     * API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == PubsubTopic.Type.NORMAL) {
        return "/topics/" + project + "/" + topic;
      } else {
        return topic;
      }
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub v1beta2
     * API.
     *
     * @deprecated the v1beta2 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta2Path() {
      if (type == PubsubTopic.Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }

    /** Returns the string representation of this topic as a path used in the Cloud Pub/Sub API. */
    public String asPath() {
      if (type == PubsubTopic.Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }

    public List<String> dataCatalogSegments() {
      return ImmutableList.of(project, topic);
    }

    @Override
    public String toString() {
      return asPath();
    }
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will only contain a {@link PubsubMessage#getPayload() payload}, but no {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessages() {
    return Read.newBuilder().setCoder(PubsubMessagePayloadOnlyCoder.of()).build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will only contain a {@link PubsubMessage#getPayload() payload} with the {@link
   * PubsubMessage#getMessageId() messageId} from PubSub, but no {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessagesWithMessageId() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithMessageIdCoder.of())
        .setNeedsMessageId(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will contain both a {@link PubsubMessage#getPayload() payload} and {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessagesWithAttributes() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesCoder.of())
        .setNeedsAttributes(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will contain both a {@link PubsubMessage#getPayload() payload} and {@link
   * PubsubMessage#getAttributeMap() attributes}, along with the {@link PubsubMessage#getMessageId()
   * messageId} from PubSub.
   */
  public static Read<PubsubMessage> readMessagesWithAttributesAndMessageId() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of())
        .setNeedsAttributes(true)
        .setNeedsMessageId(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will contain a {@link PubsubMessage#getPayload() payload}, {@link
   * PubsubMessage#getAttributeMap() attributes}, along with the {@link PubsubMessage#getMessageId()
   * messageId} and {PubsubMessage#getOrderingKey() orderingKey} from PubSub.
   */
  public static Read<PubsubMessage> readMessagesWithAttributesAndMessageIdAndOrderingKey() {
    return Read.newBuilder()
        .setCoder(PubsubMessageWithAttributesAndMessageIdAndOrderingKeyCoder.of())
        .setNeedsAttributes(true)
        .setNeedsMessageId(true)
        .setNeedsOrderingKey(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads UTF-8 encoded strings from a Google Cloud
   * Pub/Sub stream.
   */
  public static Read<String> readStrings() {
    return Read.newBuilder(
            (PubsubMessage message) -> new String(message.getPayload(), StandardCharsets.UTF_8))
        .setCoder(StringUtf8Coder.of())
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads binary encoded protobuf messages of the
   * given type from a Google Cloud Pub/Sub stream.
   */
  public static <T extends Message> Read<T> readProtos(Class<T> messageClass) {
    // TODO: Stop using ProtoCoder and instead parse the payload directly.
    // We should not be relying on the fact that ProtoCoder's wire format is identical to
    // the protobuf wire format, as the wire format is not part of a coder's API.
    ProtoCoder<T> coder = ProtoCoder.of(messageClass);
    return Read.newBuilder(parsePayloadUsingCoder(coder)).setCoder(coder).build();
  }

  /**
   * Returns a {@link PTransform} that continuously reads binary encoded protobuf messages for the
   * type specified by {@code fullMessageName}.
   *
   * <p>This is primarily here for cases where the message type cannot be known at compile time. If
   * it can be known, prefer {@link PubsubIO#readProtos(Class)}, as {@link DynamicMessage} tends to
   * perform worse than concrete types.
   *
   * <p>Beam will infer a schema for the {@link DynamicMessage} schema. Note that some proto schema
   * features are not supported by all sinks.
   *
   * @param domain The {@link ProtoDomain} that contains the target message and its dependencies.
   * @param fullMessageName The full name of the message for lookup in {@code domain}.
   */
  public static Read<DynamicMessage> readProtoDynamicMessages(
      ProtoDomain domain, String fullMessageName) {
    SerializableFunction<PubsubMessage, DynamicMessage> parser =
        message -> {
          try {
            return DynamicMessage.parseFrom(
                domain.getDescriptor(fullMessageName), message.getPayload());
          } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Could not parse Pub/Sub message", e);
          }
        };

    ProtoDynamicMessageSchema<DynamicMessage> schema =
        ProtoDynamicMessageSchema.forDescriptor(domain, domain.getDescriptor(fullMessageName));
    return Read.newBuilder(parser)
        .setCoder(
            SchemaCoder.of(
                schema.getSchema(),
                TypeDescriptor.of(DynamicMessage.class),
                schema.getToRowFunction(),
                schema.getFromRowFunction()))
        .build();
  }

  /**
   * Similar to {@link PubsubIO#readProtoDynamicMessages(ProtoDomain, String)} but for when the
   * {@link Descriptor} is already known.
   */
  public static Read<DynamicMessage> readProtoDynamicMessages(Descriptor descriptor) {
    return readProtoDynamicMessages(ProtoDomain.buildFrom(descriptor), descriptor.getFullName());
  }

  /**
   * Returns A {@link PTransform} that continuously reads binary encoded Avro messages of the given
   * type from a Google Cloud Pub/Sub stream.
   */
  public static <T> Read<T> readAvros(Class<T> clazz) {
    // TODO: Stop using AvroCoder and instead parse the payload directly.
    // We should not be relying on the fact that AvroCoder's wire format is identical to
    // the Avro wire format, as the wire format is not part of a coder's API.
    AvroCoder<T> coder = AvroCoder.of(clazz);
    return Read.newBuilder(parsePayloadUsingCoder(coder)).setCoder(coder).build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream,
   * mapping each {@link PubsubMessage} into type T using the supplied parse function and coder.
   */
  public static <T> Read<T> readMessagesWithCoderAndParseFn(
      Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
    return Read.newBuilder(parseFn).setCoder(coder).build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream,
   * mapping each {@link PubsubMessage}, with attributes, into type T using the supplied parse
   * function and coder. Similar to {@link #readMessagesWithCoderAndParseFn(Coder, SimpleFunction)},
   * but with the with addition of making the message attributes available to the ParseFn.
   */
  public static <T> Read<T> readMessagesWithAttributesWithCoderAndParseFn(
      Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
    return Read.newBuilder(parseFn).setCoder(coder).setNeedsAttributes(true).build();
  }

  /**
   * Returns a {@link PTransform} that continuously reads binary encoded Avro messages into the Avro
   * {@link GenericRecord} type.
   *
   * <p>Beam will infer a schema for the Avro schema. This allows the output to be used by SQL and
   * by the schema-transform library.
   */
  public static Read<GenericRecord> readAvroGenericRecords(org.apache.avro.Schema avroSchema) {
    AvroCoder<GenericRecord> coder = AvroCoder.of(avroSchema);
    Schema schema = AvroUtils.getSchema(GenericRecord.class, avroSchema);
    return Read.newBuilder(parsePayloadUsingCoder(coder))
        .setCoder(
            SchemaCoder.of(
                schema,
                TypeDescriptor.of(GenericRecord.class),
                AvroUtils.getToRowFunction(GenericRecord.class, avroSchema),
                AvroUtils.getFromRowFunction(GenericRecord.class)))
        .build();
  }

  /**
   * Returns a {@link PTransform} that continuously reads binary encoded Avro messages of the
   * specific type.
   *
   * <p>Beam will infer a schema for the Avro schema. This allows the output to be used by SQL and
   * by the schema-transform library.
   */
  public static <T> Read<T> readAvrosWithBeamSchema(Class<T> clazz) {
    if (clazz.equals(GenericRecord.class)) {
      throw new IllegalArgumentException("For GenericRecord, please call readAvroGenericRecords");
    }
    AvroCoder<T> coder = AvroCoder.of(clazz);
    org.apache.avro.Schema avroSchema = coder.getSchema();
    Schema schema = AvroUtils.getSchema(clazz, avroSchema);
    return Read.newBuilder(parsePayloadUsingCoder(coder))
        .setCoder(
            SchemaCoder.of(
                schema,
                TypeDescriptor.of(clazz),
                AvroUtils.getToRowFunction(clazz, avroSchema),
                AvroUtils.getFromRowFunction(clazz)))
        .build();
  }

  /** Returns A {@link PTransform} that writes to a Google Cloud Pub/Sub stream. */
  public static Write<PubsubMessage> writeMessages() {
    return Write.newBuilder()
        .setTopicProvider(null)
        .setTopicFunction(null)
        .setDynamicDestinations(false)
        .build();
  }

  /**
   * Enables dynamic destination topics. The {@link PubsubMessage} elements are each expected to
   * contain a destination topic, which can be set using {@link PubsubMessage#withTopic}. If {@link
   * Write#to} is called, that will be used instead to generate the topic and the value returned by
   * {@link PubsubMessage#getTopic} will be ignored.
   */
  public static Write<PubsubMessage> writeMessagesDynamic() {
    return Write.newBuilder()
        .setTopicProvider(null)
        .setTopicFunction(null)
        .setDynamicDestinations(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that writes UTF-8 encoded strings to a Google Cloud Pub/Sub
   * stream.
   */
  public static Write<String> writeStrings() {
    return Write.newBuilder(
            (ValueInSingleWindow<String> stringAndWindow) ->
                new PubsubMessage(
                    stringAndWindow.getValue().getBytes(StandardCharsets.UTF_8), ImmutableMap.of()))
        .setDynamicDestinations(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded protobuf messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T extends Message> Write<T> writeProtos(Class<T> messageClass) {
    // TODO: Like in readProtos(), stop using ProtoCoder and instead format the payload directly.
    return Write.newBuilder(formatPayloadUsingCoder(ProtoCoder.of(messageClass)))
        .setDynamicDestinations(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded protobuf messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T extends Message> Write<T> writeProtos(
      Class<T> messageClass,
      SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributeFn) {
    // TODO: Like in readProtos(), stop using ProtoCoder and instead format the payload directly.
    return Write.newBuilder(formatPayloadUsingCoder(ProtoCoder.of(messageClass), attributeFn))
        .setDynamicDestinations(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded Avro messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T> Write<T> writeAvros(Class<T> clazz) {
    // TODO: Like in readAvros(), stop using AvroCoder and instead format the payload directly.
    return Write.newBuilder(formatPayloadUsingCoder(AvroCoder.of(clazz)))
        .setDynamicDestinations(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded Avro messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T> Write<T> writeAvros(
      Class<T> clazz,
      SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributeFn) {
    // TODO: Like in readAvros(), stop using AvroCoder and instead format the payload directly.
    return Write.newBuilder(formatPayloadUsingCoder(AvroCoder.of(clazz), attributeFn))
        .setDynamicDestinations(false)
        .build();
  }

  /** Implementation of read methods. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {

    abstract @Nullable ValueProvider<PubsubTopic> getTopicProvider();

    abstract @Nullable ValueProvider<PubsubTopic> getDeadLetterTopicProvider();

    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    abstract @Nullable ValueProvider<PubsubSubscription> getSubscriptionProvider();

    /** The name of the message attribute to read timestamps from. */
    abstract @Nullable String getTimestampAttribute();

    /** The name of the message attribute to read unique message IDs from. */
    abstract @Nullable String getIdAttribute();

    /** The coder used to decode each record. */
    abstract Coder<T> getCoder();

    /** User function for parsing PubsubMessage object. */
    abstract @Nullable SerializableFunction<PubsubMessage, T> getParseFn();

    abstract @Nullable Schema getBeamSchema();

    abstract @Nullable TypeDescriptor<T> getTypeDescriptor();

    abstract @Nullable SerializableFunction<T, Row> getToRowFn();

    abstract @Nullable SerializableFunction<Row, T> getFromRowFn();

    abstract @Nullable Clock getClock();

    abstract boolean getNeedsAttributes();

    abstract boolean getNeedsMessageId();

    abstract boolean getNeedsOrderingKey();

    abstract BadRecordRouter getBadRecordRouter();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract boolean getValidate();

    abstract Builder<T> toBuilder();

    static <T> Builder<T> newBuilder(SerializableFunction<PubsubMessage, T> parseFn) {
      Builder<T> builder = new AutoValue_PubsubIO_Read.Builder<T>();
      builder.setParseFn(parseFn);
      builder.setPubsubClientFactory(FACTORY);
      builder.setNeedsAttributes(false);
      builder.setNeedsMessageId(false);
      builder.setNeedsOrderingKey(false);
      builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
      builder.setBadRecordErrorHandler(new DefaultErrorHandler<>());
      builder.setValidate(true);
      return builder;
    }

    static Builder<PubsubMessage> newBuilder() {
      return newBuilder(x -> x);
    }

    @AutoValue.Builder
    abstract static class Builder<T> {

      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topic);

      abstract Builder<T> setDeadLetterTopicProvider(ValueProvider<PubsubTopic> deadLetterTopic);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory clientFactory);

      abstract Builder<T> setSubscriptionProvider(ValueProvider<PubsubSubscription> subscription);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setParseFn(SerializableFunction<PubsubMessage, T> parseFn);

      abstract Builder<T> setBeamSchema(@Nullable Schema beamSchema);

      abstract Builder<T> setTypeDescriptor(@Nullable TypeDescriptor<T> typeDescriptor);

      abstract Builder<T> setToRowFn(@Nullable SerializableFunction<T, Row> toRowFn);

      abstract Builder<T> setFromRowFn(@Nullable SerializableFunction<Row, T> fromRowFn);

      abstract Builder<T> setNeedsAttributes(boolean needsAttributes);

      abstract Builder<T> setNeedsMessageId(boolean needsMessageId);

      abstract Builder<T> setNeedsOrderingKey(boolean needsOrderingKey);

      abstract Builder<T> setClock(Clock clock);

      abstract Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Builder<T> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Builder<T> setValidate(boolean validation);

      abstract Read<T> build();
    }

    /**
     * Reads from the given subscription.
     *
     * <p>See {@link PubsubIO.PubsubSubscription#fromPath(String)} for more details on the format of
     * the {@code subscription} string.
     *
     * <p>Multiple readers reading from the same subscription will each receive some arbitrary
     * portion of the data. Most likely, separate readers should use their own subscriptions.
     */
    public Read<T> fromSubscription(String subscription) {
      return fromSubscription(StaticValueProvider.of(subscription));
    }

    /** Like {@code subscription()} but with a {@link ValueProvider}. */
    public Read<T> fromSubscription(ValueProvider<String> subscription) {
      if (subscription.isAccessible()) {
        // Validate.
        PubsubSubscription.fromPath(subscription.get());
      }
      return toBuilder()
          .setSubscriptionProvider(
              NestedValueProvider.of(subscription, PubsubSubscription::fromPath))
          .setValidate(true)
          .build();
    }

    /**
     * Creates and returns a transform for reading from a Cloud Pub/Sub topic. Mutually exclusive
     * with {@link #fromSubscription(String)}.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * {@code topic} string.
     *
     * <p>The Beam runner will start reading data published on this topic from the time the pipeline
     * is started. Any data published on the topic before the pipeline is started will not be read
     * by the runner.
     */
    public Read<T> fromTopic(String topic) {
      return fromTopic(StaticValueProvider.of(topic));
    }

    /** Like {@link Read#fromTopic(String)} but with a {@link ValueProvider}. */
    public Read<T> fromTopic(ValueProvider<String> topic) {
      validateTopic(topic);
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath))
          .setValidate(true)
          .build();
    }

    /**
     * Creates and returns a transform for writing read failures out to a dead-letter topic.
     *
     * <p>The message written to the dead-letter will contain three attributes:
     *
     * <ul>
     *   <li>exceptionClassName: The type of exception that was thrown.
     *   <li>exceptionMessage: The message in the exception
     *   <li>pubsubMessageId: The message id of the original Pub/Sub message if it was read in,
     *       otherwise "<null>"
     * </ul>
     *
     * <p>The {@link PubsubClient.PubsubClientFactory} used in the {@link Write} transform for
     * errors will be the same as used in the final {@link Read} transform.
     *
     * <p>If there <i>might</i> be a parsing error (or similar), then this should be set up on the
     * topic to avoid wasting resources and to provide more error details with the message written
     * to Pub/Sub. Otherwise, the Pub/Sub topic should have a dead-letter configuration set up to
     * avoid an infinite retry loop.
     *
     * <p>Only failures that result from the {@link Read} configuration (e.g. parsing errors) will
     * be sent to the dead-letter topic. Errors that occur after a successful read will need to set
     * up their own {@link Write} transform. Errors with delivery require configuring Pub/Sub itself
     * to write to the dead-letter topic after a certain number of failed attempts.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * {@code deadLetterTopic} string.
     *
     * <p>This functionality is mutually exclusive with {@link Read#withErrorHandler(ErrorHandler)}
     */
    public Read<T> withDeadLetterTopic(String deadLetterTopic) {
      return withDeadLetterTopic(StaticValueProvider.of(deadLetterTopic));
    }

    /** Like {@link Read#withDeadLetterTopic(String)} but with a {@link ValueProvider}. */
    public Read<T> withDeadLetterTopic(ValueProvider<String> deadLetterTopic) {
      validateTopic(deadLetterTopic);
      return toBuilder()
          .setDeadLetterTopicProvider(
              NestedValueProvider.of(deadLetterTopic, PubsubTopic::fromPath))
          .setValidate(true)
          .build();
    }

    /** Handles validation of {@code topic}. */
    private static void validateTopic(ValueProvider<String> topic) {
      if (topic.isAccessible()) {
        PubsubTopic.fromPath(topic.get());
      }
    }

    /**
     * The default client to write to Pub/Sub is the {@link PubsubJsonClient}, created by the {@link
     * PubsubJsonClient.PubsubJsonClientFactory}. This function allows to change the Pub/Sub client
     * by providing another {@link PubsubClient.PubsubClientFactory} like the {@link
     * PubsubGrpcClientFactory}.
     */
    public Read<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).setValidate(true).build();
    }

    /**
     * When reading from Cloud Pub/Sub where record timestamps are provided as Pub/Sub message
     * attributes, specifies the name of the attribute that contains the timestamp.
     *
     * <p>The timestamp value is expected to be represented in the attribute as either:
     *
     * <ul>
     *   <li>a numerical value representing the number of milliseconds since the Unix epoch. For
     *       example, if using the Joda time classes, {@link Instant#getMillis()} returns the
     *       correct value for this attribute.
     *   <li>a String in RFC 3339 format. For example, {@code 2015-10-29T23:41:41.123Z}. The
     *       sub-second component of the timestamp is optional, and digits beyond the first three
     *       (i.e., time units smaller than milliseconds) will be ignored.
     * </ul>
     *
     * <p>If {@code timestampAttribute} is not provided, the timestamp will be taken from the Pubsub
     * message's publish timestamp. All windowing will be done relative to these timestamps.
     *
     * <p>By default, windows are emitted based on an estimate of when this source is likely done
     * producing data for a given timestamp (referred to as the Watermark; see {@link
     * AfterWatermark} for more details). Any late data will be handled by the trigger specified
     * with the windowing strategy &ndash; by default it will be output immediately.
     *
     * <p>Note that the system can guarantee that no late data will ever be seen when it assigns
     * timestamps by arrival time (i.e. {@code timestampAttribute} is not provided).
     *
     * @see <a href="https://www.ietf.org/rfc/rfc3339.txt">RFC 3339</a>
     */
    public Read<T> withTimestampAttribute(String timestampAttribute) {
      return toBuilder().setTimestampAttribute(timestampAttribute).setValidate(true).build();
    }

    /**
     * When reading from Cloud Pub/Sub where unique record identifiers are provided as Pub/Sub
     * message attributes, specifies the name of the attribute containing the unique identifier. The
     * value of the attribute can be any string that uniquely identifies this record.
     *
     * <p>Pub/Sub cannot guarantee that no duplicate data will be delivered on the Pub/Sub stream.
     * If {@code idAttribute} is not provided, Beam cannot guarantee that no duplicate data will be
     * delivered, and deduplication of the stream will be strictly best effort.
     */
    public Read<T> withIdAttribute(String idAttribute) {
      return toBuilder().setIdAttribute(idAttribute).setValidate(true).build();
    }

    /**
     * Causes the source to return a PubsubMessage that includes Pubsub attributes, and uses the
     * given parsing function to transform the PubsubMessage into an output type. A Coder for the
     * output type T must be registered or set on the output via {@link
     * PCollection#setCoder(Coder)}.
     */
    public Read<T> withCoderAndParseFn(Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
      return toBuilder().setCoder(coder).setParseFn(parseFn).setValidate(true).build();
    }

    /**
     * Configures the PubSub read with an alternate error handler. When a message is read from
     * PubSub, but fails to parse, the message and the parse failure information will be sent to the
     * error handler. See {@link ErrorHandler} for more details on configuring an Error Handler.
     * This functionality is mutually exclusive with {@link Read#withDeadLetterTopic(String)}.
     */
    public Read<T> withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .setValidate(true)
          .build();
    }

    /** Disable validation of the existence of the topic. */
    public Read<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    @VisibleForTesting
    /**
     * Set's the internal Clock.
     *
     * <p>Only for use by unit tests.
     */
    Read<T> withClock(Clock clock) {
      return toBuilder().setClock(clock).build();
    }

    @Override
    public PCollection<T> expand(PBegin input) {
      if (getTopicProvider() == null && getSubscriptionProvider() == null) {
        throw new IllegalStateException(
            "Need to set either the topic or the subscription for " + "a PubsubIO.Read transform");
      }
      if (getTopicProvider() != null && getSubscriptionProvider() != null) {
        throw new IllegalStateException(
            "Can't set both the topic and the subscription for " + "a PubsubIO.Read transform");
      }

      if (getDeadLetterTopicProvider() != null
          && !(getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
        throw new IllegalArgumentException(
            "PubSubIO cannot be configured with both a dead letter topic and a bad record router");
      }

      @Nullable
      ValueProvider<TopicPath> topicPath =
          getTopicProvider() == null
              ? null
              : NestedValueProvider.of(getTopicProvider(), new TopicPathTranslator());
      @Nullable
      ValueProvider<SubscriptionPath> subscriptionPath =
          getSubscriptionProvider() == null
              ? null
              : NestedValueProvider.of(getSubscriptionProvider(), new SubscriptionPathTranslator());
      PubsubUnboundedSource source =
          new PubsubUnboundedSource(
              getClock(),
              getPubsubClientFactory(),
              null /* always get project from runtime PipelineOptions */,
              topicPath,
              subscriptionPath,
              getTimestampAttribute(),
              getIdAttribute(),
              getNeedsAttributes(),
              getNeedsMessageId(),
              getNeedsOrderingKey());

      PCollection<PubsubMessage> preParse = input.apply(source);
      return expandReadContinued(preParse, topicPath, subscriptionPath);
    }

    /**
     * Runner agnostic part of the Expansion.
     *
     * <p>Common logics (MapElements, SDK metrics, DLQ, etc) live here as PubsubUnboundedSource is
     * overridden on Dataflow runner.
     */
    private PCollection<T> expandReadContinued(
        PCollection<PubsubMessage> preParse,
        @Nullable ValueProvider<TopicPath> topicPath,
        @Nullable ValueProvider<SubscriptionPath> subscriptionPath) {

      TypeDescriptor<T> typeDescriptor = new TypeDescriptor<T>() {};
      PCollection<T> read;
      if (getDeadLetterTopicProvider() == null
          && (getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
        read = preParse.apply(MapElements.into(typeDescriptor).via(getParseFn()));
      } else {
        // parse PubSub messages, separating out exceptions
        Result<PCollection<T>, KV<PubsubMessage, EncodableThrowable>> result =
            preParse.apply(
                "PubsubIO.Read/Map/Parse-Incoming-Messages",
                MapElements.into(typeDescriptor)
                    .via(getParseFn())
                    .exceptionsVia(new WithFailures.ThrowableHandler<PubsubMessage>() {}));

        // Emit parsed records
        read = result.output();

        // Send exceptions to either the bad record router or the dead letter topic
        if (!(getBadRecordRouter() instanceof ThrowingBadRecordRouter)) {
          PCollection<BadRecord> badRecords =
              result
                  .failures()
                  .apply(
                      "Map Failures To BadRecords",
                      ParDo.of(new ParseReadFailuresToBadRecords(preParse.getCoder())));
          getBadRecordErrorHandler()
              .addErrorCollection(badRecords.setCoder(BadRecord.getCoder(preParse.getPipeline())));
        } else {
          // Write out failures to the provided dead-letter topic.
          result
              .failures()
              // Since the stack trace could easily exceed Pub/Sub limits, we need to remove it from
              // the attributes.
              .apply(
                  "PubsubIO.Read/Map/Remove-Stack-Trace-Attribute",
                  MapElements.into(new TypeDescriptor<KV<PubsubMessage, Map<String, String>>>() {})
                      .via(
                          kv -> {
                            PubsubMessage message = kv.getKey();
                            String messageId =
                                message.getMessageId() == null ? "<null>" : message.getMessageId();
                            Throwable throwable = kv.getValue().throwable();

                            // In order to stay within Pub/Sub limits, we aren't adding the stack
                            // trace to the attributes. Therefore, we need to log the throwable.
                            LOG.error(
                                "Error parsing Pub/Sub message with id '{}'", messageId, throwable);

                            ImmutableMap<String, String> attributes =
                                ImmutableMap.<String, String>builder()
                                    .put("exceptionClassName", throwable.getClass().getName())
                                    .put("exceptionMessage", throwable.getMessage())
                                    .put("pubsubMessageId", messageId)
                                    .build();

                            return KV.of(kv.getKey(), attributes);
                          }))
              .apply(
                  "PubsubIO.Read/Map/Create-Dead-Letter-Payload",
                  MapElements.into(TypeDescriptor.of(PubsubMessage.class))
                      .via(kv -> new PubsubMessage(kv.getKey().getPayload(), kv.getValue())))
              .apply(
                  writeMessages()
                      .to(getDeadLetterTopicProvider().get().asPath())
                      .withClientFactory(getPubsubClientFactory()));
        }
      }
      // report Lineage once
      preParse
          .getPipeline()
          .apply(Impulse.create())
          .apply(
              ParDo.of(
                  new DoFn<byte[], Void>() {
                    @ProcessElement
                    public void process() {
                      if (topicPath != null) {
                        TopicPath topic = topicPath.get();
                        if (topic != null) {
                          Lineage.getSources()
                              .add("pubsub", "topic", topic.getDataCatalogSegments());
                        }
                      }
                      if (subscriptionPath != null) {
                        SubscriptionPath sub = subscriptionPath.get();
                        if (sub != null) {
                          Lineage.getSources()
                              .add("pubsub", "subscription", sub.getDataCatalogSegments());
                        }
                      }
                    }
                  }));
      return read.setCoder(getCoder());
    }

    @Override
    public void validate(PipelineOptions options) {
      if (!getValidate()) {
        return;
      }

      PubsubOptions psOptions = options.as(PubsubOptions.class);

      // Validate the existence of the topic.
      if (getTopicProvider() != null) {
        PubsubTopic topic = getTopicProvider().get();
        boolean topicExists = true;
        try (PubsubClient pubsubClient =
            getPubsubClientFactory()
                .newClient(getTimestampAttribute(), getIdAttribute(), psOptions)) {
          topicExists =
              pubsubClient.isTopicExists(
                  PubsubClient.topicPathFromName(topic.project, topic.topic));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        if (!topicExists) {
          throw new IllegalArgumentException(
              String.format("Pubsub topic '%s' does not exist.", topic));
        }
      }
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateCommonDisplayData(
          builder, getTimestampAttribute(), getIdAttribute(), getTopicProvider());
      builder.addIfNotNull(
          DisplayData.item("subscription", getSubscriptionProvider())
              .withLabel("Pubsub Subscription"));
    }
  }

  private static class ParseReadFailuresToBadRecords
      extends DoFn<KV<PubsubMessage, EncodableThrowable>, BadRecord> {
    private final Coder<PubsubMessage> coder;

    public ParseReadFailuresToBadRecords(Coder<PubsubMessage> coder) {
      this.coder = coder;
    }

    @ProcessElement
    public void processElement(
        OutputReceiver<BadRecord> outputReceiver,
        @Element KV<PubsubMessage, EncodableThrowable> element)
        throws Exception {
      outputReceiver.output(
          BadRecord.fromExceptionInformation(
              element.getKey(),
              coder,
              (Exception) element.getValue().throwable(),
              "Failed to parse message read from PubSub"));
    }
  }

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private PubsubIO() {}

  /** Implementation of write methods. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {

    /**
     * Max batch byte size. Messages are base64 encoded which encodes each set of three bytes into
     * four bytes.
     */
    private static final int MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT = ((10 * 1000 * 1000) / 4) * 3;

    private static final int MAX_PUBLISH_BATCH_SIZE = 100;

    abstract @Nullable ValueProvider<PubsubTopic> getTopicProvider();

    abstract @Nullable SerializableFunction<ValueInSingleWindow<T>, PubsubTopic> getTopicFunction();

    abstract boolean getDynamicDestinations();

    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    /** the batch size for bulk submissions to pubsub. */
    abstract @Nullable Integer getMaxBatchSize();

    /** the maximum batch size, by bytes. */
    abstract @Nullable Integer getMaxBatchBytesSize();

    /** The name of the message attribute to publish message timestamps in. */
    abstract @Nullable String getTimestampAttribute();

    /** The name of the message attribute to publish unique message IDs in. */
    abstract @Nullable String getIdAttribute();

    /** The format function for input PubsubMessage objects. */
    abstract SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> getFormatFn();

    abstract @Nullable String getPubsubRootUrl();

    abstract BadRecordRouter getBadRecordRouter();

    abstract ErrorHandler<BadRecord, ?> getBadRecordErrorHandler();

    abstract boolean getValidate();

    abstract Builder<T> toBuilder();

    static <T> Builder<T> newBuilder(
        SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn) {
      Builder<T> builder = new AutoValue_PubsubIO_Write.Builder<T>();
      builder.setPubsubClientFactory(FACTORY);
      builder.setFormatFn(formatFn);
      builder.setBadRecordRouter(BadRecordRouter.THROWING_ROUTER);
      builder.setBadRecordErrorHandler(new DefaultErrorHandler<>());
      builder.setValidate(true);
      return builder;
    }

    static Builder<PubsubMessage> newBuilder() {
      return newBuilder(x -> x.getValue());
    }

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topicProvider);

      abstract Builder<T> setTopicFunction(
          SerializableFunction<ValueInSingleWindow<T>, PubsubTopic> topicFunction);

      abstract Builder<T> setDynamicDestinations(boolean dynamicDestinations);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory factory);

      abstract Builder<T> setMaxBatchSize(Integer batchSize);

      abstract Builder<T> setMaxBatchBytesSize(Integer maxBatchBytesSize);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setFormatFn(
          SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatFn);

      abstract Builder<T> setPubsubRootUrl(String pubsubRootUrl);

      abstract Builder<T> setBadRecordRouter(BadRecordRouter badRecordRouter);

      abstract Builder<T> setBadRecordErrorHandler(
          ErrorHandler<BadRecord, ?> badRecordErrorHandler);

      abstract Builder<T> setValidate(boolean validation);

      abstract Write<T> build();
    }

    /**
     * Publishes to the specified topic.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * {@code topic} string.
     */
    public Write<T> to(String topic) {
      ValueProvider<String> topicProvider = StaticValueProvider.of(topic);
      validateTopic(topicProvider);
      return to(topicProvider);
    }

    /** Like {@code topic()} but with a {@link ValueProvider}. */
    public Write<T> to(ValueProvider<String> topic) {
      validateTopic(topic);
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, PubsubTopic::fromPath))
          .setTopicFunction(null)
          .setDynamicDestinations(false)
          .setValidate(true)
          .build();
    }

    /**
     * Provides a function to dynamically specify the target topic per message. Not compatible with
     * any of the other to methods. If {@link #to} is called again specifying a topic, then this
     * topicFunction will be ignored.
     */
    public Write<T> to(SerializableFunction<ValueInSingleWindow<T>, String> topicFunction) {
      return toBuilder()
          .setTopicProvider(null)
          .setTopicFunction(v -> PubsubTopic.fromPath(topicFunction.apply(v)))
          .setDynamicDestinations(true)
          .setValidate(true)
          .build();
    }

    /** Handles validation of {@code topic}. */
    private static void validateTopic(ValueProvider<String> topic) {
      if (topic.isAccessible()) {
        PubsubTopic.fromPath(topic.get());
      }
    }

    /**
     * The default client to write to Pub/Sub is the {@link PubsubJsonClient}, created by the {@link
     * PubsubJsonClient.PubsubJsonClientFactory}. This function allows to change the Pub/Sub client
     * by providing another {@link PubsubClient.PubsubClientFactory} like the {@link
     * PubsubGrpcClientFactory}.
     */
    public Write<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).setValidate(true).build();
    }

    /**
     * Writes to Pub/Sub are batched to efficiently send data. The value of the attribute will be a
     * number representing the number of Pub/Sub messages to queue before sending off the bulk
     * request. For example, if given 1000 the write sink will wait until 1000 messages have been
     * received, or the pipeline has finished, whichever is first.
     *
     * <p>Pub/Sub has a limitation of 10mb per individual request/batch. This attribute was
     * requested dynamic to allow larger Pub/Sub messages to be sent using this source. Thus
     * allowing customizable batches and control of number of events before the 10mb size limit is
     * hit.
     */
    public Write<T> withMaxBatchSize(int batchSize) {
      return toBuilder().setMaxBatchSize(batchSize).setValidate(true).build();
    }

    /**
     * Writes to Pub/Sub are limited by 10mb in general. This attribute controls the maximum allowed
     * bytes to be sent to Pub/Sub in a single batched message.
     */
    public Write<T> withMaxBatchBytesSize(int maxBatchBytesSize) {
      return toBuilder().setMaxBatchBytesSize(maxBatchBytesSize).setValidate(true).build();
    }

    /**
     * Writes to Pub/Sub and adds each record's timestamp to the published messages in an attribute
     * with the specified name. The value of the attribute will be a number representing the number
     * of milliseconds since the Unix epoch. For example, if using the Joda time classes, {@link
     * Instant#Instant(long)} can be used to parse this value.
     *
     * <p>If the output from this sink is being read by another Beam pipeline, then {@link
     * PubsubIO.Read#withTimestampAttribute(String)} can be used to ensure the other source reads
     * these timestamps from the appropriate attribute.
     */
    public Write<T> withTimestampAttribute(String timestampAttribute) {
      return toBuilder().setTimestampAttribute(timestampAttribute).setValidate(true).build();
    }

    /**
     * Writes to Pub/Sub, adding each record's unique identifier to the published messages in an
     * attribute with the specified name. The value of the attribute is an opaque string.
     *
     * <p>If the output from this sink is being read by another Beam pipeline, then {@link
     * PubsubIO.Read#withIdAttribute(String)} can be used to ensure that* the other source reads
     * these unique identifiers from the appropriate attribute.
     */
    public Write<T> withIdAttribute(String idAttribute) {
      return toBuilder().setIdAttribute(idAttribute).setValidate(true).build();
    }

    public Write<T> withPubsubRootUrl(String pubsubRootUrl) {
      return toBuilder().setPubsubRootUrl(pubsubRootUrl).setValidate(true).build();
    }

    /**
     * Writes any serialization failures out to the Error Handler. See {@link ErrorHandler} for
     * details on how to configure an Error Handler. Error Handlers are not well supported when
     * writing to topics with schemas, and it is not recommended to configure an error handler if
     * the target topic has a schema.
     */
    public Write<T> withErrorHandler(ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
      return toBuilder()
          .setBadRecordErrorHandler(badRecordErrorHandler)
          .setBadRecordRouter(BadRecordRouter.RECORDING_ROUTER)
          .setValidate(true)
          .build();
    }

    /**
     * Disable validation of the existence of the topic. Validation of the topic works only if the
     * topic is set statically and not dynamically.
     */
    public Write<T> withoutValidation() {
      return toBuilder().setValidate(false).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (getTopicProvider() == null && !getDynamicDestinations()) {
        throw new IllegalStateException(
            "need to set the topic of a PubsubIO.Write transform if not using "
                + "dynamic topic destinations.");
      }

      SerializableFunction<ValueInSingleWindow<T>, PubsubIO.PubsubTopic> topicFunction =
          getTopicFunction();
      if (topicFunction == null && getTopicProvider() != null) {
        topicFunction = v -> getTopicProvider().get();
      }
      int maxMessageSize = PUBSUB_MESSAGE_MAX_TOTAL_SIZE;
      if (input.isBounded() == PCollection.IsBounded.BOUNDED) {
        maxMessageSize =
            Math.min(
                maxMessageSize,
                MoreObjects.firstNonNull(
                    getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT));
      }
      TupleTag<PubsubMessage> pubsubMessageTupleTag = new TupleTag<>();
      PCollectionTuple pubsubMessageTuple =
          input.apply(
              ParDo.of(
                      new PreparePubsubWriteDoFn<>(
                          getFormatFn(),
                          topicFunction,
                          maxMessageSize,
                          getBadRecordRouter(),
                          input.getCoder(),
                          pubsubMessageTupleTag))
                  .withOutputTags(pubsubMessageTupleTag, TupleTagList.of(BAD_RECORD_TAG)));

      getBadRecordErrorHandler()
          .addErrorCollection(
              pubsubMessageTuple
                  .get(BAD_RECORD_TAG)
                  .setCoder(BadRecord.getCoder(input.getPipeline())));
      PCollection<PubsubMessage> pubsubMessages =
          pubsubMessageTuple.get(pubsubMessageTupleTag).setCoder(PubsubMessageWithTopicCoder.of());
      switch (input.isBounded()) {
        case BOUNDED:
          pubsubMessages.apply(
              ParDo.of(
                  new PubsubBoundedWriter(
                      MoreObjects.firstNonNull(getMaxBatchSize(), MAX_PUBLISH_BATCH_SIZE),
                      MoreObjects.firstNonNull(
                          getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT))));
          return PDone.in(input.getPipeline());
        case UNBOUNDED:
          return pubsubMessages.apply(
              new PubsubUnboundedSink(
                  getPubsubClientFactory(),
                  getTopicProvider() != null
                      ? NestedValueProvider.of(getTopicProvider(), new TopicPathTranslator())
                      : null,
                  getTimestampAttribute(),
                  getIdAttribute(),
                  100 /* numShards */,
                  MoreObjects.firstNonNull(
                      getMaxBatchSize(), PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_SIZE),
                  MoreObjects.firstNonNull(
                      getMaxBatchBytesSize(), PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_BYTES),
                  getPubsubRootUrl()));
      }
      throw new RuntimeException(); // cases are exhaustive.
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateCommonDisplayData(
          builder, getTimestampAttribute(), getIdAttribute(), getTopicProvider());
    }

    @Override
    public void validate(PipelineOptions options) {
      if (!getValidate()) {
        return;
      }

      PubsubOptions psOptions = options.as(PubsubOptions.class);

      // Validate the existence of the topic.
      if (getTopicProvider() != null) {
        PubsubTopic topic = getTopicProvider().get();
        boolean topicExists = true;
        try (PubsubClient pubsubClient =
            getPubsubClientFactory()
                .newClient(getTimestampAttribute(), getIdAttribute(), psOptions)) {
          topicExists =
              pubsubClient.isTopicExists(
                  PubsubClient.topicPathFromName(topic.project, topic.topic));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }

        if (!topicExists) {
          throw new IllegalArgumentException(
              String.format("Pubsub topic '%s' does not exist.", topic));
        }
      }
    }

    /**
     * Writer to Pubsub which batches messages from bounded collections.
     *
     * <p>Public so can be suppressed by runners.
     */
    public class PubsubBoundedWriter extends DoFn<PubsubMessage, Void> {
      private class OutgoingData {
        List<OutgoingMessage> messages;
        long bytes;

        OutgoingData() {
          this.messages = Lists.newArrayList();
          this.bytes = 0;
        }
      }

      private transient Map<PubsubTopic, OutgoingData> output;

      private transient PubsubClient pubsubClient;

      private int maxPublishBatchByteSize;
      private int maxPublishBatchSize;

      PubsubBoundedWriter(int maxPublishBatchSize, int maxPublishBatchByteSize) {
        this.maxPublishBatchSize = maxPublishBatchSize;
        this.maxPublishBatchByteSize = maxPublishBatchByteSize;
      }

      PubsubBoundedWriter() {
        this(MAX_PUBLISH_BATCH_SIZE, MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT);
      }

      @StartBundle
      public void startBundle(StartBundleContext c) throws IOException {
        this.output = Maps.newHashMap();

        // NOTE: idAttribute is ignored.
        this.pubsubClient =
            getPubsubClientFactory()
                .newClient(
                    getTimestampAttribute(), null, c.getPipelineOptions().as(PubsubOptions.class));
      }

      @ProcessElement
      public void processElement(@Element PubsubMessage message, @Timestamp Instant timestamp)
          throws IOException, SizeLimitExceededException {
        // Validate again here just as a sanity check.
        PreparePubsubWriteDoFn.validatePubsubMessageSize(message, maxPublishBatchByteSize);
        byte[] payload = message.getPayload();
        int messageSize = payload.length;

        PubsubTopic pubsubTopic;
        if (getTopicProvider() != null) {
          pubsubTopic = getTopicProvider().get();
        } else {
          pubsubTopic =
              PubsubTopic.fromPath(Preconditions.checkArgumentNotNull(message.getTopic()));
        }
        // Checking before adding the message stops us from violating max batch size or bytes
        OutgoingData currentTopicOutput =
            output.computeIfAbsent(pubsubTopic, t -> new OutgoingData());
        if (currentTopicOutput.messages.size() >= maxPublishBatchSize
            || (!currentTopicOutput.messages.isEmpty()
                && (currentTopicOutput.bytes + messageSize) >= maxPublishBatchByteSize)) {
          publish(pubsubTopic, currentTopicOutput.messages);
          currentTopicOutput.messages.clear();
          currentTopicOutput.bytes = 0;
        }

        Map<String, String> attributes = message.getAttributeMap();
        String orderingKey = message.getOrderingKey();

        com.google.pubsub.v1.PubsubMessage.Builder msgBuilder =
            com.google.pubsub.v1.PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(payload))
                .putAllAttributes(attributes);

        if (orderingKey != null) {
          msgBuilder.setOrderingKey(orderingKey);
        }

        // NOTE: The record id is always null.
        currentTopicOutput.messages.add(
            OutgoingMessage.of(
                msgBuilder.build(), timestamp.getMillis(), null, message.getTopic()));
        currentTopicOutput.bytes += messageSize;
      }

      @FinishBundle
      public void finishBundle() throws IOException {
        for (Map.Entry<PubsubTopic, OutgoingData> entry : output.entrySet()) {
          publish(entry.getKey(), entry.getValue().messages);
        }
        output = null;
        pubsubClient.close();
        pubsubClient = null;
      }

      private void publish(PubsubTopic topic, List<OutgoingMessage> messages) throws IOException {
        int n =
            pubsubClient.publish(
                PubsubClient.topicPathFromName(topic.project, topic.topic), messages);
        checkState(n == messages.size());
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.delegate(Write.this);
      }
    }
  }

  private static <T> SerializableFunction<PubsubMessage, T> parsePayloadUsingCoder(Coder<T> coder) {
    return message -> {
      try {
        return CoderUtils.decodeFromByteArray(coder, message.getPayload());
      } catch (CoderException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    };
  }

  private static <T>
      SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatPayloadUsingCoder(
          Coder<T> coder) {
    return input -> {
      try {
        return new PubsubMessage(
            CoderUtils.encodeToByteArray(coder, input.getValue()), ImmutableMap.of());
      } catch (CoderException e) {
        throw new RuntimeException("Could not encode Pubsub message", e);
      }
    };
  }

  private static <T>
      SerializableFunction<ValueInSingleWindow<T>, PubsubMessage> formatPayloadUsingCoder(
          Coder<T> coder,
          SerializableFunction<ValueInSingleWindow<T>, Map<String, String>> attributesFn) {
    return input -> {
      try {
        return new PubsubMessage(
            CoderUtils.encodeToByteArray(coder, input.getValue()), attributesFn.apply(input));
      } catch (CoderException e) {
        throw new RuntimeException("Could not encode Pubsub message", e);
      }
    };
  }
}

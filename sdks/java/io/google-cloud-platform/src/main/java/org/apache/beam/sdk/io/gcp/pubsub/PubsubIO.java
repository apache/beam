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

import com.google.api.client.util.Clock;
import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.naming.SizeLimitExceededException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.OutgoingMessage;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.ProjectPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SubscriptionPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
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
 */
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

  private static final int PUBSUB_NAME_MIN_LENGTH = 3;
  private static final int PUBSUB_NAME_MAX_LENGTH = 255;

  private static final String SUBSCRIPTION_RANDOM_TEST_PREFIX = "_random/";
  private static final String SUBSCRIPTION_STARTING_SIGNAL = "_starting_signal/";
  private static final String TOPIC_DEV_NULL_TEST_NAME = "/topics/dev/null";

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

    private final Type type;
    private final String project;
    private final String subscription;

    private PubsubSubscription(Type type, String project, String subscription) {
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
        return new PubsubSubscription(Type.FAKE, "", path);
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
      return new PubsubSubscription(Type.NORMAL, projectName, subscriptionName);
    }

    /**
     * Returns the string representation of this subscription as a path used in the Cloud Pub/Sub
     * v1beta1 API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
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
      if (type == Type.NORMAL) {
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
      if (type == Type.NORMAL) {
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

  /** Used to build a {@link ValueProvider} for {@link PubsubSubscription}. */
  private static class SubscriptionTranslator
      implements SerializableFunction<String, PubsubSubscription> {

    @Override
    public PubsubSubscription apply(String from) {
      return PubsubSubscription.fromPath(from);
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

  /** Used to build a {@link ValueProvider} for {@link PubsubTopic}. */
  private static class TopicTranslator implements SerializableFunction<String, PubsubTopic> {

    @Override
    public PubsubTopic apply(String from) {
      return PubsubTopic.fromPath(from);
    }
  }

  /** Used to build a {@link ValueProvider} for {@link TopicPath}. */
  private static class TopicPathTranslator implements SerializableFunction<PubsubTopic, TopicPath> {

    @Override
    public TopicPath apply(PubsubTopic from) {
      return PubsubClient.topicPathFromName(from.project, from.topic);
    }
  }

  /** Used to build a {@link ValueProvider} for {@link ProjectPath}. */
  private static class ProjectPathTranslator
      implements SerializableFunction<PubsubSubscription, ProjectPath> {

    @Override
    public ProjectPath apply(PubsubSubscription from) {
      return PubsubClient.projectPathFromId(from.project);
    }
  }

  /** Class representing a Cloud Pub/Sub Topic. */
  public static class PubsubTopic implements Serializable {

    private enum Type {
      NORMAL,
      FAKE
    }

    private final Type type;
    private final String project;
    private final String topic;

    private PubsubTopic(Type type, String project, String topic) {
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
        return new PubsubTopic(Type.FAKE, "", path);
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
      return new PubsubTopic(Type.NORMAL, projectName, topicName);
    }

    /**
     * Returns the string representation of this topic as a path used in the Cloud Pub/Sub v1beta1
     * API.
     *
     * @deprecated the v1beta1 API for Cloud Pub/Sub is deprecated.
     */
    @Deprecated
    public String asV1Beta1Path() {
      if (type == Type.NORMAL) {
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
      if (type == Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }

    /** Returns the string representation of this topic as a path used in the Cloud Pub/Sub API. */
    public String asPath() {
      if (type == Type.NORMAL) {
        return "projects/" + project + "/topics/" + topic;
      } else {
        return topic;
      }
    }

    @Override
    public String toString() {
      return asPath();
    }
  }

  /** Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. */
  private static <T> Read<T> read() {
    return new AutoValue_PubsubIO_Read.Builder<T>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will only contain a {@link PubsubMessage#getPayload() payload}, but no {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessages() {
    return new AutoValue_PubsubIO_Read.Builder<PubsubMessage>()
        .setPubsubClientFactory(FACTORY)
        .setCoder(PubsubMessagePayloadOnlyCoder.of())
        .setParseFn(new IdentityMessageFn())
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will only contain a {@link PubsubMessage#getPayload() payload} with the {@link
   * PubsubMessage#getMessageId() messageId} from PubSub, but no {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessagesWithMessageId() {
    return new AutoValue_PubsubIO_Read.Builder<PubsubMessage>()
        .setPubsubClientFactory(FACTORY)
        .setCoder(PubsubMessageWithMessageIdCoder.of())
        .setParseFn(new IdentityMessageFn())
        .setNeedsAttributes(false)
        .setNeedsMessageId(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will contain both a {@link PubsubMessage#getPayload() payload} and {@link
   * PubsubMessage#getAttributeMap() attributes}.
   */
  public static Read<PubsubMessage> readMessagesWithAttributes() {
    return new AutoValue_PubsubIO_Read.Builder<PubsubMessage>()
        .setPubsubClientFactory(FACTORY)
        .setCoder(PubsubMessageWithAttributesCoder.of())
        .setParseFn(new IdentityMessageFn())
        .setNeedsAttributes(true)
        .setNeedsMessageId(false)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream. The
   * messages will contain both a {@link PubsubMessage#getPayload() payload} and {@link
   * PubsubMessage#getAttributeMap() attributes}, along with the {@link PubsubMessage#getMessageId()
   * messageId} from PubSub.
   */
  public static Read<PubsubMessage> readMessagesWithAttributesAndMessageId() {
    return new AutoValue_PubsubIO_Read.Builder<PubsubMessage>()
        .setPubsubClientFactory(FACTORY)
        .setCoder(PubsubMessageWithAttributesAndMessageIdCoder.of())
        .setParseFn(new IdentityMessageFn())
        .setNeedsAttributes(true)
        .setNeedsMessageId(true)
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads UTF-8 encoded strings from a Google Cloud
   * Pub/Sub stream.
   */
  public static Read<String> readStrings() {
    return new AutoValue_PubsubIO_Read.Builder<String>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setCoder(StringUtf8Coder.of())
        .setParseFn(new ParsePayloadAsUtf8())
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
    return new AutoValue_PubsubIO_Read.Builder<T>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setCoder(coder)
        .setParseFn(new ParsePayloadUsingCoder<>(coder))
        .build();
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
    return new AutoValue_PubsubIO_Read.Builder<T>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setCoder(coder)
        .setParseFn(new ParsePayloadUsingCoder<>(coder))
        .build();
  }

  /**
   * Returns A {@link PTransform} that continuously reads from a Google Cloud Pub/Sub stream,
   * mapping each {@link PubsubMessage} into type T using the supplied parse function and coder.
   */
  public static <T> Read<T> readMessagesWithCoderAndParseFn(
      Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
    return new AutoValue_PubsubIO_Read.Builder<T>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setCoder(coder)
        .setParseFn(parseFn)
        .build();
  }

  /**
   * Returns a {@link PTransform} that continuously reads binary encoded Avro messages into the Avro
   * {@link GenericRecord} type.
   *
   * <p>Beam will infer a schema for the Avro schema. This allows the output to be used by SQL and
   * by the schema-transform library.
   */
  @Experimental(Kind.SCHEMAS)
  public static Read<GenericRecord> readAvroGenericRecords(org.apache.avro.Schema avroSchema) {
    Schema schema = AvroUtils.getSchema(GenericRecord.class, avroSchema);
    AvroCoder<GenericRecord> coder = AvroCoder.of(GenericRecord.class, avroSchema);
    return new AutoValue_PubsubIO_Read.Builder<GenericRecord>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setBeamSchema(schema)
        .setTypeDescriptor(TypeDescriptor.of(GenericRecord.class))
        .setToRowFn(AvroUtils.getToRowFunction(GenericRecord.class, avroSchema))
        .setFromRowFn(AvroUtils.getFromRowFunction(GenericRecord.class))
        .setParseFn(new ParsePayloadUsingCoder<>(coder))
        .build();
  }

  /**
   * Returns a {@link PTransform} that continuously reads binary encoded Avro messages of the
   * specific type.
   *
   * <p>Beam will infer a schema for the Avro schema. This allows the output to be used by SQL and
   * by the schema-transform library.
   */
  @Experimental(Kind.SCHEMAS)
  public static <T> Read<T> readAvrosWithBeamSchema(Class<T> clazz) {
    if (clazz.equals(GenericRecord.class)) {
      throw new IllegalArgumentException("For GenericRecord, please call readAvroGenericRecords");
    }
    org.apache.avro.Schema avroSchema = ReflectData.get().getSchema(clazz);
    AvroCoder<T> coder = AvroCoder.of(clazz);
    Schema schema = AvroUtils.getSchema(clazz, null);
    return new AutoValue_PubsubIO_Read.Builder<T>()
        .setNeedsAttributes(false)
        .setNeedsMessageId(false)
        .setPubsubClientFactory(FACTORY)
        .setBeamSchema(schema)
        .setTypeDescriptor(TypeDescriptor.of(clazz))
        .setToRowFn(AvroUtils.getToRowFunction(clazz, avroSchema))
        .setFromRowFn(AvroUtils.getFromRowFunction(clazz))
        .setParseFn(new ParsePayloadUsingCoder<>(coder))
        .build();
  }

  /** Returns A {@link PTransform} that writes to a Google Cloud Pub/Sub stream. */
  private static <T> Write<T> write() {
    return new AutoValue_PubsubIO_Write.Builder<T>().build();
  }

  /** Returns A {@link PTransform} that writes to a Google Cloud Pub/Sub stream. */
  public static Write<PubsubMessage> writeMessages() {
    return PubsubIO.<PubsubMessage>write().withFormatFn(new IdentityMessageFn());
  }

  /**
   * Returns A {@link PTransform} that writes UTF-8 encoded strings to a Google Cloud Pub/Sub
   * stream.
   */
  public static Write<String> writeStrings() {
    return PubsubIO.<String>write().withFormatFn(new FormatPayloadAsUtf8());
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded protobuf messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T extends Message> Write<T> writeProtos(Class<T> messageClass) {
    // TODO: Like in readProtos(), stop using ProtoCoder and instead format the payload directly.
    return PubsubIO.<T>write()
        .withFormatFn(new FormatPayloadUsingCoder<>(ProtoCoder.of(messageClass)));
  }

  /**
   * Returns A {@link PTransform} that writes binary encoded Avro messages of a given type to a
   * Google Cloud Pub/Sub stream.
   */
  public static <T> Write<T> writeAvros(Class<T> clazz) {
    // TODO: Like in readAvros(), stop using AvroCoder and instead format the payload directly.
    return PubsubIO.<T>write().withFormatFn(new FormatPayloadUsingCoder<>(AvroCoder.of(clazz)));
  }

  /** Implementation of {@link #read}. */
  @AutoValue
  public abstract static class Read<T> extends PTransform<PBegin, PCollection<T>> {
    @Nullable
    abstract ValueProvider<PubsubTopic> getTopicProvider();

    @Nullable
    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    @Nullable
    abstract ValueProvider<PubsubSubscription> getSubscriptionProvider();

    /** The name of the message attribute to read timestamps from. */
    @Nullable
    abstract String getTimestampAttribute();

    /** The name of the message attribute to read unique message IDs from. */
    @Nullable
    abstract String getIdAttribute();

    /** The coder used to decode each record. */
    @Nullable
    abstract Coder<T> getCoder();

    /** User function for parsing PubsubMessage object. */
    @Nullable
    abstract SimpleFunction<PubsubMessage, T> getParseFn();

    @Nullable
    @Experimental(Kind.SCHEMAS)
    abstract Schema getBeamSchema();

    @Nullable
    abstract TypeDescriptor<T> getTypeDescriptor();

    @Nullable
    abstract SerializableFunction<T, Row> getToRowFn();

    @Nullable
    abstract SerializableFunction<Row, T> getFromRowFn();

    @Nullable
    abstract Clock getClock();

    abstract boolean getNeedsAttributes();

    abstract boolean getNeedsMessageId();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topic);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory clientFactory);

      abstract Builder<T> setSubscriptionProvider(ValueProvider<PubsubSubscription> subscription);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setCoder(Coder<T> coder);

      abstract Builder<T> setParseFn(SimpleFunction<PubsubMessage, T> parseFn);

      @Experimental(Kind.SCHEMAS)
      abstract Builder<T> setBeamSchema(@Nullable Schema beamSchema);

      abstract Builder<T> setTypeDescriptor(@Nullable TypeDescriptor<T> typeDescriptor);

      abstract Builder<T> setToRowFn(@Nullable SerializableFunction<T, Row> toRowFn);

      abstract Builder<T> setFromRowFn(@Nullable SerializableFunction<Row, T> fromRowFn);

      abstract Builder<T> setNeedsAttributes(boolean needsAttributes);

      abstract Builder<T> setNeedsMessageId(boolean needsMessageId);

      abstract Builder<T> setClock(@Nullable Clock clock);

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
              NestedValueProvider.of(subscription, new SubscriptionTranslator()))
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

    /** Like {@code topic()} but with a {@link ValueProvider}. */
    public Read<T> fromTopic(ValueProvider<String> topic) {
      if (topic.isAccessible()) {
        // Validate.
        PubsubTopic.fromPath(topic.get());
      }
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, new TopicTranslator()))
          .build();
    }

    /**
     * The default client to write to Pub/Sub is the {@link PubsubJsonClient}, created by the {@link
     * PubsubJsonClient.PubsubJsonClientFactory}. This function allows to change the Pub/Sub client
     * by providing another {@link PubsubClient.PubsubClientFactory} like the {@link
     * PubsubGrpcClientFactory}.
     */
    public Read<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).build();
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
     * <p>If {@code timestampAttribute} is not provided, the system will generate record timestamps
     * the first time it sees each record. All windowing will be done relative to these timestamps.
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
      return toBuilder().setTimestampAttribute(timestampAttribute).build();
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
      return toBuilder().setIdAttribute(idAttribute).build();
    }

    /**
     * Causes the source to return a PubsubMessage that includes Pubsub attributes, and uses the
     * given parsing function to transform the PubsubMessage into an output type. A Coder for the
     * output type T must be registered or set on the output via {@link
     * PCollection#setCoder(Coder)}.
     */
    public Read<T> withCoderAndParseFn(Coder<T> coder, SimpleFunction<PubsubMessage, T> parseFn) {
      return toBuilder().setCoder(coder).setParseFn(parseFn).build();
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
              Optional.ofNullable(getPubsubClientFactory()).orElse(FACTORY),
              null /* always get project from runtime PipelineOptions */,
              topicPath,
              subscriptionPath,
              getTimestampAttribute(),
              getIdAttribute(),
              getNeedsAttributes(),
              getNeedsMessageId());
      PCollection<T> read = input.apply(source).apply(MapElements.via(getParseFn()));
      return (getBeamSchema() != null)
          ? read.setSchema(getBeamSchema(), getTypeDescriptor(), getToRowFn(), getFromRowFn())
          : read.setCoder(getCoder());
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

  /////////////////////////////////////////////////////////////////////////////

  /** Disallow construction of utility class. */
  private PubsubIO() {}

  /** Implementation of {@link #write}. */
  @AutoValue
  public abstract static class Write<T> extends PTransform<PCollection<T>, PDone> {
    /**
     * Max batch byte size. Messages are base64 encoded which encodes each set of three bytes into
     * four bytes.
     */
    private static final int MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT = ((10 * 1000 * 1000) / 4) * 3;

    private static final int MAX_PUBLISH_BATCH_SIZE = 100;

    @Nullable
    abstract ValueProvider<PubsubTopic> getTopicProvider();

    @Nullable
    abstract PubsubClient.PubsubClientFactory getPubsubClientFactory();

    /** the batch size for bulk submissions to pubsub. */
    @Nullable
    abstract Integer getMaxBatchSize();

    /** the maximum batch size, by bytes. */
    @Nullable
    abstract Integer getMaxBatchBytesSize();

    /** The name of the message attribute to publish message timestamps in. */
    @Nullable
    abstract String getTimestampAttribute();

    /** The name of the message attribute to publish unique message IDs in. */
    @Nullable
    abstract String getIdAttribute();

    /** The format function for input PubsubMessage objects. */
    @Nullable
    abstract SimpleFunction<T, PubsubMessage> getFormatFn();

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setTopicProvider(ValueProvider<PubsubTopic> topicProvider);

      abstract Builder<T> setPubsubClientFactory(PubsubClient.PubsubClientFactory factory);

      abstract Builder<T> setMaxBatchSize(Integer batchSize);

      abstract Builder<T> setMaxBatchBytesSize(Integer maxBatchBytesSize);

      abstract Builder<T> setTimestampAttribute(String timestampAttribute);

      abstract Builder<T> setIdAttribute(String idAttribute);

      abstract Builder<T> setFormatFn(SimpleFunction<T, PubsubMessage> formatFn);

      abstract Write<T> build();
    }

    /**
     * Publishes to the specified topic.
     *
     * <p>See {@link PubsubIO.PubsubTopic#fromPath(String)} for more details on the format of the
     * {@code topic} string.
     */
    public Write<T> to(String topic) {
      return to(StaticValueProvider.of(topic));
    }

    /** Like {@code topic()} but with a {@link ValueProvider}. */
    public Write<T> to(ValueProvider<String> topic) {
      return toBuilder()
          .setTopicProvider(NestedValueProvider.of(topic, new TopicTranslator()))
          .build();
    }

    /**
     * The default client to write to Pub/Sub is the {@link PubsubJsonClient}, created by the {@link
     * PubsubJsonClient.PubsubJsonClientFactory}. This function allows to change the Pub/Sub client
     * by providing another {@link PubsubClient.PubsubClientFactory} like the {@link
     * PubsubGrpcClientFactory}.
     */
    public Write<T> withClientFactory(PubsubClient.PubsubClientFactory factory) {
      return toBuilder().setPubsubClientFactory(factory).build();
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
      return toBuilder().setMaxBatchSize(batchSize).build();
    }

    /**
     * Writes to Pub/Sub are limited by 10mb in general. This attribute controls the maximum allowed
     * bytes to be sent to Pub/Sub in a single batched message.
     */
    public Write<T> withMaxBatchBytesSize(int maxBatchBytesSize) {
      return toBuilder().setMaxBatchBytesSize(maxBatchBytesSize).build();
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
      return toBuilder().setTimestampAttribute(timestampAttribute).build();
    }

    /**
     * Writes to Pub/Sub, adding each record's unique identifier to the published messages in an
     * attribute with the specified name. The value of the attribute is an opaque string.
     *
     * <p>If the the output from this sink is being read by another Beam pipeline, then {@link
     * PubsubIO.Read#withIdAttribute(String)} can be used to ensure that* the other source reads
     * these unique identifiers from the appropriate attribute.
     */
    public Write<T> withIdAttribute(String idAttribute) {
      return toBuilder().setIdAttribute(idAttribute).build();
    }

    /**
     * Used to write a PubSub message together with PubSub attributes. The user-supplied format
     * function translates the input type T to a PubsubMessage object, which is used by the sink to
     * separately set the PubSub message's payload and attributes.
     */
    private Write<T> withFormatFn(SimpleFunction<T, PubsubMessage> formatFn) {
      return toBuilder().setFormatFn(formatFn).build();
    }

    @Override
    public PDone expand(PCollection<T> input) {
      if (getTopicProvider() == null) {
        throw new IllegalStateException("need to set the topic of a PubsubIO.Write transform");
      }

      switch (input.isBounded()) {
        case BOUNDED:
          input.apply(
              ParDo.of(
                  new PubsubBoundedWriter(
                      MoreObjects.firstNonNull(getMaxBatchSize(), MAX_PUBLISH_BATCH_SIZE),
                      MoreObjects.firstNonNull(
                          getMaxBatchBytesSize(), MAX_PUBLISH_BATCH_BYTE_SIZE_DEFAULT))));
          return PDone.in(input.getPipeline());
        case UNBOUNDED:
          return input
              .apply(MapElements.via(getFormatFn()))
              .apply(
                  new PubsubUnboundedSink(
                      Optional.ofNullable(getPubsubClientFactory()).orElse(FACTORY),
                      NestedValueProvider.of(getTopicProvider(), new TopicPathTranslator()),
                      getTimestampAttribute(),
                      getIdAttribute(),
                      100 /* numShards */,
                      MoreObjects.firstNonNull(
                          getMaxBatchSize(), PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_SIZE),
                      MoreObjects.firstNonNull(
                          getMaxBatchBytesSize(),
                          PubsubUnboundedSink.DEFAULT_PUBLISH_BATCH_BYTES)));
      }
      throw new RuntimeException(); // cases are exhaustive.
    }

    @Override
    public void populateDisplayData(DisplayData.Builder builder) {
      super.populateDisplayData(builder);
      populateCommonDisplayData(
          builder, getTimestampAttribute(), getIdAttribute(), getTopicProvider());
    }

    /**
     * Writer to Pubsub which batches messages from bounded collections.
     *
     * <p>Public so can be suppressed by runners.
     */
    public class PubsubBoundedWriter extends DoFn<T, Void> {
      private transient List<OutgoingMessage> output;
      private transient PubsubClient pubsubClient;
      private transient int currentOutputBytes;

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
        this.output = new ArrayList<>();
        this.currentOutputBytes = 0;

        // NOTE: idAttribute is ignored.
        this.pubsubClient =
            Optional.ofNullable(getPubsubClientFactory())
                .orElse(FACTORY)
                .newClient(
                    getTimestampAttribute(), null, c.getPipelineOptions().as(PubsubOptions.class));
      }

      @ProcessElement
      public void processElement(ProcessContext c) throws IOException, SizeLimitExceededException {
        byte[] payload;
        PubsubMessage message = getFormatFn().apply(c.element());
        payload = message.getPayload();
        Map<String, String> attributes = message.getAttributeMap();

        if (payload.length > maxPublishBatchByteSize) {
          String msg =
              String.format(
                  "Pub/Sub message size (%d) exceeded maximum batch size (%d)",
                  payload.length, maxPublishBatchByteSize);
          throw new SizeLimitExceededException(msg);
        }

        // Checking before adding the message stops us from violating the max bytes
        if (((currentOutputBytes + payload.length) >= maxPublishBatchByteSize)
            || (output.size() >= maxPublishBatchSize)) {
          publish();
        }

        // NOTE: The record id is always null.
        output.add(
            OutgoingMessage.of(
                com.google.pubsub.v1.PubsubMessage.newBuilder()
                    .setData(ByteString.copyFrom(payload))
                    .putAllAttributes(attributes)
                    .build(),
                c.timestamp().getMillis(),
                null));
        currentOutputBytes += payload.length;
      }

      @FinishBundle
      public void finishBundle() throws IOException {
        if (!output.isEmpty()) {
          publish();
        }
        output = null;
        currentOutputBytes = 0;
        pubsubClient.close();
        pubsubClient = null;
      }

      private void publish() throws IOException {
        PubsubTopic topic = getTopicProvider().get();
        int n =
            pubsubClient.publish(
                PubsubClient.topicPathFromName(topic.project, topic.topic), output);
        checkState(n == output.size());
        output.clear();
        currentOutputBytes = 0;
      }

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        super.populateDisplayData(builder);
        builder.delegate(Write.this);
      }
    }
  }

  private static class ParsePayloadAsUtf8 extends SimpleFunction<PubsubMessage, String> {
    @Override
    public String apply(PubsubMessage input) {
      return new String(input.getPayload(), StandardCharsets.UTF_8);
    }
  }

  private static class ParsePayloadUsingCoder<T> extends SimpleFunction<PubsubMessage, T> {
    private Coder<T> coder;

    public ParsePayloadUsingCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public T apply(PubsubMessage input) {
      try {
        return CoderUtils.decodeFromByteArray(coder, input.getPayload());
      } catch (CoderException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    }
  }

  private static class FormatPayloadAsUtf8 extends SimpleFunction<String, PubsubMessage> {
    @Override
    public PubsubMessage apply(String input) {
      return new PubsubMessage(input.getBytes(StandardCharsets.UTF_8), ImmutableMap.of());
    }
  }

  private static class FormatPayloadUsingCoder<T> extends SimpleFunction<T, PubsubMessage> {
    private Coder<T> coder;

    public FormatPayloadUsingCoder(Coder<T> coder) {
      this.coder = coder;
    }

    @Override
    public PubsubMessage apply(T input) {
      try {
        return new PubsubMessage(CoderUtils.encodeToByteArray(coder, input), ImmutableMap.of());
      } catch (CoderException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    }
  }

  private static class IdentityMessageFn extends SimpleFunction<PubsubMessage, PubsubMessage> {
    @Override
    public PubsubMessage apply(PubsubMessage input) {
      return input;
    }
  }
}

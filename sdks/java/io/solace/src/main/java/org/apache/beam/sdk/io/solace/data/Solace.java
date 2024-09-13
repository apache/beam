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
package org.apache.beam.sdk.io.solace.data;

import com.google.auto.value.AutoValue;
import com.solacesystems.jcsmp.BytesXMLMessage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides core data models and utilities for working with Solace messages in the context of Apache
 * Beam pipelines. This class includes representations for Solace topics, queues, destinations, and
 * message records, as well as a utility for converting Solace messages into Beam-compatible
 * records.
 */
public class Solace {
  /** Represents a Solace queue. */
  public static class Queue {
    private final String name;

    private Queue(String name) {
      this.name = name;
    }

    public static Queue fromName(String name) {
      return new Queue(name);
    }

    public String getName() {
      return name;
    }
  }
  /** Represents a Solace topic. */
  public static class Topic {
    private final String name;

    private Topic(String name) {
      this.name = name;
    }

    public static Topic fromName(String name) {
      return new Topic(name);
    }

    public String getName() {
      return name;
    }
  }
  /** Represents a Solace destination type. */
  public enum DestinationType {
    TOPIC,
    QUEUE,
    UNKNOWN
  }

  /** Represents a Solace message destination (either a Topic or a Queue). */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Destination {
    /**
     * Gets the name of the destination.
     *
     * @return The destination name.
     */
    public abstract String getName();

    /**
     * Gets the type of the destination (TOPIC, QUEUE or UNKNOWN).
     *
     * @return The destination type.
     */
    public abstract DestinationType getType();

    public static Builder builder() {
      return new AutoValue_Solace_Destination.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setName(String name);

      public abstract Builder setType(DestinationType type);

      public abstract Destination build();
    }
  }

  /** Represents a Solace message record with its associated metadata. */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Record {
    /**
     * Gets the unique identifier of the message, a string for an application-specific message
     * identifier.
     *
     * <p>Mapped from {@link BytesXMLMessage#getApplicationMessageId()}
     *
     * @return The message ID, or null if not available.
     */
    @SchemaFieldNumber("0")
    public abstract String getMessageId();

    /**
     * Gets the payload of the message as a ByteString.
     *
     * <p>Mapped from {@link BytesXMLMessage#getBytes()}
     *
     * @return The message payload.
     */
    @SuppressWarnings("mutable")
    @SchemaFieldNumber("1")
    public abstract byte[] getPayload();
    /**
     * Gets the destination (topic or queue) to which the message was sent.
     *
     * <p>Mapped from {@link BytesXMLMessage#getDestination()}
     *
     * @return The destination, or null if not available.
     */
    @SchemaFieldNumber("2")
    public abstract @Nullable Destination getDestination();

    /**
     * Gets the message expiration time in milliseconds since the Unix epoch.
     *
     * <p>A value of 0 indicates the message does not expire.
     *
     * <p>Mapped from {@link BytesXMLMessage#getExpiration()}
     *
     * @return The expiration timestamp.
     */
    @SchemaFieldNumber("3")
    public abstract long getExpiration();

    /**
     * Gets the priority level of the message (0-255, higher is more important). -1 if not set.
     *
     * <p>Mapped from {@link BytesXMLMessage#getPriority()}
     *
     * @return The message priority.
     */
    @SchemaFieldNumber("4")
    public abstract int getPriority();

    /**
     * Indicates whether the message has been redelivered due to a prior delivery failure.
     *
     * <p>Mapped from {@link BytesXMLMessage#getRedelivered()}
     *
     * @return True if redelivered, false otherwise.
     */
    @SchemaFieldNumber("5")
    public abstract boolean getRedelivered();

    /**
     * Gets the destination to which replies to this message should be sent.
     *
     * <p>Mapped from {@link BytesXMLMessage#getReplyTo()}
     *
     * @return The reply-to destination, or null if not specified.
     */
    @SchemaFieldNumber("6")
    public abstract @Nullable Destination getReplyTo();

    /**
     * Gets the timestamp (in milliseconds since the Unix epoch) when the message was received by
     * the Solace broker.
     *
     * <p>Mapped from {@link BytesXMLMessage#getReceiveTimestamp()}
     *
     * @return The timestamp.
     */
    @SchemaFieldNumber("7")
    public abstract @Nullable Long getReceiveTimestamp();

    /**
     * Gets the timestamp (in milliseconds since the Unix epoch) when the message was sent by the
     * sender. Can be null if not provided.
     *
     * @return The sender timestamp, or null if not available.
     */
    @SchemaFieldNumber("8")
    public abstract @Nullable Long getSenderTimestamp();

    /**
     * Gets the sequence number of the message (if applicable).
     *
     * <p>Mapped from {@link BytesXMLMessage#getSequenceNumber()}
     *
     * @return The sequence number, or null if not available.
     */
    @SchemaFieldNumber("9")
    public abstract @Nullable Long getSequenceNumber();

    /**
     * The number of milliseconds before the message is discarded or moved to Dead Message Queue. A
     * value of 0 means the message will never expire. The default value is 0.
     *
     * <p>Mapped from {@link BytesXMLMessage#getTimeToLive()}
     *
     * @return The time-to-live value.
     */
    @SchemaFieldNumber("10")
    public abstract long getTimeToLive();

    /**
     * Gets the ID for the message within its replication group (if applicable).
     *
     * <p>Mapped from {@link BytesXMLMessage#getReplicationGroupMessageId()}
     *
     * <p>The ID for a particular message is only guaranteed to be the same for a particular copy of
     * a message on a particular queue or topic endpoint within a replication group. The same
     * message on different queues or topic endpoints within the same replication group may or may
     * not have the same replication group message ID. See more at <a
     * href="https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm">https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm</a>
     *
     * @return The replication group message ID, or null if not present.
     */
    @SchemaFieldNumber("11")
    public abstract @Nullable String getReplicationGroupMessageId();

    /**
     * Gets the attachment data of the message as a ByteString, if any. This might represent files
     * or other binary content associated with the message.
     *
     * <p>Mapped from {@link BytesXMLMessage#getAttachmentByteBuffer()}
     *
     * @return The attachment data, or an empty ByteString if no attachment is present.
     */
    @SuppressWarnings("mutable")
    @SchemaFieldNumber("12")
    public abstract byte[] getAttachmentBytes();

    public static Builder builder() {
      return new AutoValue_Solace_Record.Builder()
          .setExpiration(0L)
          .setPriority(-1)
          .setRedelivered(false)
          .setTimeToLive(0)
          .setAttachmentBytes(new byte[0]);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMessageId(String messageId);

      public abstract Builder setPayload(byte[] payload);

      public abstract Builder setDestination(@Nullable Destination destination);

      public abstract Builder setExpiration(long expiration);

      public abstract Builder setPriority(int priority);

      public abstract Builder setRedelivered(boolean redelivered);

      public abstract Builder setReplyTo(@Nullable Destination replyTo);

      public abstract Builder setReceiveTimestamp(@Nullable Long receiveTimestamp);

      public abstract Builder setSenderTimestamp(@Nullable Long senderTimestamp);

      public abstract Builder setSequenceNumber(@Nullable Long sequenceNumber);

      public abstract Builder setTimeToLive(long timeToLive);

      public abstract Builder setReplicationGroupMessageId(
          @Nullable String replicationGroupMessageId);

      public abstract Builder setAttachmentBytes(byte[] attachmentBytes);

      public abstract Record build();
    }
  }

  /**
   * The result of writing a message to Solace. This will be returned by the {@link
   * com.google.cloud.dataflow.dce.io.solace.SolaceIO.Write} connector.
   *
   * <p>This class provides a builder to create instances, but you will probably not need it. The
   * write connector will create and return instances of {@link Solace.PublishResult}.
   *
   * <p>If the message has been published, {@link Solace.PublishResult#getPublished()} will be true.
   * If it is false, it means that the message could not be published, and {@link
   * Solace.PublishResult#getError()} will contain more details about why the message could not be
   * published.
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PublishResult {
    /** The message id of the message that was published. */
    @SchemaFieldNumber("0")
    public abstract String getMessageId();

    /** Whether the message was published or not. */
    @SchemaFieldNumber("1")
    public abstract Boolean getPublished();

    /**
     * The publishing latency in milliseconds. This is the difference between the time the message
     * was created, and the time the message was published. It is only available if the {@link
     * CorrelationKey} class is used as correlation key of the messages.
     */
    @SchemaFieldNumber("2")
    public abstract @Nullable Long getLatencyMilliseconds();

    /** The error details if the message could not be published. */
    @SchemaFieldNumber("3")
    public abstract @Nullable String getError();

    public static Builder builder() {
      return new AutoValue_Solace_PublishResult.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMessageId(String messageId);

      public abstract Builder setPublished(Boolean published);

      public abstract Builder setLatencyMilliseconds(Long latencyMs);

      public abstract Builder setError(String error);

      public abstract PublishResult build();
    }
  }

  /**
   * The correlation key is an object that is passed back to the client during the event broker ack
   * or nack.
   *
   * <p>In the streaming writer is optionally used to calculate publish latencies, by calculating
   * the time difference between the creation of the correlation key, and the time of the ack.
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class CorrelationKey {
    @SchemaFieldNumber("0")
    public abstract String getMessageId();

    @SchemaFieldNumber("1")
    public abstract long getPublishMonotonicMillis();

    public static Builder builder() {
      return new AutoValue_Solace_CorrelationKey.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMessageId(String messageId);

      public abstract Builder setPublishMonotonicMillis(long millis);

      public abstract CorrelationKey build();
    }
  }

  /**
   * A utility class for mapping {@link BytesXMLMessage} instances to {@link Solace.Record} objects.
   * This simplifies the process of converting raw Solace messages into a format suitable for use
   * within Apache Beam pipelines.
   */
  public static class SolaceRecordMapper {
    private static final Logger LOG = LoggerFactory.getLogger(SolaceRecordMapper.class);
    /**
     * Maps a {@link BytesXMLMessage} (if not null) to a {@link Solace.Record}.
     *
     * <p>Extracts relevant information from the message, including payload, metadata, and
     * destination details.
     *
     * @param msg The Solace message to map.
     * @return A Solace Record representing the message, or null if the input message was null.
     */
    public static @Nullable Record map(@Nullable BytesXMLMessage msg) {
      if (msg == null) {
        return null;
      }

      ByteArrayOutputStream payloadBytesStream = new ByteArrayOutputStream();
      if (msg.getContentLength() != 0) {
        try {
          payloadBytesStream.write(msg.getBytes());
        } catch (IOException e) {
          LOG.error("Could not write bytes from the BytesXMLMessage to the Solace.record.", e);
        }
      }

      ByteArrayOutputStream attachmentBytesStream = new ByteArrayOutputStream();
      if (msg.getAttachmentContentLength() != 0) {
        try {
          attachmentBytesStream.write(msg.getAttachmentByteBuffer().array());
        } catch (IOException e) {
          LOG.error(
              "Could not AttachmentByteBuffer from the BytesXMLMessage to the Solace.record.", e);
        }
      }

      Destination replyTo = getDestination(msg.getCorrelationId(), msg.getReplyTo());
      Destination destination = getDestination(msg.getCorrelationId(), msg.getDestination());
      return Record.builder()
          .setMessageId(msg.getApplicationMessageId())
          .setPayload(payloadBytesStream.toByteArray())
          .setDestination(destination)
          .setExpiration(msg.getExpiration())
          .setPriority(msg.getPriority())
          .setRedelivered(msg.getRedelivered())
          .setReplyTo(replyTo)
          .setReceiveTimestamp(msg.getReceiveTimestamp())
          .setSenderTimestamp(msg.getSenderTimestamp())
          .setSequenceNumber(msg.getSequenceNumber())
          .setTimeToLive(msg.getTimeToLive())
          .setReplicationGroupMessageId(
              msg.getReplicationGroupMessageId() != null
                  ? msg.getReplicationGroupMessageId().toString()
                  : null)
          .setAttachmentBytes(attachmentBytesStream.toByteArray())
          .build();
    }

    private static @Nullable Destination getDestination(
        String msgId, com.solacesystems.jcsmp.Destination originalDestinationField) {
      if (originalDestinationField == null) {
        return null;
      }
      Destination.Builder destinationBuilder =
          Destination.builder().setName(originalDestinationField.getName());
      if (originalDestinationField instanceof com.solacesystems.jcsmp.Topic) {
        destinationBuilder.setType(DestinationType.TOPIC);
      } else if (originalDestinationField instanceof com.solacesystems.jcsmp.Queue) {
        destinationBuilder.setType(DestinationType.QUEUE);
      } else {
        LOG.error(
            "SolaceIO: Unknown destination type type for message {}, setting to {}",
            msgId,
            DestinationType.UNKNOWN.name());
        destinationBuilder.setType(DestinationType.UNKNOWN);
      }
      return destinationBuilder.build();
    }
  }
}

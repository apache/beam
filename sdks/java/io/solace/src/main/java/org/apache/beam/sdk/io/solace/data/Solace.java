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
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A record to be written to a Solace topic.
 *
 * <p>You need to transform to {@link Record} to be able to write to Solace. For that, you can use
 * the {@link Record.Builder} provided with this class.
 *
 * <p>For instance, to create a record, use the following code:
 *
 * <pre>{@code
 * Solace.Record record = Solace.Record.builder()
 *         .setMessageId(messageId)
 *         .setSenderTimestamp(timestampMillis)
 *         .setPayload(payload)
 *         .build();
 * }</pre>
 *
 * Setting the message id and the timestamp is mandatory.
 */
public class Solace {

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

  public enum DestinationType {
    TOPIC,
    QUEUE
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Destination {
    @SchemaFieldNumber("0")
    public abstract String getName();

    @SchemaFieldNumber("1")
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

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Record implements Serializable {
    @SchemaFieldNumber("0")
    public abstract @Nullable String getMessageId();

    @SuppressWarnings("mutable")
    @SchemaFieldNumber("1")
    public abstract byte[] getPayload();

    @SchemaFieldNumber("2")
    public abstract @Nullable Destination getDestination();

    @SchemaFieldNumber("3")
    public abstract @Nullable Long getExpiration();

    @SchemaFieldNumber("4")
    public abstract @Nullable Integer getPriority();

    @SchemaFieldNumber("5")
    public abstract @Nullable Boolean getRedelivered();

    @SchemaFieldNumber("6")
    public abstract @Nullable String getReplyTo();

    @SchemaFieldNumber("7")
    public abstract @Nullable Long getReceiveTimestamp();

    @SchemaFieldNumber("8")
    public abstract @Nullable Long getSenderTimestamp();

    @SchemaFieldNumber("9")
    public abstract @Nullable Long getSequenceNumber();

    @SchemaFieldNumber("10")
    public abstract @Nullable Long getTimeToLive();

    /**
     * The ID for a particular message is only guaranteed to be the same for a particular copy of a
     * message on a particular queue or topic endpoint within a replication group. The same message
     * on different queues or topic endpoints within the same replication group may or may not have
     * the same replication group message ID. See more at <a
     * href="https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm">https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm</a>
     */
    @SchemaFieldNumber("11")
    public abstract @Nullable String getReplicationGroupMessageId();

    public static Builder builder() {
      return new AutoValue_Solace_Record.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setMessageId(String messageId);

      public abstract Builder setPayload(byte[] payload);

      public abstract Builder setDestination(Destination destination);

      public abstract Builder setExpiration(Long expiration);

      public abstract Builder setPriority(Integer priority);

      public abstract Builder setRedelivered(Boolean redelivered);

      public abstract Builder setReplyTo(String replyTo);

      public abstract Builder setReceiveTimestamp(Long receiveTimestamp);

      public abstract Builder setSenderTimestamp(Long senderTimestamp);

      public abstract Builder setSequenceNumber(Long sequenceNumber);

      public abstract Builder setTimeToLive(Long timeToLive);

      public abstract Builder setReplicationGroupMessageId(String replicationGroupMessageId);

      public abstract Record build();
    }
  }

  /**
   * The result of writing a message to Solace. This will be returned by the {@link
   * org.apache.beam.sdk.io.solace.SolaceIO.Write} connector.
   *
   * <p>This class provides a builder to create instances, but you will probably not need it. The
   * write connector will create and return instances of {@link PublishResult}.
   *
   * <p>If the message has been published, {@link PublishResult#getPublished()} will be true. If it
   * is false, it means that the message could not be published, and {@link
   * PublishResult#getError()} will contain more details about why the message could not be
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

  public static class SolaceRecordMapper {
    public static final Logger LOG = LoggerFactory.getLogger(SolaceRecordMapper.class);

    public static Record map(@Nullable BytesXMLMessage msg) {
      if (msg == null) {
        return null;
      }

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      if (msg.getContentLength() != 0) {
        try {
          outputStream.write(msg.getBytes());
        } catch (IOException e) {
          LOG.error("Could not write Bytes from the BytesXMLMessage to the Solace.record.", e);
        }
      }
      if (msg.getAttachmentContentLength() != 0) {
        try {
          outputStream.write(msg.getAttachmentByteBuffer().array());
        } catch (IOException e) {
          LOG.error(
              "Could not AttachmentByteBuffer from the BytesXMLMessage to the" + " Solace.record.",
              e);
        }
      }

      String replyTo = (msg.getReplyTo() != null) ? msg.getReplyTo().getName() : null;

      com.solacesystems.jcsmp.Destination originalDestination = msg.getDestination();
      Destination.Builder destBuilder =
          Destination.builder().setName(originalDestination.getName());
      if (originalDestination instanceof Topic) {
        destBuilder.setType(DestinationType.TOPIC);
      } else if (originalDestination instanceof Queue) {
        destBuilder.setType(DestinationType.QUEUE);
      } else {
        LOG.error(
            "SolaceIO: Unknown destination type for message {}, assuming that {} is a" + " topic",
            msg.getCorrelationId(),
            originalDestination.getName());
        destBuilder.setType(DestinationType.TOPIC);
      }

      return Record.builder()
          .setDestination(destBuilder.build())
          .setExpiration(msg.getExpiration())
          .setMessageId(msg.getApplicationMessageId())
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
          .setPayload(outputStream.toByteArray())
          .build();
    }
  }
}

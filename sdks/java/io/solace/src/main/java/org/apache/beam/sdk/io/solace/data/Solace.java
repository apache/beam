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
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A record to be written to a Solace topic.
 *
 * <p>You need to transform to {@link Solace.Record} to be able to write to Solace. For that, you
 * can use the {@link Solace.Record.Builder} provided with this class.
 *
 * <p>For instance, to create a record, use the following code:
 *
 * <pre>{@code
 * Solace.Record record = Solace.Record.builder()
 *         .setMessageId(messageId)
 *         .setSenderTimestamp(timestampMillis)
 *         .setPayload(payload)
 *         ...
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
    QUEUE,
    UNKNOWN
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Destination {
    @SchemaFieldNumber("0")
    public abstract String getName();

    @SchemaFieldNumber("1")
    public abstract DestinationType getType();

    static Builder builder() {
      return new AutoValue_Solace_Destination.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setName(String name);

      abstract Builder setType(DestinationType type);

      abstract Destination build();
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Record {
    @SchemaFieldNumber("0")
    public abstract @Nullable String getMessageId();

    @SchemaFieldNumber("1")
    public abstract ByteString getPayload();

    @SchemaFieldNumber("2")
    public abstract @Nullable Destination getDestination();

    @SchemaFieldNumber("3")
    public abstract long getExpiration();

    @SchemaFieldNumber("4")
    public abstract int getPriority();

    @SchemaFieldNumber("5")
    public abstract boolean getRedelivered();

    @SchemaFieldNumber("6")
    public abstract @Nullable Destination getReplyTo();

    @SchemaFieldNumber("7")
    public abstract long getReceiveTimestamp();

    @SchemaFieldNumber("8")
    public abstract @Nullable Long getSenderTimestamp();

    @SchemaFieldNumber("9")
    public abstract @Nullable Long getSequenceNumber();

    @SchemaFieldNumber("10")
    public abstract long getTimeToLive();

    /**
     * The ID for a particular message is only guaranteed to be the same for a particular copy of a
     * message on a particular queue or topic endpoint within a replication group. The same message
     * on different queues or topic endpoints within the same replication group may or may not have
     * the same replication group message ID. See more at <a
     * href="https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm">https://docs.solace.com/API/API-Developer-Guide/Detecting-Duplicate-Mess.htm</a>
     */
    @SchemaFieldNumber("11")
    public abstract @Nullable String getReplicationGroupMessageId();

    @SchemaFieldNumber("12")
    public abstract ByteString getAttachmentBytes();

    static Builder builder() {
      return new AutoValue_Solace_Record.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setMessageId(@Nullable String messageId);

      abstract Builder setPayload(ByteString payload);

      abstract Builder setDestination(@Nullable Destination destination);

      abstract Builder setExpiration(long expiration);

      abstract Builder setPriority(int priority);

      abstract Builder setRedelivered(boolean redelivered);

      abstract Builder setReplyTo(@Nullable Destination replyTo);

      abstract Builder setReceiveTimestamp(long receiveTimestamp);

      abstract Builder setSenderTimestamp(@Nullable Long senderTimestamp);

      abstract Builder setSequenceNumber(@Nullable Long sequenceNumber);

      abstract Builder setTimeToLive(long timeToLive);

      abstract Builder setReplicationGroupMessageId(@Nullable String replicationGroupMessageId);

      abstract Builder setAttachmentBytes(ByteString attachmentBytes);

      abstract Record build();
    }
  }

  public static class SolaceRecordMapper {
    private static final Logger LOG = LoggerFactory.getLogger(SolaceRecordMapper.class);

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
          .setPayload(ByteString.copyFrom(payloadBytesStream.toByteArray()))
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
          .setAttachmentBytes(ByteString.copyFrom(attachmentBytesStream.toByteArray()))
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

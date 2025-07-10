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

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.MessageType;
import com.solacesystems.jcsmp.ReplicationGroupMessageId;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.User_Cos;
import com.solacesystems.jcsmp.impl.ReplicationGroupMessageIdImpl;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SolaceDataUtils {
  public static final ReplicationGroupMessageId DEFAULT_REPLICATION_GROUP_ID =
      new ReplicationGroupMessageIdImpl(1L, 136L);

  @DefaultSchema(JavaBeanSchema.class)
  public static class SimpleRecord {
    public String payload;
    public String messageId;

    public SimpleRecord() {}

    public SimpleRecord(String payload, String messageId) {
      this.payload = payload;
      this.messageId = messageId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SimpleRecord)) {
        return false;
      }
      SimpleRecord that = (SimpleRecord) o;
      return Objects.equals(payload, that.payload) && Objects.equals(messageId, that.messageId);
    }

    @Override
    public int hashCode() {
      return Objects.hash(payload, messageId);
    }

    @Override
    public String toString() {
      return "SimpleRecord{"
          + "payload='"
          + payload
          + '\''
          + ", messageId='"
          + messageId
          + '\''
          + '}';
    }
  }

  public static Solace.Record getSolaceRecord(String payload, String messageId) {
    return getSolaceRecord(payload, messageId, null);
  }

  public static Solace.Record getSolaceRecord(
      String payload,
      String messageId,
      @Nullable ReplicationGroupMessageId replicationGroupMessageId) {
    String replicationGroupMessageIdString =
        replicationGroupMessageId != null
            ? replicationGroupMessageId.toString()
            : DEFAULT_REPLICATION_GROUP_ID.toString();

    return Solace.Record.builder()
        .setPayload(payload.getBytes(StandardCharsets.UTF_8))
        .setMessageId(messageId)
        .setDestination(
            Solace.Destination.builder()
                .setName("destination-topic")
                .setType(Solace.DestinationType.TOPIC)
                .build())
        .setExpiration(1000L)
        .setPriority(0)
        .setReceiveTimestamp(1708100477067L)
        .setRedelivered(false)
        .setReplyTo(null)
        .setSequenceNumber(null)
        .setTimeToLive(1000L)
        .setSenderTimestamp(null)
        .setReplicationGroupMessageId(replicationGroupMessageIdString)
        .setAttachmentBytes(new byte[0])
        .build();
  }

  public static BytesXMLMessage getBytesXmlMessage(String payload, String messageId) {
    return getBytesXmlMessage(payload, messageId, null, null);
  }

  public static BytesXMLMessage getBytesXmlMessage(
      String payload, String messageId, SerializableFunction<Integer, Integer> ackMessageFn) {
    return getBytesXmlMessage(payload, messageId, ackMessageFn, null);
  }

  public static BytesXMLMessage getBytesXmlMessage(
      String payload,
      String messageId,
      SerializableFunction<Integer, Integer> ackMessageFn,
      ReplicationGroupMessageId replicationGroupMessageId) {
    long receiverTimestamp = 1708100477067L;
    long expiration = 1000L;
    long timeToLive = 1000L;
    String destination = "destination-topic";

    ReplicationGroupMessageId useReplicationGroupId =
        replicationGroupMessageId != null
            ? replicationGroupMessageId
            : DEFAULT_REPLICATION_GROUP_ID;
    return new BytesXMLMessage() {

      @Override
      public byte[] getBytes() {
        return payload.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public int getContentLength() {
        return payload.getBytes(StandardCharsets.UTF_8).length;
      }

      @Override
      public int readBytes(byte[] arg0) {
        return 0;
      }

      @Override
      public int readBytes(byte[] arg0, int arg1) {
        return 0;
      }

      @Override
      public void rewindContent() {}

      @Override
      public void writeBytes(byte[] arg0) {}

      @Override
      public void writeBytes(byte[] arg0, int arg1, int arg2) {}

      @Override
      public void ackMessage() {
        if (ackMessageFn != null) {
          ackMessageFn.apply(0);
        }
      }

      @Override
      public void clearAttachment() {}

      @Override
      public void clearBinaryMetadataBytes(int arg0) {}

      @Override
      public void clearContent() {}

      @Override
      public void clearQueueNameLocation() {}

      @Override
      public void clearTopicNameLocation() {}

      @Override
      public String dump() {
        return null;
      }

      @Override
      public String dump(int arg0) {
        return null;
      }

      @Override
      public long getAckMessageId() {
        return 0;
      }

      @Override
      public String getAppMessageID() {
        return null;
      }

      @Override
      public String getAppMessageType() {
        return null;
      }

      @Override
      public String getApplicationMessageId() {
        return messageId;
      }

      @Override
      public String getApplicationMessageType() {
        return null;
      }

      @Override
      public ByteBuffer getAttachmentByteBuffer() {
        return null;
      }

      @Override
      public int getAttachmentContentLength() {
        return 0;
      }

      @Override
      public int getBinaryMetadataContentLength(int arg0) {
        return 0;
      }

      @Override
      public Collection<Integer> getBinaryMetadataTypes() {
        return null;
      }

      @Override
      public Long getCacheRequestId() {
        return null;
      }

      @Override
      public List<Long> getConsumerIdList() {
        return null;
      }

      @Override
      public String getCorrelationId() {
        return null;
      }

      @Override
      public Object getCorrelationKey() {
        return null;
      }

      @Override
      public User_Cos getCos() {
        return null;
      }

      @Override
      public boolean getDeliverToOne() {
        return false;
      }

      @Override
      public int getDeliveryCount() throws UnsupportedOperationException {
        return 0;
      }

      @Override
      public DeliveryMode getDeliveryMode() {
        return null;
      }

      @Override
      public Destination getDestination() {
        return JCSMPFactory.onlyInstance().createTopic(destination);
      }

      @Override
      public String getDestinationTopicSuffix() {
        return null;
      }

      @Override
      public boolean getDiscardIndication() {
        return false;
      }

      @Override
      public long getExpiration() {
        return expiration;
      }

      @Override
      public String getHTTPContentEncoding() {
        return null;
      }

      @Override
      public String getHTTPContentType() {
        return null;
      }

      @Override
      public String getMessageId() {
        return null;
      }

      @Override
      public long getMessageIdLong() {
        return 0;
      }

      @Override
      public MessageType getMessageType() {
        return null;
      }

      @Override
      public int getPriority() {
        return 0;
      }

      @Override
      public SDTMap getProperties() {
        return null;
      }

      @Override
      public int getQueueNameLength() {
        return 0;
      }

      @Override
      public int getQueueNameOffset() {
        return 0;
      }

      @Override
      public long getReceiveTimestamp() {
        return receiverTimestamp;
      }

      @Override
      public boolean getRedelivered() {
        return false;
      }

      @Override
      public ReplicationGroupMessageId getReplicationGroupMessageId() {
        // this is always set by Solace
        return useReplicationGroupId;
      }

      @Override
      public Destination getReplyTo() {
        return null;
      }

      @Override
      public String getReplyToSuffix() {
        return null;
      }

      @Override
      public Long getSendTimestamp() {
        return null;
      }

      @Override
      public String getSenderID() {
        return null;
      }

      @Override
      public String getSenderId() {
        return null;
      }

      @Override
      public Long getSenderTimestamp() {
        return null;
      }

      @Override
      public Long getSequenceNumber() {
        return null;
      }

      @Override
      public byte getStructuredMsgType() {
        return 0x2;
      }

      @Override
      public boolean getTQDiscardIndication() {
        return false;
      }

      @Override
      public long getTimeToLive() {
        return timeToLive;
      }

      @Override
      public int getTopicNameLength() {
        return 5;
      }

      @Override
      public int getTopicNameOffset() {
        return 0;
      }

      @Override
      public Long getTopicSequenceNumber() {
        return null;
      }

      @Override
      public byte[] getUserData() {
        return null;
      }

      @Override
      public boolean hasAttachment() {
        return false;
      }

      @Override
      public boolean hasBinaryMetadata(int arg0) {
        return false;
      }

      @Override
      public boolean hasContent() {
        return false;
      }

      @Override
      public boolean hasUserData() {
        return false;
      }

      @Override
      public boolean isAckImmediately() {
        return false;
      }

      @Override
      public boolean isCacheMessage() {
        return false;
      }

      @Override
      public boolean isDMQEligible() {
        return false;
      }

      @Override
      public boolean isDeliveryCountSupported() {
        return false;
      }

      @Override
      public boolean isElidingEligible() {
        return false;
      }

      @Override
      public boolean isReadOnly() {
        return false;
      }

      @Override
      public boolean isReplyMessage() {
        return false;
      }

      @Override
      public boolean isStructuredMsg() {
        return false;
      }

      @Override
      public boolean isSuspect() {
        return false;
      }

      @Override
      public int readAttachmentBytes(byte[] arg0) {
        return 0;
      }

      @Override
      public int readAttachmentBytes(byte[] arg0, int arg1) {
        return 0;
      }

      @Override
      public int readAttachmentBytes(int arg0, byte[] arg1, int arg2, int arg3) {
        return 0;
      }

      @Override
      public int readBinaryMetadataBytes(int arg0, byte[] arg1) {
        return 0;
      }

      @Override
      public int readContentBytes(byte[] arg0) {
        return 0;
      }

      @Override
      public int readContentBytes(byte[] arg0, int arg1) {
        return 0;
      }

      @Override
      public int readContentBytes(int arg0, byte[] arg1, int arg2, int arg3) {
        return 0;
      }

      @Override
      public void rejectMessage() {}

      @Override
      public void reset() {}

      @Override
      public void resetPayload() {}

      @Override
      public void rewindAttachment() {}

      @Override
      public void setAckImmediately(boolean arg0) {}

      @Override
      public void setAppMessageID(String arg0) {}

      @Override
      public void setAppMessageType(String arg0) {}

      @Override
      public void setApplicationMessageId(String arg0) {}

      @Override
      public void setApplicationMessageType(String arg0) {}

      @Override
      public void setAsReplyMessage(boolean arg0) {}

      @Override
      public void setCorrelationId(String arg0) {}

      @Override
      public void setCorrelationKey(Object arg0) {}

      @Override
      public void setCos(User_Cos arg0) {}

      @Override
      public void setDMQEligible(boolean arg0) {}

      @Override
      public void setDeliverToOne(boolean arg0) {}

      @Override
      public void setDeliveryMode(DeliveryMode arg0) {}

      @Override
      public void setElidingEligible(boolean arg0) {}

      @Override
      public void setExpiration(long arg0) {}

      @Override
      public void setHTTPContentEncoding(String arg0) {}

      @Override
      public void setHTTPContentType(String arg0) {}

      @Override
      public void setMessageType(MessageType arg0) {}

      @Override
      public void setPriority(int arg0) {}

      @Override
      public void setProperties(SDTMap arg0) {}

      @Override
      public void setQueueNameLocation(int arg0, int arg1) {}

      @Override
      public void setReadOnly() {}

      @Override
      public void setReplyTo(Destination arg0) {}

      @Override
      public void setReplyToSuffix(String arg0) {}

      @Override
      public void setSendTimestamp(long arg0) {}

      @Override
      public void setSenderID(String arg0) {}

      @Override
      public void setSenderId(String arg0) {}

      @Override
      public void setSenderTimestamp(long arg0) {}

      @Override
      public void setSequenceNumber(long arg0) {}

      @Override
      public void setStructuredMsg(boolean arg0) {}

      @Override
      public void setStructuredMsgType(byte arg0) {}

      @Override
      public void setTimeToLive(long arg0) {}

      @Override
      public void setTopicNameLocation(int arg0, int arg1) {}

      @Override
      public void setUserData(byte[] arg0) {}

      @Override
      public void settle(Outcome arg0) throws JCSMPException {}

      @Override
      public int writeAttachment(byte[] arg0) {
        return 0;
      }

      @Override
      public int writeAttachment(InputStream arg0) throws IOException {
        return 0;
      }

      @Override
      public int writeAttachment(byte[] arg0, int arg1, int arg2) throws BufferUnderflowException {
        return 0;
      }

      @Override
      public int writeBinaryMetadataBytes(int arg0, byte[] arg1) {
        return 0;
      }

      @Override
      public int writeBinaryMetadataBytes(int arg0, byte[] arg1, int arg2, int arg3)
          throws BufferUnderflowException {
        return 0;
      }

      @Override
      public int writeNewAttachment(byte[] arg0) {
        return 0;
      }

      @Override
      public int writeNewAttachment(InputStream arg0) throws IOException {
        return 0;
      }

      @Override
      public int writeNewAttachment(byte[] arg0, int arg1, int arg2)
          throws BufferUnderflowException {
        return 0;
      }

      @Override
      public int writeNewAttachment(InputStream arg0, int arg1, int arg2) throws IOException {
        return 0;
      }
    };
  }
}

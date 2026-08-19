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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/** Common util functions for converting between PubsubMessage proto and {@link PubsubMessage}. */
public final class PubsubMessages {
  private PubsubMessages() {}

  public static com.google.pubsub.v1.PubsubMessage toProto(PubsubMessage input) {
    @Nullable Map<String, String> attributes = input.getAttributeMap();
    com.google.pubsub.v1.PubsubMessage.Builder message =
        com.google.pubsub.v1.PubsubMessage.newBuilder()
            .setData(ByteString.copyFrom(input.getPayload()));
    // TODO(https://github.com/apache/beam/issues/19787) this should not be null
    if (attributes != null && !attributes.isEmpty()) {
      message.putAllAttributes(attributes);
    }
    @Nullable String messageId = input.getMessageId();
    if (messageId != null && !messageId.isEmpty()) {
      message.setMessageId(messageId);
    }

    @Nullable String orderingKey = input.getOrderingKey();
    if (orderingKey != null && !orderingKey.isEmpty()) {
      message.setOrderingKey(orderingKey);
    }
    return message.build();
  }

  // Optimization of toProto(input).toByteArray()
  private static byte[] toSerializedPubsubMessageProto(PubsubMessage input) {
    @Nullable Map<String, String> attributes = input.getAttributeMap();
    @Nullable String messageId = input.getMessageId();
    @Nullable String orderingKey = input.getOrderingKey();
    if ((attributes == null || attributes.isEmpty())
        && (messageId == null || messageId.isEmpty())
        && (orderingKey == null || orderingKey.isEmpty())) {
      // Optimize the case where we are just sending a payload.
      byte[] payload = input.getPayload();
      if (payload == null || payload.length == 0) {
        return new byte[0];
      }
      int size =
          CodedOutputStream.computeByteArraySize(
              com.google.pubsub.v1.PubsubMessage.DATA_FIELD_NUMBER, payload);
      byte[] serialized = new byte[size];
      try {
        CodedOutputStream output = CodedOutputStream.newInstance(serialized);
        output.writeByteArray(com.google.pubsub.v1.PubsubMessage.DATA_FIELD_NUMBER, payload);
        output.checkNoSpaceLeft();
      } catch (IOException e) {
        // Should not happen since we are writing to a byte array of the exact size.
        throw new RuntimeException(
            "Unexpected error while serializing PubsubMessage to a byte array.", e);
      }
      return serialized;
    }
    // Fallback to general case by building up a protobuf and serializing it.
    return toProto(input).toByteArray();
  }

  public static PubsubMessage fromProto(com.google.pubsub.v1.PubsubMessage input) {
    return new PubsubMessage(
        input.getData().toByteArray(),
        input.getAttributesMap(),
        input.getMessageId(),
        input.getOrderingKey());
  }

  // Convert the beam PubsubMessage to a serialized com.google.pubsub.v1.PubsubMessage proto
  // representation.
  public static class ParsePayloadAsPubsubMessageProto
      implements SerializableFunction<PubsubMessage, byte[]> {
    @Override
    public byte[] apply(PubsubMessage input) {
      return toSerializedPubsubMessageProto(input);
    }
  }

  // Convert the serialized PubsubMessage proto to PubsubMessage.
  public static class ParsePubsubMessageProtoAsPayload
      implements SerializableFunction<byte[], PubsubMessage> {
    @Override
    public PubsubMessage apply(byte[] input) {
      try {
        com.google.pubsub.v1.PubsubMessage message =
            com.google.pubsub.v1.PubsubMessage.parseFrom(input);
        return fromProto(message);
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    }
  }

  public static class DeserializeBytesIntoPubsubMessagePayloadOnly
      implements SerializableFunction<byte[], PubsubMessage> {

    @Override
    public PubsubMessage apply(byte[] value) {
      return new PubsubMessage(value, ImmutableMap.of());
    }
  }
}

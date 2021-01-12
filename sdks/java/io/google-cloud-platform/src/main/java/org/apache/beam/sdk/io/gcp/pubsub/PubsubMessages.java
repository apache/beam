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
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Map;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Common util functions for converting between PubsubMessage proto and {@link PubsubMessage}. */
public class PubsubMessages {
  // Convert the PubsubMessage to a PubsubMessage proto, then return its serialized representation.
  public static class ParsePayloadAsPubsubMessageProto
      implements SerializableFunction<PubsubMessage, byte[]> {
    @Override
    public byte[] apply(PubsubMessage input) {
      Map<String, String> attributes = input.getAttributeMap();
      com.google.pubsub.v1.PubsubMessage.Builder message =
          com.google.pubsub.v1.PubsubMessage.newBuilder()
              .setData(ByteString.copyFrom(input.getPayload()));
      // TODO(BEAM-8085) this should not be null
      if (attributes != null) {
        message.putAllAttributes(attributes);
      }
      return message.build().toByteArray();
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
        return new PubsubMessage(message.getData().toByteArray(), message.getAttributesMap());
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException("Could not decode Pubsub message", e);
      }
    }
  }
}

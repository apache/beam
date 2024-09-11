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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A coder for PubsubMessage including the topic from the PubSub server. */
public class PubsubMessageWithTopicCoder extends CustomCoder<PubsubMessage> {
  // A message's payload cannot be null
  private static final Coder<byte[]> PAYLOAD_CODER = ByteArrayCoder.of();
  private static final Coder<@Nullable Map<String, String>> ATTRIBUTES_CODER =
      NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
  private static final Coder<@Nullable String> MESSAGE_ID_CODER =
      NullableCoder.of(StringUtf8Coder.of());

  private static final Coder<@Nullable String> TOPIC_CODER = NullableCoder.of(StringUtf8Coder.of());

  public static Coder<PubsubMessage> of(TypeDescriptor<PubsubMessage> ignored) {
    return of();
  }

  public static PubsubMessageWithTopicCoder of() {
    return new PubsubMessageWithTopicCoder();
  }

  @Override
  public void encode(PubsubMessage value, OutputStream outStream) throws IOException {
    PAYLOAD_CODER.encode(value.getPayload(), outStream);
    ATTRIBUTES_CODER.encode(value.getAttributeMap(), outStream);
    MESSAGE_ID_CODER.encode(value.getMessageId(), outStream);
    TOPIC_CODER.encode(value.getTopic(), outStream);
  }

  @Override
  public PubsubMessage decode(InputStream inStream) throws IOException {
    byte[] payload = PAYLOAD_CODER.decode(inStream);
    @Nullable Map<String, String> attributes = ATTRIBUTES_CODER.decode(inStream);
    @Nullable String messageId = MESSAGE_ID_CODER.decode(inStream);
    @Nullable String topic = TOPIC_CODER.decode(inStream);
    PubsubMessage pubsubMessage = new PubsubMessage(payload, attributes, messageId);
    if (topic != null) {
      pubsubMessage = pubsubMessage.withTopic(topic);
    }
    return pubsubMessage;
  }
}

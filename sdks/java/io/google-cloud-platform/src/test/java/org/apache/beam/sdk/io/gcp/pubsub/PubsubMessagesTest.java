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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PubsubMessages}. */
@RunWith(JUnit4.class)
public class PubsubMessagesTest {

  @Test
  public void testRoundTripToProto() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes = ImmutableMap.of("key1", "value1", "key2", "value2");
    String messageId = "test-message-id";
    String orderingKey = "test-ordering-key";

    PubsubMessage originalMessage = new PubsubMessage(payload, attributes, messageId, orderingKey);
    PubsubMessage roundTrippedMessage =
        PubsubMessages.fromProto(PubsubMessages.toProto(originalMessage));

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(originalMessage.getAttributeMap(), roundTrippedMessage.getAttributeMap());
    assertEquals(originalMessage.getMessageId(), roundTrippedMessage.getMessageId());
    assertEquals(originalMessage.getOrderingKey(), roundTrippedMessage.getOrderingKey());
  }

  @Test
  public void testRoundTripToProto_emptyAttributes() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes = Collections.emptyMap();
    String messageId = "test-message-id";
    String orderingKey = "test-ordering-key";

    PubsubMessage originalMessage = new PubsubMessage(payload, attributes, messageId, orderingKey);
    PubsubMessage roundTrippedMessage =
        PubsubMessages.fromProto(PubsubMessages.toProto(originalMessage));

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(originalMessage.getAttributeMap(), roundTrippedMessage.getAttributeMap());
    assertEquals(originalMessage.getMessageId(), roundTrippedMessage.getMessageId());
    assertEquals(originalMessage.getOrderingKey(), roundTrippedMessage.getOrderingKey());
  }

  @Test
  public void testRoundTripToProto_nullAttributes() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    String messageId = "test-message-id";
    String orderingKey = "test-ordering-key";

    PubsubMessage originalMessage = new PubsubMessage(payload, null, messageId, orderingKey);
    PubsubMessage roundTrippedMessage =
        PubsubMessages.fromProto(PubsubMessages.toProto(originalMessage));

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    // PubsubMessage.fromProto returns an empty map when proto attributes map is empty
    assertEquals(Collections.emptyMap(), roundTrippedMessage.getAttributeMap());
    assertEquals(originalMessage.getMessageId(), roundTrippedMessage.getMessageId());
    assertEquals(originalMessage.getOrderingKey(), roundTrippedMessage.getOrderingKey());
  }

  @Test
  public void testRoundTripToProto_nullMessageIdAndOrderingKey() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes = ImmutableMap.of("key", "value");

    PubsubMessage originalMessage = new PubsubMessage(payload, attributes, null, null);
    PubsubMessage roundTrippedMessage =
        PubsubMessages.fromProto(PubsubMessages.toProto(originalMessage));

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(originalMessage.getAttributeMap(), roundTrippedMessage.getAttributeMap());
    // protobuf translates null string into empty string.
    assertTrue(roundTrippedMessage.getMessageId().isEmpty());
    assertTrue(roundTrippedMessage.getOrderingKey().isEmpty());
  }

  @Test
  public void testRoundTripToProto_messageIdAndOrderingKey() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes = ImmutableMap.of("key", "value");

    PubsubMessage originalMessage =
        new PubsubMessage(payload, attributes, "messageId", "orderingKey");
    PubsubMessage roundTrippedMessage =
        PubsubMessages.fromProto(PubsubMessages.toProto(originalMessage));

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(originalMessage.getAttributeMap(), roundTrippedMessage.getAttributeMap());
    assertEquals(originalMessage.getOrderingKey(), roundTrippedMessage.getOrderingKey());
    assertEquals(originalMessage.getMessageId(), roundTrippedMessage.getMessageId());
  }

  @Test
  public void testParsePayloadAsPubsubMessageProto() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    PubsubMessage originalMessage = new PubsubMessage(payload, null, null, null);

    byte[] serialized =
        new PubsubMessages.ParsePayloadAsPubsubMessageProto().apply(originalMessage);
    PubsubMessage roundTrippedMessage =
        new PubsubMessages.ParsePubsubMessageProtoAsPayload().apply(serialized);

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(Collections.emptyMap(), roundTrippedMessage.getAttributeMap());
    assertTrue(
        roundTrippedMessage.getMessageId() == null || roundTrippedMessage.getMessageId().isEmpty());
    assertTrue(
        roundTrippedMessage.getOrderingKey() == null
            || roundTrippedMessage.getOrderingKey().isEmpty());
  }

  @Test
  public void testParsePayloadAsPubsubMessageProto_emptyPayload() {
    byte[] payload = new byte[0];
    PubsubMessage originalMessage = new PubsubMessage(payload, null, null, null);

    byte[] serialized =
        new PubsubMessages.ParsePayloadAsPubsubMessageProto().apply(originalMessage);
    PubsubMessage roundTrippedMessage =
        new PubsubMessages.ParsePubsubMessageProtoAsPayload().apply(serialized);

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(Collections.emptyMap(), roundTrippedMessage.getAttributeMap());
    assertTrue(
        roundTrippedMessage.getMessageId() == null || roundTrippedMessage.getMessageId().isEmpty());
    assertTrue(
        roundTrippedMessage.getOrderingKey() == null
            || roundTrippedMessage.getOrderingKey().isEmpty());
  }

  @Test
  public void testParsePayloadAsPubsubMessageProto_withAttributes() {
    byte[] payload = "test-payload".getBytes(StandardCharsets.UTF_8);
    Map<String, String> attributes = ImmutableMap.of("key1", "value1", "key2", "value2");
    PubsubMessage originalMessage = new PubsubMessage(payload, attributes, null, null);

    byte[] serialized =
        new PubsubMessages.ParsePayloadAsPubsubMessageProto().apply(originalMessage);
    PubsubMessage roundTrippedMessage =
        new PubsubMessages.ParsePubsubMessageProtoAsPayload().apply(serialized);

    assertArrayEquals(originalMessage.getPayload(), roundTrippedMessage.getPayload());
    assertEquals(originalMessage.getAttributeMap(), roundTrippedMessage.getAttributeMap());
    assertTrue(
        roundTrippedMessage.getMessageId() == null || roundTrippedMessage.getMessageId().isEmpty());
    assertTrue(
        roundTrippedMessage.getOrderingKey() == null
            || roundTrippedMessage.getOrderingKey().isEmpty());
  }
}

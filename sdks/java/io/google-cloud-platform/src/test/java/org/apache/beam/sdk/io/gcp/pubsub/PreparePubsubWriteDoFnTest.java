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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.PUBSUB_MESSAGE_MAX_TOTAL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.naming.SizeLimitExceededException;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.RandomStringUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PreparePubsubWriteDoFnTest implements Serializable {
  @Test
  public void testValidatePubsubMessageOnlyPayload() throws SizeLimitExceededException {
    byte[] data = new byte[1024];
    PubsubMessage message = new PubsubMessage(data, null);

    int messageSize =
        PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE);

    assertEquals(data.length, messageSize);
  }

  @Test
  public void testValidatePubsubMessagePayloadAndOrderingKey() throws SizeLimitExceededException {
    byte[] data = new byte[1024];
    String orderingKey = "key";
    PubsubMessage message = new PubsubMessage(data, null, null, orderingKey);

    int messageSize =
        PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE);

    assertEquals(data.length + orderingKey.getBytes(StandardCharsets.UTF_8).length, messageSize);
  }

  @Test
  public void testValidatePubsubMessagePayloadAndAttributes() throws SizeLimitExceededException {
    byte[] data = new byte[1024];
    String attributeKey = "key";
    String attributeValue = "value";
    Map<String, String> attributes = ImmutableMap.of(attributeKey, attributeValue);
    PubsubMessage message = new PubsubMessage(data, attributes);

    int messageSize =
        PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE);

    assertEquals(
        data.length
            + 6 // PUBSUB_MESSAGE_ATTRIBUTE_ENCODE_ADDITIONAL_BYTES
            + attributeKey.getBytes(StandardCharsets.UTF_8).length
            + attributeValue.getBytes(StandardCharsets.UTF_8).length,
        messageSize);
  }

  @Test
  public void testValidatePubsubMessagePayloadTooLarge() {
    byte[] data = new byte[(10 << 20) + 1];
    PubsubMessage message = new PubsubMessage(data, null);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessagePayloadPlusOrderingKeyTooLarge() {
    byte[] data = new byte[(10 << 20)];
    String orderingKey = "key";
    PubsubMessage message = new PubsubMessage(data, null, null, orderingKey);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessagePayloadPlusAttributesTooLarge() {
    byte[] data = new byte[(10 << 20)];
    String attributeKey = "key";
    String attributeValue = "value";
    Map<String, String> attributes = ImmutableMap.of(attributeKey, attributeValue);
    PubsubMessage message = new PubsubMessage(data, attributes);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageAttributeKeyTooLarge() {
    byte[] data = new byte[1024];
    String attributeKey = RandomStringUtils.randomAscii(257);
    String attributeValue = "value";
    Map<String, String> attributes = ImmutableMap.of(attributeKey, attributeValue);
    PubsubMessage message = new PubsubMessage(data, attributes);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageAttributeValueTooLarge() {
    byte[] data = new byte[1024];
    String attributeKey = "key";
    String attributeValue = RandomStringUtils.randomAscii(1025);
    Map<String, String> attributes = ImmutableMap.of(attributeKey, attributeValue);
    PubsubMessage message = new PubsubMessage(data, attributes);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageOrderingKeyTooLarge() {
    byte[] data = new byte[1024];
    String orderingKey = RandomStringUtils.randomAscii(1025);
    PubsubMessage message = new PubsubMessage(data, null, null, orderingKey);

    assertThrows(
        SizeLimitExceededException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageEmptyMessageRejectedNullMap() {
    byte[] data = new byte[0];
    PubsubMessage message = new PubsubMessage(data, null);
    assertThrows(
        "non-empty payload or at least one attribute",
        IllegalArgumentException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageEmptyMessageRejectedEmptyMap() {
    byte[] data = new byte[0];
    PubsubMessage message = new PubsubMessage(data, ImmutableMap.of());
    assertThrows(
        "non-empty payload or at least one attribute",
        IllegalArgumentException.class,
        () -> PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE));
  }

  @Test
  public void testValidatePubsubMessageEmptyDataButAttributesAllowed()
      throws SizeLimitExceededException {
    byte[] data = new byte[0];
    PubsubMessage message = new PubsubMessage(data, ImmutableMap.of("key", "value"));
    PreparePubsubWriteDoFn.validatePubsubMessage(message, PUBSUB_MESSAGE_MAX_TOTAL_SIZE);
  }
}

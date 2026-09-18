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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.TextMessage;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.solace.broker.MessageProducerUtils;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.Solace.Record.PayloadType;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Bytes;
import org.junit.Test;

public class SolaceRecordMapperTest {

  @Test
  public void testNullMessage() {
    assertNull(Solace.SolaceRecordMapper.toRecord(null));
  }

  @Test
  public void testTextMessage() {
    TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setApplicationMessageId("id");
    message.setText("héllo");

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.TEXT, record.getPayloadType());
    assertArrayEquals("héllo".getBytes(StandardCharsets.UTF_8), record.getPayload());
    assertArrayEquals(new byte[0], record.getAttachmentBytes());
  }

  @Test
  public void testBytesMessage() {
    byte[] payload = new byte[] {0, 1, (byte) 255};
    BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
    message.setApplicationMessageId("id");
    message.setData(payload);

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.BYTES, record.getPayloadType());
    assertArrayEquals(payload, record.getPayload());
    assertArrayEquals(new byte[0], record.getAttachmentBytes());
  }

  @Test
  public void testBytesXmlMessageAndAttachment() {
    Record source =
        Solace.Record.builder()
            .setMessageId("id")
            .setPayload(new byte[] {1, 2})
            .setAttachmentBytes(new byte[] {3, 4})
            .build();
    BytesXMLMessage message =
        MessageProducerUtils.createMessage(source, false, DeliveryMode.DIRECT);
    message.setReadOnly();

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.BYTES_XML, record.getPayloadType());
    assertArrayEquals(new byte[] {1, 2}, Arrays.copyOf(record.getPayload(), 2));
    assertArrayEquals(new byte[] {3, 4}, record.getAttachmentBytes());
  }

  @Test
  public void testNullTextPayload() {
    TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setApplicationMessageId("id");

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.TEXT, record.getPayloadType());
    assertArrayEquals(new byte[0], record.getPayload());
  }

  @Test
  public void testNullBytesPayload() {
    BytesMessage message = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
    message.setApplicationMessageId("id");

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.BYTES, record.getPayloadType());
    assertArrayEquals(new byte[0], record.getPayload());
  }

  @Test
  public void testEmptyBytesXmlMessage() {
    BytesXMLMessage message = JCSMPFactory.onlyInstance().createBytesXMLMessage();
    message.setApplicationMessageId("id");

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals(Record.PayloadType.BYTES_XML, record.getPayloadType());
    assertArrayEquals(new byte[0], record.getPayload());
    assertArrayEquals(new byte[0], record.getAttachmentBytes());
  }

  @Test
  public void testMapMessageMetadata() {
    TextMessage message = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
    message.setApplicationMessageId("id");
    message.setText("hello");
    message.setExpiration(123L);
    message.setPriority(7);
    message.setReplyTo(JCSMPFactory.onlyInstance().createQueue("reply-queue"));
    message.setSenderTimestamp(456L);
    message.setTimeToLive(789L);

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertEquals("id", record.getMessageId());
    assertEquals(123L, record.getExpiration());
    assertEquals(7, record.getPriority());
    assertEquals(false, record.getRedelivered());
    assertEquals("reply-queue", record.getReplyTo().getName());
    assertEquals(Solace.DestinationType.QUEUE, record.getReplyTo().getType());
    assertEquals(Long.valueOf(456L), record.getSenderTimestamp());
    assertEquals(789L, record.getTimeToLive());
  }

  @Test
  public void testMapMessageUserProperties() throws SDTException {
    BytesXMLMessage message = JCSMPFactory.onlyInstance().createBytesXMLMessage();
    message.setApplicationMessageId("id");
    SDTMap properties = JCSMPFactory.onlyInstance().createMap();
    properties.putString("string", "value");
    properties.putBoolean("boolean", true);
    properties.putDouble("double", 1.23);
    properties.putFloat("float", 4.56f);
    properties.putLong("long", 123456789L);
    properties.putInteger("integer", 3);
    properties.putShort("short", (short) 123);
    properties.putCharacter("character", 'c');
    properties.putByte("byte", (byte) 1);
    properties.putBytes("bytes", new byte[] {1, 2, 3});
    properties.putByteArray("byteArray", new ByteArray(new byte[] {4, 5, 6}));
    properties.putDestination("topic", JCSMPFactory.onlyInstance().createTopic("topic"));
    properties.putDestination("queue", JCSMPFactory.onlyInstance().createQueue("queue"));

    // unsupported types will be ignored
    properties.putMap("unsupported-map", JCSMPFactory.onlyInstance().createMap());
    properties.putStream("unsupported-stream", JCSMPFactory.onlyInstance().createStream());

    message.setProperties(properties);

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    properties.remove("unsupported-map");
    properties.remove("unsupported-stream");
    assertEquals(properties.keySet(), record.getUserProperties().keySet());
  }

  @Test
  public void testMapWithEmptyMessageUserProperties() {
    BytesXMLMessage message = JCSMPFactory.onlyInstance().createBytesXMLMessage();
    message.setApplicationMessageId("id");

    Record record = Solace.SolaceRecordMapper.toRecord(message);

    assertTrue(record.getUserProperties().isEmpty());
  }

  @Test
  public void testMapTextRecord() {
    Record record =
        Record.builder().setMessageId("id").setText("héllo").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertTrue(msg instanceof TextMessage);
    assertEquals("héllo", ((TextMessage) msg).getText());
  }

  @Test
  public void testMapTextRecordWithEmptyText() {
    Record record = Record.builder().setMessageId("id").setText("").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertTrue(msg instanceof TextMessage);
    assertEquals("", ((TextMessage) msg).getText());
  }

  @Test
  public void testMapBytesRecord() {
    byte[] payload = new byte[] {0, 1, (byte) 255};
    Record record =
        Record.builder()
            .setMessageId("id")
            .setPayload(payload)
            .setPayloadType(PayloadType.BYTES)
            .setSenderTimestamp(1L)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertTrue(msg instanceof BytesMessage);
    assertArrayEquals(payload, ((BytesMessage) msg).getData());
  }

  @Test
  public void testMapBytesXmlRecord() {
    byte[] payload = new byte[] {1, 2, 3};
    Record record =
        Record.builder().setMessageId("id").setPayload(payload).setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertArrayEquals(payload, Arrays.copyOf(msg.getBytes(), msg.getContentLength()));
  }

  @Test
  public void testMapBytesXmlRecordWithAttachment() {
    byte[] payload = new byte[] {1, 2};
    byte[] attachment = new byte[] {3, 4};
    Record record =
        Record.builder()
            .setMessageId("id")
            .setPayload(payload)
            .setAttachmentBytes(attachment)
            .setSenderTimestamp(1L)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertEquals(attachment.length, msg.getAttachmentContentLength());
    assertArrayEquals(attachment, msg.getAttachmentByteBuffer().array());
  }

  @Test
  public void testMapBytesXmlRecordWithEmptyAttachment() {
    Record record =
        Record.builder()
            .setMessageId("id")
            .setPayload(new byte[] {1})
            .setSenderTimestamp(1L)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertEquals(0, msg.getAttachmentContentLength());
  }

  @Test
  public void testMapRecordMetadata() {
    Record record =
        Record.builder().setMessageId("id").setText("hello").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertEquals("id", msg.getApplicationMessageId());
    assertEquals(Long.valueOf(1L), Long.valueOf(msg.getSenderTimestamp()));
  }

  @Test
  public void testToMessageDefaultsSenderTimestamp() {
    Record record = Record.builder().setMessageId("id").setText("hello").build();

    long before = System.currentTimeMillis();
    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);
    long after = System.currentTimeMillis();

    assertTrue(msg.getSenderTimestamp() >= before && msg.getSenderTimestamp() <= after);
  }

  @Test
  public void testToMessageDoesNotSetPublishingFields() {
    Record record =
        Record.builder().setMessageId("id").setText("hello").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertNull(msg.getCorrelationKey());
  }

  @Test
  public void testMapRecordUserProperties() throws Exception {
    Map<String, Solace.UserPropertyValue> userProperties = new HashMap<>();
    userProperties.put("string", Solace.UserPropertyValue.of("value"));
    userProperties.put("boolean", Solace.UserPropertyValue.of(true));
    userProperties.put("double", Solace.UserPropertyValue.of(1.23));
    userProperties.put("float", Solace.UserPropertyValue.of(4.56f));
    userProperties.put("long", Solace.UserPropertyValue.of(123456789L));
    userProperties.put("integer", Solace.UserPropertyValue.of(3));
    userProperties.put("short", Solace.UserPropertyValue.of((short) 123));
    userProperties.put("character", Solace.UserPropertyValue.of('c'));
    userProperties.put("byte", Solace.UserPropertyValue.of((byte) 1));
    userProperties.put("bytes", Solace.UserPropertyValue.of(Bytes.asList(new byte[] {1, 2, 3})));
    userProperties.put(
        "topic",
        Solace.UserPropertyValue.of(
            Solace.Destination.builder()
                .setType(Solace.DestinationType.TOPIC)
                .setName("topic")
                .build()));
    userProperties.put(
        "queue",
        Solace.UserPropertyValue.of(
            Solace.Destination.builder()
                .setType(Solace.DestinationType.QUEUE)
                .setName("queue")
                .build()));

    Record record =
        Record.builder()
            .setMessageId("id")
            .setText("test")
            .setUserProperties(userProperties)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    SDTMap expected = JCSMPFactory.onlyInstance().createMap();
    expected.putString("string", "value");
    expected.putBoolean("boolean", true);
    expected.putDouble("double", 1.23);
    expected.putFloat("float", 4.56f);
    expected.putLong("long", 123456789L);
    expected.putInteger("integer", 3);
    expected.putShort("short", (short) 123);
    expected.putCharacter("character", 'c');
    expected.putByte("byte", (byte) 1);
    expected.putBytes("bytes", new byte[] {1, 2, 3});
    expected.putDestination("topic", JCSMPFactory.onlyInstance().createTopic("topic"));
    expected.putDestination("queue", JCSMPFactory.onlyInstance().createQueue("queue"));

    assertEquals(expected, msg.getProperties());
  }

  @Test
  public void testMapWithEmptyRecordUserProperties() {
    Record record =
        Record.builder().setMessageId("id").setText("hello").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(record);

    assertNull(msg.getProperties());
  }

  // ---------------------------------------------------------------------------
  // round-trip
  // ---------------------------------------------------------------------------
  @Test
  public void testRoundTripTextPayload() {
    Record original =
        Record.builder().setMessageId("id").setText("héllo").setSenderTimestamp(1L).build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(original);
    msg.setApplicationMessageId("id");
    Record decoded = Solace.SolaceRecordMapper.toRecord(msg);

    assertEquals(original.getPayloadType(), decoded.getPayloadType());
    assertArrayEquals(original.getPayload(), decoded.getPayload());
  }

  @Test
  public void testRoundTripBytesPayload() {
    byte[] payload = new byte[] {10, 20, 30};
    Record original =
        Record.builder()
            .setMessageId("id")
            .setPayload(payload)
            .setPayloadType(PayloadType.BYTES)
            .setSenderTimestamp(1L)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(original);
    msg.setApplicationMessageId("id");
    Record decoded = Solace.SolaceRecordMapper.toRecord(msg);

    assertEquals(PayloadType.BYTES, decoded.getPayloadType());
    assertArrayEquals(payload, decoded.getPayload());
  }

  @Test
  public void testRoundTripBytesXmlPayloadWithAttachment() {
    Record original =
        Record.builder()
            .setMessageId("id")
            .setPayload(new byte[] {1, 2})
            .setAttachmentBytes(new byte[] {3, 4})
            .setSenderTimestamp(1L)
            .build();

    BytesXMLMessage msg = Solace.SolaceRecordMapper.toMessage(original);
    msg.setApplicationMessageId("id");
    Record decoded = Solace.SolaceRecordMapper.toRecord(msg);

    assertEquals(PayloadType.BYTES_XML, decoded.getPayloadType());
    assertArrayEquals(new byte[] {1, 2}, Arrays.copyOf(decoded.getPayload(), 2));
    assertArrayEquals(new byte[] {3, 4}, decoded.getAttachmentBytes());
  }

  private static void assertUserPropertiesEqual(
      SDTMap expected, Map<String, Solace.UserPropertyValue> actual) throws SDTException {
    assertEquals(expected.keySet(), actual.keySet());
    for (String key : expected.keySet()) {
      Object expectedValue = expected.get(key);
      Object actualValue = actual.get(key);
      if (expectedValue instanceof byte[]) {
        assertArrayEquals((byte[]) expectedValue, (byte[]) actualValue);
      } else {
        assertEquals(expectedValue, actualValue);
      }
    }
  }
}

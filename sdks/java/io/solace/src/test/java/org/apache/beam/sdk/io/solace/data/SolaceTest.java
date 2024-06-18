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

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.solace.data.Solace.Destination;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class SolaceTest {

  Destination destination =
      Solace.Destination.builder()
          .setName("some destination")
          .setType(Solace.DestinationType.TOPIC)
          .build();
  String messageId = "some message id";
  long expiration = 123L;
  int priority = 7;
  Boolean redelivered = true;
  Destination replyTo =
      Solace.Destination.builder()
          .setName("some reply-to destination")
          .setType(Solace.DestinationType.TOPIC)
          .build();
  long receiveTimestamp = 123456789L;
  Long senderTimestamp = 987654321L;
  Long sequenceNumber = 27L;
  long timeToLive = 34567890L;
  String payloadString = "some payload";
  ByteString payload = ByteString.copyFrom(payloadString, StandardCharsets.UTF_8);
  String attachmentString = "some attachment";
  ByteString attachment = ByteString.copyFrom(attachmentString, StandardCharsets.UTF_8);

  @Test
  public void testRecordEquality() {
    Solace.Record obj1 =
        Solace.Record.builder()
            .setDestination(destination)
            .setMessageId(messageId)
            .setExpiration(expiration)
            .setPriority(priority)
            .setRedelivered(redelivered)
            .setReplyTo(replyTo)
            .setReceiveTimestamp(receiveTimestamp)
            .setSenderTimestamp(senderTimestamp)
            .setSequenceNumber(sequenceNumber)
            .setTimeToLive(timeToLive)
            .setPayload(payload)
            .setAttachmentBytes(attachment)
            .build();

    Solace.Record obj2 =
        Solace.Record.builder()
            .setDestination(destination)
            .setMessageId(messageId)
            .setExpiration(expiration)
            .setPriority(priority)
            .setRedelivered(redelivered)
            .setReplyTo(replyTo)
            .setReceiveTimestamp(receiveTimestamp)
            .setSenderTimestamp(senderTimestamp)
            .setSequenceNumber(sequenceNumber)
            .setTimeToLive(timeToLive)
            .setPayload(payload)
            .setAttachmentBytes(attachment)
            .build();

    Solace.Record obj3 =
        Solace.Record.builder()
            .setDestination(destination)
            .setMessageId(messageId)
            .setExpiration(expiration)
            .setPriority(priority)
            .setRedelivered(!redelivered)
            .setReplyTo(replyTo)
            .setReceiveTimestamp(receiveTimestamp)
            .setSenderTimestamp(senderTimestamp)
            .setSequenceNumber(sequenceNumber)
            .setTimeToLive(timeToLive)
            .setPayload(payload)
            .setAttachmentBytes(attachment)
            .build();

    Assert.assertEquals(obj1, obj2);
    Assert.assertNotEquals(obj1, obj3);
    Assert.assertEquals(obj1.hashCode(), obj2.hashCode());
    Assert.assertEquals(obj1.getDestination(), destination);
    Assert.assertEquals(obj1.getMessageId(), messageId);
    Assert.assertEquals(obj1.getExpiration(), expiration);
    Assert.assertEquals(obj1.getPriority(), priority);
    Assert.assertEquals(obj1.getRedelivered(), redelivered);
    Assert.assertEquals(obj1.getReplyTo(), replyTo);
    Assert.assertEquals(obj1.getReceiveTimestamp(), receiveTimestamp);
    Assert.assertEquals(obj1.getSenderTimestamp(), senderTimestamp);
    Assert.assertEquals(obj1.getSequenceNumber(), sequenceNumber);
    Assert.assertEquals(obj1.getTimeToLive(), timeToLive);
    Assert.assertEquals(obj1.getPayload().toString(StandardCharsets.UTF_8), payloadString);
    Assert.assertEquals(
        obj1.getAttachmentBytes().toString(StandardCharsets.UTF_8), attachmentString);
  }

  @Test
  public void testRecordNullability() {
    Solace.Record obj =
        Solace.Record.builder()
            .setMessageId(messageId)
            .setPayload(payload)
            .setExpiration(0L)
            .setPriority(1)
            .setRedelivered(false)
            .setReceiveTimestamp(1234567L)
            .setTimeToLive(111L)
            .setAttachmentBytes(ByteString.EMPTY)
            .build();
    Assert.assertNotNull(obj);
    Assert.assertNull(obj.getDestination());
    Assert.assertEquals(obj.getMessageId(), messageId);
    Assert.assertNull(obj.getReplyTo());
    Assert.assertNull(obj.getSenderTimestamp());
    Assert.assertNull(obj.getSequenceNumber());
    Assert.assertTrue(obj.getAttachmentBytes().isEmpty());
    Assert.assertEquals(obj.getPayload().toString(StandardCharsets.UTF_8), payloadString);
  }

  @Test(expected = IllegalStateException.class)
  public void testRecordBuilder() {
    Solace.Record.builder().build();
  }
}

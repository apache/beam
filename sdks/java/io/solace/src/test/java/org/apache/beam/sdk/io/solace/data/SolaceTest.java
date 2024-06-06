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
import java.util.Map;
import org.apache.beam.sdk.io.solace.data.Solace.Destination;
import org.junit.Assert;
import org.junit.Test;

public class SolaceTest {

  Map<String, Object> properties;
  Destination destination =
      Solace.Destination.builder()
          .setName("some destination")
          .setType(Solace.DestinationType.TOPIC)
          .build();
  String messageId = "some message id";
  Long expiration = 123L;
  Integer priority = 7;
  Boolean redelivered = true;
  String replyTo = "no-one";
  Long receiveTimestamp = 123456789L;
  Long senderTimestamp = 987654321L;
  Long sequenceNumber = 27L;
  Long timeToLive = 34567890L;
  String payloadString = "some payload";
  byte[] payload = payloadString.getBytes(StandardCharsets.UTF_8);

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
    Assert.assertEquals(new String(obj1.getPayload(), StandardCharsets.UTF_8), payloadString);
  }

  @Test
  public void testRecordNullability() {
    Solace.Record obj = Solace.Record.builder().setMessageId(messageId).setPayload(payload).build();
    Assert.assertNotNull(obj);
    Assert.assertNull(obj.getDestination());
    Assert.assertEquals(obj.getMessageId(), messageId);
    Assert.assertNull(obj.getExpiration());
    Assert.assertNull(obj.getPriority());
    Assert.assertNull(obj.getRedelivered());
    Assert.assertNull(obj.getReplyTo());
    Assert.assertNull(obj.getReceiveTimestamp());
    Assert.assertNull(obj.getSenderTimestamp());
    Assert.assertNull(obj.getSequenceNumber());
    Assert.assertNull(obj.getTimeToLive());
    Assert.assertEquals(new String(obj.getPayload(), StandardCharsets.UTF_8), payloadString);
  }

  @Test(expected = IllegalStateException.class)
  public void testRecordBuilder() {
    Solace.Record.builder().build();
  }
}

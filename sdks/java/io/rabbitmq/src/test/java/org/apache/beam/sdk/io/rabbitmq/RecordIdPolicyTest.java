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
package org.apache.beam.sdk.io.rabbitmq;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RecordIdPolicyTest {
  final AtomicInteger uniqueId = new AtomicInteger(1);

  RabbitMqMessage newMessage(int id, AMQP.BasicProperties properties) {
    byte[] body = ("body" + id).getBytes(StandardCharsets.UTF_8);
    GetResponse resp = new GetResponse(null, properties, body, 0);
    return new RabbitMqMessage(resp);
  }

  @Test
  public void testCorrelationIdPropertyPolicy() {
    int id1 = uniqueId.incrementAndGet();
    RabbitMqMessage msg1 =
        newMessage(
            id1, new AMQP.BasicProperties().builder().correlationId("correlationId" + id1).build());
    int id2 = uniqueId.incrementAndGet();
    RabbitMqMessage msg2 =
        newMessage(
            id2, new AMQP.BasicProperties().builder().correlationId("correlationId" + id2).build());
    RecordIdPolicy policy = RecordIdPolicy.correlationId();
    byte[] result1 = policy.apply(msg1);
    byte[] result1Again = policy.apply(msg1);
    byte[] result2 = policy.apply(msg2);
    assertFalse(Arrays.equals(result1, result2));
    assertArrayEquals(result1, result1Again);
  }

  @Test
  public void testMessageIdPropertyPolicy() {
    int id1 = uniqueId.incrementAndGet();
    RabbitMqMessage msg1 =
        newMessage(id1, new AMQP.BasicProperties().builder().messageId("messageId" + id1).build());
    int id2 = uniqueId.incrementAndGet();
    RabbitMqMessage msg2 =
        newMessage(id2, new AMQP.BasicProperties().builder().messageId("messageId" + id2).build());
    RecordIdPolicy policy = RecordIdPolicy.messageId();
    byte[] result1 = policy.apply(msg1);
    byte[] result1Again = policy.apply(msg1);
    byte[] result2 = policy.apply(msg2);
    assertFalse(Arrays.equals(result1, result2));
    assertArrayEquals(result1, result1Again);
  }

  @Test
  public void testBodyPropertyPolicy() {
    RabbitMqMessage msg1 = new RabbitMqMessage("body1".getBytes(StandardCharsets.UTF_8));
    RabbitMqMessage msg2 = new RabbitMqMessage("body2".getBytes(StandardCharsets.UTF_8));

    RecordIdPolicy policy = RecordIdPolicy.body();
    byte[] result1 = policy.apply(msg1);
    byte[] result1Again = policy.apply(msg1);
    byte[] result2 = policy.apply(msg2);
    assertFalse(Arrays.equals(result1, result2));
    assertArrayEquals(result1, result1Again);
    assertArrayEquals(result1, msg1.getBody());
  }

  @Test
  public void testBodySha256PropertyPolicy() {
    RabbitMqMessage msg1 = new RabbitMqMessage("body1".getBytes(StandardCharsets.UTF_8));
    RabbitMqMessage msg2 = new RabbitMqMessage("body2".getBytes(StandardCharsets.UTF_8));

    RecordIdPolicy policy = RecordIdPolicy.body();
    byte[] result1 = policy.apply(msg1);
    byte[] result1Again = policy.apply(msg1);
    byte[] result2 = policy.apply(msg2);
    assertFalse(Arrays.equals(result1, result2));
    assertArrayEquals(result1, result1Again);
  }

  @Test
  public void testAlwaysUniquePropertyPolicy() {
    RecordIdPolicy policy = RecordIdPolicy.body();
    RabbitMqMessage msg = new RabbitMqMessage("body".getBytes(StandardCharsets.UTF_8));
    byte[] prev = policy.apply(msg);
    for (int i = 0; i < 1000; i++) {
      byte[] current = policy.apply(msg);
      assertFalse(Arrays.equals(current, prev));
    }
  }
}

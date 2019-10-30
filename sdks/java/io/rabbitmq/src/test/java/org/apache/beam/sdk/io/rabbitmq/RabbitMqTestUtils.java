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

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RabbitMqTestUtils {
  private RabbitMqTestUtils() {
    throw new UnsupportedOperationException(
        "RabbitMqTestUtils is a non-instantiable utility class");
  }

  public static byte[] generateRecord(int recordNum) {
    return ("Test " + recordNum).getBytes(StandardCharsets.UTF_8);
  }

  public static List<byte[]> generateRecords(int maxNumRecords) {
    return IntStream.range(0, maxNumRecords)
        .mapToObj(RabbitMqTestUtils::generateRecord)
        .collect(Collectors.toList());
  }

  public static String recordToString(byte[] record) {
    return new String(record, StandardCharsets.UTF_8);
  }

  static class TestConsumer extends DefaultConsumer {

    private final List<String> received;

    public TestConsumer(Channel channel, List<String> received) {
      super(channel);
      this.received = received;
    }

    @Override
    public void handleDelivery(
        String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
        throws IOException {
      received.add(recordToString(body));
    }
  }
}

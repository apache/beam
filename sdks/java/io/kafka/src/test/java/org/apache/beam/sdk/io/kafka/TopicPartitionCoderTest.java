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
package org.apache.beam.sdk.io.kafka;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

public class TopicPartitionCoderTest {

  @Test
  public void testEncodeDecodeRoundTrip() throws Exception {
    TopicPartitionCoder coder = new TopicPartitionCoder();
    TopicPartition topicPartition = new TopicPartition("topic", 1);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    coder.encode(topicPartition, outputStream);
    assertEquals(
        topicPartition, coder.decode(new ByteArrayInputStream(outputStream.toByteArray())));
  }

  @Test
  public void testToString() throws Exception {
    TopicPartitionCoder coder = new TopicPartitionCoder();
    assertEquals("TopicPartitionCoder(StringUtf8Coder,VarIntCoder)", coder.toString());
  }
}

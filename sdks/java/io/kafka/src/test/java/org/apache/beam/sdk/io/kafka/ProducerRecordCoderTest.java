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
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProducerRecordCoder}. */
@RunWith(JUnit4.class)
public class ProducerRecordCoderTest {
  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        ProducerRecordCoder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testProducerRecordSerializableWithHeaders() throws IOException {
    RecordHeaders headers = new RecordHeaders();
    headers.add("headerKey", "headerVal".getBytes(StandardCharsets.UTF_8));
    verifySerialization(headers, 0, System.currentTimeMillis());
  }

  @Test
  public void testProducerRecordSerializableWithoutHeaders() throws IOException {
    ConsumerRecord consumerRecord = new ConsumerRecord<>("", 0, 0L, "", "");
    verifySerialization(consumerRecord.headers(), 0, System.currentTimeMillis());
  }

  @Test
  public void testProducerRecordSerializableWithPartition() throws IOException {
    ProducerRecord<String, String> decodedRecord =
        verifySerialization(1, System.currentTimeMillis());
    assertEquals(1, decodedRecord.partition().intValue());
  }

  @Test
  public void testProducerRecordSerializableWithoutPartition() throws IOException {
    ProducerRecord<String, String> decodedRecord =
        verifySerialization(null, System.currentTimeMillis());
    assertNull(decodedRecord.partition());
  }

  @Test
  public void testProducerRecordSerializableWithTimestamp() throws IOException {
    long timestamp = System.currentTimeMillis();
    ProducerRecord<String, String> decodedRecord = verifySerialization(1, timestamp);
    assertEquals(timestamp, decodedRecord.timestamp().longValue());
  }

  @Test
  public void testProducerRecordSerializableWithoutTimestamp() throws IOException {
    ProducerRecord<String, String> decodedRecord = verifySerialization(1, null);
    assertNull(decodedRecord.timestamp());
  }

  private ProducerRecord<String, String> verifySerialization(Integer partition, Long timestamp)
      throws IOException {
    return verifySerialization(null, partition, timestamp);
  }

  private ProducerRecord<String, String> verifySerialization(
      Headers headers, Integer partition, Long timestamp) throws IOException {
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("topic", partition, timestamp, "key", "value", headers);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ProducerRecordCoder producerRecordCoder =
        ProducerRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    producerRecordCoder.encode(producerRecord, outputStream);
    ProducerRecord<String, String> decodedRecord =
        producerRecordCoder.decode(new ByteArrayInputStream(outputStream.toByteArray()));

    assertEquals(producerRecord, decodedRecord);

    return decodedRecord;
  }
}

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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link KafkaRecordCoder}. */
@RunWith(JUnit4.class)
public class KafkaRecordCoderTest {
  @Test
  public void testCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(
        KafkaRecordCoder.of(GlobalWindow.Coder.INSTANCE, GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testKafkaRecordSerializableWithHeaders() throws IOException {
    RecordHeaders headers = new RecordHeaders();
    headers.add("headerKey", "headerVal".getBytes(StandardCharsets.UTF_8));
    verifySerialization(headers);
  }

  @Test
  public void testKafkaRecordSerializableWithoutHeaders() throws IOException {
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>("", 0, 0L, "", "");
    verifySerialization(consumerRecord.headers());
  }

  @Test
  public void testKafkaRecordSerializableWithNullValueHeader() throws IOException {
    RecordHeaders headers = new RecordHeaders();
    headers.add("headerKey", null);
    verifySerialization(headers);
  }

  private void verifySerialization(Headers headers) throws IOException {
    KafkaRecord<String, String> kafkaRecord =
        new KafkaRecord<>(
            "topic", 0, 0, 0, KafkaTimestampType.CREATE_TIME, headers, "key", "value");

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    KafkaRecordCoder<String, String> kafkaRecordCoder =
        KafkaRecordCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

    kafkaRecordCoder.encode(kafkaRecord, outputStream);
    KafkaRecord<String, String> decodedRecord =
        kafkaRecordCoder.decode(new ByteArrayInputStream(outputStream.toByteArray()));

    assertEquals(kafkaRecord, decodedRecord);
  }
}

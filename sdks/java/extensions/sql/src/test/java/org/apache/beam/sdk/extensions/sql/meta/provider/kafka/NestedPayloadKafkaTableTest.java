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
package org.apache.beam.sdk.extensions.sql.meta.provider.kafka;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.MockitoAnnotations.openMocks;

import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaTimestampType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ListMultimap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class NestedPayloadKafkaTableTest {
  private static final String TOPIC = "mytopic";
  private static final Schema FULL_WRITE_SCHEMA =
      Schema.builder()
          .addByteArrayField(Schemas.MESSAGE_KEY_FIELD)
          .addField(Schemas.EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
          .addArrayField(Schemas.HEADERS_FIELD, FieldType.row(Schemas.HEADERS_ENTRY_SCHEMA))
          .addByteArrayField(Schemas.PAYLOAD_FIELD)
          .build();
  private static final Schema FULL_READ_SCHEMA =
      Schema.builder()
          .addByteArrayField(Schemas.MESSAGE_KEY_FIELD)
          .addDateTimeField(Schemas.EVENT_TIMESTAMP_FIELD)
          .addArrayField(Schemas.HEADERS_FIELD, FieldType.row(Schemas.HEADERS_ENTRY_SCHEMA))
          .addByteArrayField(Schemas.PAYLOAD_FIELD)
          .build();

  @Mock public PayloadSerializer serializer;

  @Before
  public void setUp() {
    openMocks(this);
  }

  private NestedPayloadKafkaTable newTable(Schema schema, Optional<PayloadSerializer> serializer) {
    return new NestedPayloadKafkaTable(
        schema, "abc.bootstrap", ImmutableList.of(TOPIC), serializer);
  }

  @Test
  public void constructionFailures() {
    // Not nested schema (no headers)
    assertThrows(
        IllegalArgumentException.class,
        () ->
            newTable(
                Schema.builder().addByteArrayField(Schemas.PAYLOAD_FIELD).build(),
                Optional.empty()));
    // Row payload without serializer
    assertThrows(
        IllegalArgumentException.class,
        () ->
            newTable(
                Schema.builder()
                    .addRowField(
                        Schemas.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                    .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                    .build(),
                Optional.empty()));
    // Bytes payload with serializer
    assertThrows(
        IllegalArgumentException.class,
        () ->
            newTable(
                Schema.builder()
                    .addByteArrayField(Schemas.PAYLOAD_FIELD)
                    .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                    .build(),
                Optional.of(serializer)));
    // Bad field in schema
    assertThrows(
        IllegalArgumentException.class,
        () ->
            newTable(
                Schema.builder()
                    .addByteArrayField(Schemas.PAYLOAD_FIELD)
                    .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                    .addBooleanField("bad")
                    .build(),
                Optional.empty()));
    // Bad field type in schema
    assertThrows(
        IllegalArgumentException.class,
        () ->
            newTable(
                Schema.builder()
                    .addByteArrayField(Schemas.PAYLOAD_FIELD)
                    .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                    .addBooleanField(Schemas.EVENT_TIMESTAMP_FIELD)
                    .build(),
                Optional.empty()));
  }

  private static KafkaRecord<byte[], byte[]> readRecord(
      byte[] key, byte[] value, long timestamp, ListMultimap<String, byte[]> attributes) {
    Headers headers = new RecordHeaders();
    attributes.forEach(headers::add);
    return new KafkaRecord<>(
        TOPIC, 0, 0, timestamp, KafkaTimestampType.LOG_APPEND_TIME, headers, key, value);
  }

  @Test
  public void recordToRowFailures() {
    {
      Schema payloadSchema = Schema.builder().addStringField("def").build();
      NestedPayloadKafkaTable table =
          newTable(
              Schema.builder()
                  .addRowField(Schemas.PAYLOAD_FIELD, payloadSchema)
                  .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                  .build(),
              Optional.of(serializer));
      doThrow(new IllegalArgumentException("")).when(serializer).deserialize(any());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              table.transformInput(
                  readRecord(
                      new byte[] {}, "abc".getBytes(UTF_8), 123, ImmutableListMultimap.of())));
    }
    // Schema requires headers, missing in message
    {
      NestedPayloadKafkaTable table =
          newTable(
              Schema.builder()
                  .addByteArrayField(Schemas.PAYLOAD_FIELD)
                  .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
                  .build(),
              Optional.empty());
      assertThrows(
          IllegalArgumentException.class,
          () ->
              table.transformInput(
                  new KafkaRecord<>(
                      TOPIC,
                      0,
                      0,
                      0,
                      KafkaTimestampType.LOG_APPEND_TIME,
                      null,
                      new byte[] {},
                      new byte[] {})));
    }
  }

  @Test
  public void rowToRecordFailures() {
    Schema payloadSchema = Schema.builder().addStringField("def").build();
    Schema schema =
        Schema.builder()
            .addRowField(Schemas.PAYLOAD_FIELD, payloadSchema)
            .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE.withNullable(true))
            .build();
    NestedPayloadKafkaTable table = newTable(schema, Optional.of(serializer));
    // badRow cannot be cast to schema
    Schema badRowSchema = Schema.builder().addStringField("xxx").build();
    Row badRow =
        Row.withSchema(badRowSchema).attachValues(Row.withSchema(badRowSchema).attachValues("abc"));
    assertThrows(IllegalArgumentException.class, () -> table.transformOutput(badRow));

    Row goodRow =
        Row.withSchema(schema)
            .withFieldValue(
                Schemas.PAYLOAD_FIELD,
                Row.withSchema(payloadSchema).withFieldValue("def", "abc").build())
            .build();
    doThrow(new IllegalArgumentException("")).when(serializer).serialize(any());
    assertThrows(IllegalArgumentException.class, () -> table.transformOutput(goodRow));
  }

  @Test
  public void reorderRowToRecord() {
    Schema schema =
        Schema.builder()
            .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
            .addByteArrayField(Schemas.PAYLOAD_FIELD)
            .build();
    Schema rowSchema =
        Schema.builder()
            .addByteArrayField(Schemas.PAYLOAD_FIELD)
            .addField(Schemas.HEADERS_FIELD, Schemas.HEADERS_FIELD_TYPE)
            .build();
    NestedPayloadKafkaTable table = newTable(schema, Optional.empty());
    Row row = Row.withSchema(rowSchema).attachValues("abc".getBytes(UTF_8), ImmutableList.of());
    ProducerRecord<byte[], byte[]> output = table.transformOutput(row);
    assertEquals("abc", new String(output.value(), UTF_8));
    assertEquals(0, output.headers().toArray().length);
  }

  @Test
  public void fullRowToRecord() {
    NestedPayloadKafkaTable table = newTable(FULL_WRITE_SCHEMA, Optional.empty());
    Instant now = Instant.now();
    Row row =
        Row.withSchema(FULL_WRITE_SCHEMA)
            .withFieldValue(Schemas.MESSAGE_KEY_FIELD, "val1".getBytes(UTF_8))
            .withFieldValue(Schemas.PAYLOAD_FIELD, "val2".getBytes(UTF_8))
            .withFieldValue(Schemas.EVENT_TIMESTAMP_FIELD, now)
            .withFieldValue(
                Schemas.HEADERS_FIELD,
                ImmutableList.of(
                    Row.withSchema(Schemas.HEADERS_ENTRY_SCHEMA)
                        .attachValues(
                            "key1",
                            ImmutableList.of("attr1".getBytes(UTF_8), "attr2".getBytes(UTF_8))),
                    Row.withSchema(Schemas.HEADERS_ENTRY_SCHEMA)
                        .attachValues("key2", ImmutableList.of("attr3".getBytes(UTF_8)))))
            .build();
    ProducerRecord<byte[], byte[]> result = table.transformOutput(row);
    assertEquals("val1", new String(result.key(), UTF_8));
    assertEquals("val2", new String(result.value(), UTF_8));
    assertEquals(now.getMillis(), result.timestamp().longValue());
    List<Header> key1Headers = ImmutableList.copyOf(result.headers().headers("key1"));
    List<Header> key2Headers = ImmutableList.copyOf(result.headers().headers("key2"));
    assertEquals(2, key1Headers.size());
    assertEquals(1, key2Headers.size());
    assertEquals("attr3", new String(key2Headers.get(0).value(), UTF_8));
  }

  @Test
  public void fullRecordToRow() {
    NestedPayloadKafkaTable table = newTable(FULL_READ_SCHEMA, Optional.empty());
    Instant event = Instant.now();
    KafkaRecord<byte[], byte[]> record =
        readRecord(
            "key".getBytes(UTF_8),
            "value".getBytes(UTF_8),
            event.getMillis(),
            ImmutableListMultimap.of(
                "key1", "attr1".getBytes(UTF_8),
                "key1", "attr2".getBytes(UTF_8),
                "key2", "attr3".getBytes(UTF_8)));
    Row expected =
        Row.withSchema(FULL_READ_SCHEMA)
            .withFieldValue(Schemas.MESSAGE_KEY_FIELD, "key".getBytes(UTF_8))
            .withFieldValue(Schemas.PAYLOAD_FIELD, "value".getBytes(UTF_8))
            .withFieldValue(Schemas.EVENT_TIMESTAMP_FIELD, event)
            .withFieldValue(
                Schemas.HEADERS_FIELD,
                ImmutableList.of(
                    Row.withSchema(Schemas.HEADERS_ENTRY_SCHEMA)
                        .attachValues(
                            "key1",
                            ImmutableList.of("attr1".getBytes(UTF_8), "attr2".getBytes(UTF_8))),
                    Row.withSchema(Schemas.HEADERS_ENTRY_SCHEMA)
                        .attachValues("key2", ImmutableList.of("attr3".getBytes(UTF_8)))))
            .build();
    assertEquals(expected, table.transformInput(record));
  }
}

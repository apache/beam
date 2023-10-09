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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsublite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.MockitoAnnotations.openMocks;

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class RowHandlerTest {
  private static final Schema FULL_WRITE_SCHEMA =
      Schema.builder()
          .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
          .addField(RowHandler.EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
          .addArrayField(
              RowHandler.ATTRIBUTES_FIELD, FieldType.row(RowHandler.ATTRIBUTES_ENTRY_SCHEMA))
          .addByteArrayField(RowHandler.PAYLOAD_FIELD)
          .build();
  private static final Schema FULL_READ_SCHEMA =
      Schema.builder()
          .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
          .addField(RowHandler.EVENT_TIMESTAMP_FIELD, FieldType.DATETIME.withNullable(true))
          .addArrayField(
              RowHandler.ATTRIBUTES_FIELD, FieldType.row(RowHandler.ATTRIBUTES_ENTRY_SCHEMA))
          .addByteArrayField(RowHandler.PAYLOAD_FIELD)
          .addDateTimeField(RowHandler.PUBLISH_TIMESTAMP_FIELD)
          .build();

  @Mock public PayloadSerializer serializer;

  @Before
  public void setUp() {
    openMocks(this);
  }

  @Test
  public void constructionFailures() {
    // Row payload without serializer
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RowHandler(
                Schema.builder()
                    .addRowField(
                        RowHandler.PAYLOAD_FIELD, Schema.builder().addStringField("abc").build())
                    .build()));
    // Bytes payload with serializer
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new RowHandler(
                Schema.builder().addByteArrayField(RowHandler.PAYLOAD_FIELD).build(), serializer));
  }

  @Test
  public void messageToRowFailures() {
    {
      Schema payloadSchema = Schema.builder().addStringField("def").build();
      RowHandler rowHandler =
          new RowHandler(
              Schema.builder().addRowField(RowHandler.PAYLOAD_FIELD, payloadSchema).build(),
              serializer);
      doThrow(new IllegalArgumentException("")).when(serializer).deserialize(any());
      assertThrows(
          IllegalArgumentException.class,
          () -> rowHandler.messageToRow(SequencedMessage.getDefaultInstance()));
    }
    // Schema requires event time, missing in message
    {
      RowHandler rowHandler =
          new RowHandler(
              Schema.builder()
                  .addByteArrayField(RowHandler.PAYLOAD_FIELD)
                  .addDateTimeField(RowHandler.EVENT_TIMESTAMP_FIELD)
                  .build());
      assertThrows(
          IllegalArgumentException.class,
          () -> rowHandler.messageToRow(SequencedMessage.getDefaultInstance()));
    }
  }

  @Test
  public void rowToMessageFailures() {
    Schema payloadSchema = Schema.builder().addStringField("def").build();
    Schema schema = Schema.builder().addRowField(RowHandler.PAYLOAD_FIELD, payloadSchema).build();
    RowHandler rowHandler = new RowHandler(schema, serializer);
    // badRow cannot be cast to schema
    Schema badRowSchema = Schema.builder().addStringField("xxx").build();
    Row badRow =
        Row.withSchema(badRowSchema).attachValues(Row.withSchema(badRowSchema).attachValues("abc"));
    assertThrows(IllegalArgumentException.class, () -> rowHandler.rowToMessage(badRow));

    Row goodRow =
        Row.withSchema(schema).addValue(Row.withSchema(payloadSchema).attachValues("abc")).build();
    doThrow(new IllegalArgumentException("")).when(serializer).serialize(any());
    assertThrows(IllegalArgumentException.class, () -> rowHandler.rowToMessage(goodRow));
  }

  @Test
  public void reorderRowToMessage() {
    Schema schema =
        Schema.builder()
            .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
            .addByteArrayField(RowHandler.PAYLOAD_FIELD)
            .build();
    Schema rowSchema =
        Schema.builder()
            .addByteArrayField(RowHandler.PAYLOAD_FIELD)
            .addByteArrayField(RowHandler.MESSAGE_KEY_FIELD)
            .build();
    RowHandler rowHandler = new RowHandler(schema);
    Row row = Row.withSchema(rowSchema).attachValues("abc".getBytes(UTF_8), "def".getBytes(UTF_8));
    PubSubMessage expected =
        PubSubMessage.newBuilder()
            .setData(ByteString.copyFromUtf8("abc"))
            .setKey(ByteString.copyFromUtf8("def"))
            .build();
    assertEquals(expected, rowHandler.rowToMessage(row));
  }

  @Test
  public void fullRowToMessage() {
    RowHandler rowHandler = new RowHandler(FULL_WRITE_SCHEMA);
    Instant now = Instant.now();
    Row row =
        Row.withSchema(FULL_WRITE_SCHEMA)
            .withFieldValue(RowHandler.MESSAGE_KEY_FIELD, "val1".getBytes(UTF_8))
            .withFieldValue(RowHandler.PAYLOAD_FIELD, "val2".getBytes(UTF_8))
            .withFieldValue(RowHandler.EVENT_TIMESTAMP_FIELD, now)
            .withFieldValue(
                RowHandler.ATTRIBUTES_FIELD,
                ImmutableList.of(
                    Row.withSchema(RowHandler.ATTRIBUTES_ENTRY_SCHEMA)
                        .attachValues(
                            "key1",
                            ImmutableList.of("attr1".getBytes(UTF_8), "attr2".getBytes(UTF_8))),
                    Row.withSchema(RowHandler.ATTRIBUTES_ENTRY_SCHEMA)
                        .attachValues("key2", ImmutableList.of("attr3".getBytes(UTF_8)))))
            .build();
    PubSubMessage expected =
        PubSubMessage.newBuilder()
            .setKey(ByteString.copyFromUtf8("val1"))
            .setData(ByteString.copyFromUtf8("val2"))
            .setEventTime(Timestamps.fromMillis(now.getMillis()))
            .putAttributes(
                "key1",
                AttributeValues.newBuilder()
                    .addValues(ByteString.copyFromUtf8("attr1"))
                    .addValues(ByteString.copyFromUtf8("attr2"))
                    .build())
            .putAttributes(
                "key2",
                AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("attr3")).build())
            .build();
    assertEquals(expected, rowHandler.rowToMessage(row));
  }

  @Test
  public void fullMessageToRow() {
    RowHandler rowHandler = new RowHandler(FULL_READ_SCHEMA);
    Instant event = Instant.now();
    Instant publish = Instant.now();
    PubSubMessage userMessage =
        PubSubMessage.newBuilder()
            .setKey(ByteString.copyFromUtf8("val1"))
            .setData(ByteString.copyFromUtf8("val2"))
            .setEventTime(Timestamps.fromMillis(event.getMillis()))
            .putAttributes(
                "key1",
                AttributeValues.newBuilder()
                    .addValues(ByteString.copyFromUtf8("attr1"))
                    .addValues(ByteString.copyFromUtf8("attr2"))
                    .build())
            .putAttributes(
                "key2",
                AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("attr3")).build())
            .build();
    SequencedMessage sequencedMessage =
        SequencedMessage.newBuilder()
            .setMessage(userMessage)
            .setPublishTime(Timestamps.fromMillis(publish.getMillis()))
            .build();
    Row expected =
        Row.withSchema(FULL_READ_SCHEMA)
            .withFieldValue(RowHandler.MESSAGE_KEY_FIELD, "val1".getBytes(UTF_8))
            .withFieldValue(RowHandler.PAYLOAD_FIELD, "val2".getBytes(UTF_8))
            .withFieldValue(RowHandler.EVENT_TIMESTAMP_FIELD, event)
            .withFieldValue(RowHandler.PUBLISH_TIMESTAMP_FIELD, publish)
            .withFieldValue(
                RowHandler.ATTRIBUTES_FIELD,
                ImmutableList.of(
                    Row.withSchema(RowHandler.ATTRIBUTES_ENTRY_SCHEMA)
                        .attachValues(
                            "key1",
                            ImmutableList.of("attr1".getBytes(UTF_8), "attr2".getBytes(UTF_8))),
                    Row.withSchema(RowHandler.ATTRIBUTES_ENTRY_SCHEMA)
                        .attachValues("key2", ImmutableList.of("attr3".getBytes(UTF_8)))))
            .build();
    assertEquals(expected, rowHandler.messageToRow(sequencedMessage));
  }
}

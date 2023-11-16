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
package org.apache.beam.sdk.io.gcp.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.MAIN_TAG;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubSchemaIOProvider.ATTRIBUTE_ARRAY_ENTRY_SCHEMA;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.size;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.SerializerProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializers;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link PubsubMessageToRow}. */
public class PubsubMessageToRowTest implements Serializable {
  private static final SerializerProvider JSON_SERIALIZER_PROVIDER =
      schema -> PayloadSerializers.getSerializer("json", schema, ImmutableMap.of());

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertsMessages() {
    Schema payloadSchema =
        Schema.builder()
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addMapField("attributes", FieldType.STRING, FieldType.STRING)
            .addRowField("payload", payloadSchema)
            .build();

    PCollectionTuple rows =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(1, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                    message(2, map("bttr", "vbl"), "{ \"name\" : \"baz\", \"id\" : 5 }"),
                    message(3, map("cttr", "vcl"), "{ \"id\" : 7, \"name\" : \"bar\" }"),
                    message(4, map("dttr", "vdl"), "{ \"name\" : \"qaz\", \"id\" : 8 }"),
                    message(4, map("dttr", "vdl"), "{ \"name\" : null, \"id\" : null }")))
            .apply(
                "convert",
                PubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(false)
                    .useFlatSchema(false)
                    .serializerProvider(JSON_SERIALIZER_PROVIDER)
                    .build());

    PAssert.that(rows.get(MAIN_TAG))
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
                .addValues(ts(1), map("attr", "val"), row(payloadSchema, 3, "foo"))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(2), map("bttr", "vbl"), row(payloadSchema, 5, "baz"))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(3), map("cttr", "vcl"), row(payloadSchema, 7, "bar"))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), map("dttr", "vdl"), row(payloadSchema, 8, "qaz"))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), map("dttr", "vdl"), row(payloadSchema, null, null))
                .build());

    pipeline.run();
  }

  @Test
  public void testConvertsMessagesBytesPayloadArrayAttributes() {
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addArrayField("attributes", FieldType.row(ATTRIBUTE_ARRAY_ENTRY_SCHEMA))
            .addByteArrayField("payload")
            .build();

    PCollectionTuple rows =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(1, map("attr", "val"), "foo"),
                    message(2, map("bttr", "vbl"), "baz"),
                    message(3, map("cttr", "vcl"), "bar"),
                    message(4, map("dttr", "vdl"), "qaz")))
            .apply(
                "convert",
                PubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(false)
                    .useFlatSchema(false)
                    .build());

    PAssert.that(rows.get(MAIN_TAG))
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
                .addValues(ts(1), mapEntries("attr", "val"), "foo".getBytes(UTF_8))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(2), mapEntries("bttr", "vbl"), "baz".getBytes(UTF_8))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(3), mapEntries("cttr", "vcl"), "bar".getBytes(UTF_8))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), mapEntries("dttr", "vdl"), "qaz".getBytes(UTF_8))
                .build());

    pipeline.run();
  }

  @Test
  public void testSendsInvalidToDLQ() {
    Schema payloadSchema = Schema.builder().addInt32Field("id").addStringField("name").build();

    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addMapField("attributes", FieldType.STRING, FieldType.STRING)
            .addRowField("payload", payloadSchema)
            .build();

    PCollectionTuple outputs =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(1, map("attr1", "val1"), "{ \"invalid1\" : \"sdfsd\" }"),
                    message(2, map("attr2", "val2"), "{ \"invalid2"),
                    message(3, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                    message(4, map("bttr", "vbl"), "{ \"name\" : \"baz\", \"id\" : 5 }")))
            .apply(
                "convert",
                PubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(true)
                    .useFlatSchema(false)
                    .serializerProvider(JSON_SERIALIZER_PROVIDER)
                    .build());

    PCollection<Row> rows = outputs.get(MAIN_TAG);
    PCollection<PubsubMessage> dlqMessages = outputs.get(DLQ_TAG);

    PAssert.that(dlqMessages)
        .satisfies(
            messages -> {
              assertEquals(2, size(messages));
              assertEquals(
                  ImmutableSet.of(map("attr1", "val1"), map("attr2", "val2")),
                  convertToSet(messages, PubsubMessage::getAttributeMap));

              assertEquals(
                  ImmutableSet.of("{ \"invalid1\" : \"sdfsd\" }", "{ \"invalid2"),
                  convertToSet(messages, m -> new String(m.getPayload(), UTF_8)));
              return null;
            });

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
                .addValues(ts(3), map("attr", "val"), row(payloadSchema, 3, "foo"))
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), map("bttr", "vbl"), row(payloadSchema, 5, "baz"))
                .build());

    pipeline.run();
  }

  @Test
  public void testConvertsMessagesToFlatRow() {
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    PCollectionTuple rows =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(1, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                    message(2, map("bttr", "vbl"), "{ \"name\" : \"baz\", \"id\" : 5 }"),
                    message(3, map("cttr", "vcl"), "{ \"id\" : 7, \"name\" : \"bar\" }"),
                    message(4, map("dttr", "vdl"), "{ \"name\" : \"qaz\", \"id\" : 8 }"),
                    message(4, map("dttr", "vdl"), "{ \"name\" : null, \"id\" : null }")))
            .apply(
                "convert",
                PubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(false)
                    .useFlatSchema(true)
                    .serializerProvider(JSON_SERIALIZER_PROVIDER)
                    .build());

    PAssert.that(rows.get(MAIN_TAG))
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
                .addValues(ts(1), /* map("attr", "val"), */ 3, "foo")
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(2), /* map("bttr", "vbl"), */ 5, "baz")
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(3), /* map("cttr", "vcl"), */ 7, "bar")
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), /* map("dttr", "vdl"), */ 8, "qaz")
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), /* map("dttr", "vdl"), */ null, null)
                .build());

    pipeline.run();
  }

  @Test
  public void testSendsFlatRowInvalidToDLQ() {
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addInt32Field("id")
            .addStringField("name")
            .build();

    PCollectionTuple outputs =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(1, map("attr1", "val1"), "{ \"invalid1\" : \"sdfsd\" }"),
                    message(2, map("attr2", "val2"), "{ \"invalid2"),
                    message(3, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                    message(4, map("bttr", "vbl"), "{ \"name\" : \"baz\", \"id\" : 5 }")))
            .apply(
                "convert",
                PubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(true)
                    .useFlatSchema(true)
                    .serializerProvider(JSON_SERIALIZER_PROVIDER)
                    .build());

    PCollection<Row> rows = outputs.get(MAIN_TAG);
    PCollection<PubsubMessage> dlqMessages = outputs.get(DLQ_TAG);

    PAssert.that(dlqMessages)
        .satisfies(
            messages -> {
              assertEquals(2, size(messages));
              assertEquals(
                  ImmutableSet.of(map("attr1", "val1"), map("attr2", "val2")),
                  convertToSet(messages, PubsubMessage::getAttributeMap));

              assertEquals(
                  ImmutableSet.of("{ \"invalid1\" : \"sdfsd\" }", "{ \"invalid2"),
                  convertToSet(messages, m -> new String(m.getPayload(), UTF_8)));
              return null;
            });

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(messageSchema)
                .addValues(ts(3), /* map("attr", "val"), */ 3, "foo")
                .build(),
            Row.withSchema(messageSchema)
                .addValues(ts(4), /* map("bttr", "vbl"), */ 5, "baz")
                .build());

    pipeline.run();
  }

  @Test
  public void testFlatSchemaMessageInvalidElement() {
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addInt32Field("id")
            .addStringField("name")
            .build();

    pipeline
        .apply(
            "create",
            Create.timestamped(
                message(1, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                message(2, map("attr1", "val1"), "{ \"invalid1\" : \"sdfsd\" }")))
        .apply(
            "convert",
            PubsubMessageToRow.builder()
                .messageSchema(messageSchema)
                .useDlq(false)
                .useFlatSchema(true)
                .serializerProvider(JSON_SERIALIZER_PROVIDER)
                .build());

    Exception exception = Assert.assertThrows(RuntimeException.class, () -> pipeline.run());
    Assert.assertTrue(
        exception.toString(), exception.getMessage().contains("Non-nullable field 'id'"));
  }

  @Test
  public void testNestedSchemaMessageInvalidElement() {
    Schema payloadSchema = Schema.builder().addInt32Field("id").addStringField("name").build();

    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addMapField("attributes", FieldType.STRING, FieldType.STRING)
            .addRowField("payload", payloadSchema)
            .build();

    pipeline
        .apply(
            "create",
            Create.timestamped(
                message(1, map("attr", "val"), "{ \"id\" : 3, \"name\" : \"foo\" }"),
                message(2, map("attr1", "val1"), "{ \"invalid1\" : \"sdfsd\" }")))
        .apply(
            "convert",
            PubsubMessageToRow.builder()
                .messageSchema(messageSchema)
                .useDlq(false)
                .useFlatSchema(false)
                .serializerProvider(JSON_SERIALIZER_PROVIDER)
                .build());

    Exception exception = Assert.assertThrows(RuntimeException.class, () -> pipeline.run());
    Assert.assertTrue(
        exception.toString(), exception.getMessage().contains("Non-nullable field 'id'"));
  }

  private Row row(Schema schema, Object... objects) {
    return Row.withSchema(schema).addValues(objects).build();
  }

  private Map<String, String> map(String attr, String val) {
    return ImmutableMap.of(attr, val);
  }

  private List<Row> mapEntries(String attr, String val) {
    return ImmutableList.of(Row.withSchema(ATTRIBUTE_ARRAY_ENTRY_SCHEMA).attachValues(attr, val));
  }

  private TimestampedValue<PubsubMessage> message(
      int timestamp, Map<String, String> attributes, String payload) {

    return TimestampedValue.of(
        new PubsubMessage(payload.getBytes(UTF_8), attributes), ts(timestamp));
  }

  private Instant ts(long epochMills) {
    return new DateTime(epochMills).toInstant();
  }

  private static <V> Set<V> convertToSet(
      Iterable<PubsubMessage> messages, Function<? super PubsubMessage, V> mapper) {

    return StreamSupport.stream(messages.spliterator(), false).map(mapper).collect(toSet());
  }
}

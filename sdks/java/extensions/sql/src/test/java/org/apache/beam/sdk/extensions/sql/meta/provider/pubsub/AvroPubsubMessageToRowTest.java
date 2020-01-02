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
package org.apache.beam.sdk.extensions.sql.meta.provider.pubsub;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.DLQ_TAG;
import static org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubMessageToRow.MAIN_TAG;
import static org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.Iterables.size;
import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link AvroPubsubMessageToRow}. */
public class AvroPubsubMessageToRowTest implements Serializable {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testConvertsMessagesToFlatRow() throws Exception {

    Schema payloadSchema =
        Schema.builder()
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    Map<String, Object> nullId = new HashMap<>();
    nullId.put("id", null);
    nullId.put("name", "Luke");
    Map<String, Object> nullValues = new HashMap<>();
    nullValues.put("id", null);
    nullValues.put("name", null);

    PCollectionTuple rows =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message(
                        1,
                        map("attr", "val"),
                        getGenericRecord(payloadSchema, ImmutableMap.of("id", 1, "name", "Jack"))),
                    message(
                        2,
                        map("attr", "val"),
                        getGenericRecord(payloadSchema, ImmutableMap.of("name", "Paul", "id", 2))),
                    message(
                        3,
                        map("attr", "val"),
                        getGenericRecord(payloadSchema, ImmutableMap.of("id", 3, "name", "Bill"))),
                    message(4, map("attr", "val"), getGenericRecord(payloadSchema, nullId)),
                    message(3, map("attr", "val"), getGenericRecord(payloadSchema, nullValues))))
            .apply(
                "convert",
                AvroPubsubMessageToRow.builder()
                    .messageSchema(messageSchema)
                    .useDlq(false)
                    .build());

    PAssert.that(rows.get(MAIN_TAG))
        .containsInAnyOrder(
            Row.withSchema(messageSchema).addValues(ts(1), 1, "Jack").build(),
            Row.withSchema(messageSchema).addValues(ts(2), 2, "Paul").build(),
            Row.withSchema(messageSchema).addValues(ts(3), 3, "Bill").build(),
            Row.withSchema(messageSchema).addValues(ts(4), null, "Luke").build(),
            Row.withSchema(messageSchema).addValues(ts(3), null, null).build());

    pipeline.run();
  }

  @Test
  public void testSendsInvalidFlatMessageToDLQ() throws Exception {
    Schema payloadSchema =
        Schema.builder()
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    Schema invalidPayloadSchema = Schema.builder().addNullableField("id", FieldType.STRING).build();

    Schema messageSchema =
        Schema.builder()
            .addNullableField("event_timestamp", FieldType.DATETIME)
            .addNullableField("id", FieldType.INT32)
            .addNullableField("name", FieldType.STRING)
            .build();

    Map<String, Object> fields = new HashMap<>();
    fields.put("id", null);
    GenericRecord record = getGenericRecord(invalidPayloadSchema, fields);
    TimestampedValue<PubsubMessage> message1 =
        message(
            1,
            map("attr1", "val1"),
            getGenericRecord(invalidPayloadSchema, ImmutableMap.of("id", "xyz")));
    TimestampedValue<PubsubMessage> message2 = message(2, map("attr2", "val2"), record);

    PCollectionTuple outputs =
        pipeline
            .apply(
                "create",
                Create.timestamped(
                    message1,
                    message2,
                    message(
                        6,
                        map("attr", "val"),
                        getGenericRecord(
                            payloadSchema, ImmutableMap.of("id", 1, "name", "George"))),
                    message(
                        7,
                        map("bttr", "vbl"),
                        getGenericRecord(
                            payloadSchema, ImmutableMap.of("id", 2, "name", "Lucas")))))
            .apply(
                "convert",
                AvroPubsubMessageToRow.builder().messageSchema(messageSchema).useDlq(true).build());

    PCollection<Row> rows = outputs.get(MAIN_TAG);
    PCollection<PubsubMessage> dlqMessages = outputs.get(DLQ_TAG);

    byte[] payload1 = message1.getValue().getPayload();
    byte[] payload2 = message2.getValue().getPayload();

    PAssert.that(dlqMessages)
        .satisfies(
            messages -> {
              assertEquals(2, size(messages));
              assertEquals(
                  ImmutableSet.of(map("attr1", "val1"), map("attr2", "val2")),
                  convertToSet(messages, m -> m.getAttributeMap()));

              assertEquals(
                  ImmutableSet.of(new String(payload1, UTF_8), new String(payload2, UTF_8)),
                  convertToSet(messages, m -> new String(m.getPayload(), UTF_8)));
              return null;
            });

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(messageSchema).addValues(ts(6), 1, "George").build(),
            Row.withSchema(messageSchema).addValues(ts(7), 2, "Lucas").build());

    pipeline.run();
  }

  @Test
  public void testFlatSchemaMessageInvalidElement() throws Exception {
    Schema messageSchema =
        Schema.builder()
            .addDateTimeField("event_timestamp")
            .addInt32Field("id")
            .addStringField("name")
            .build();

    Schema invalidPayloadSchema = Schema.builder().addNullableField("id", FieldType.STRING).build();

    pipeline
        .apply(
            "create",
            Create.timestamped(
                message(
                    1,
                    map("attr1", "val1"),
                    getGenericRecord(invalidPayloadSchema, ImmutableMap.of("id", "xyz")))))
        .apply(
            "convert",
            JsonPubsubMessageToRow.builder()
                .messageSchema(messageSchema)
                .useDlq(false)
                .useFlatSchema(true)
                .build());

    Exception exception = Assert.assertThrows(RuntimeException.class, () -> pipeline.run());
    Assert.assertTrue(exception.getMessage().contains("Error parsing message"));
  }

  private GenericRecord getGenericRecord(Schema payloadSchema, Map<String, Object> pairs) {
    GenericRecord record = new Record(AvroUtils.toAvroSchema(payloadSchema));
    for (String key : pairs.keySet()) {
      record.put(key, pairs.get(key));
    }
    return record;
  }

  private Map<String, String> map(String attr, String val) {
    return ImmutableMap.of(attr, val);
  }

  private TimestampedValue<PubsubMessage> message(
      int timestamp, Map<String, String> attributes, GenericRecord payload) throws Exception {

    return TimestampedValue.of(
        new PubsubMessage(AvroUtils.toBytes(payload), attributes), ts(timestamp));
  }

  private Instant ts(long epochMills) {
    return new DateTime(epochMills).toInstant();
  }

  private static <V> Set<V> convertToSet(
      Iterable<PubsubMessage> messages, Function<? super PubsubMessage, V> mapper) {

    return StreamSupport.stream(messages.spliterator(), false).map(mapper).collect(toSet());
  }
}

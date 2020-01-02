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
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.StreamSupport;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.PubsubTableProvider.PubsubIOTableConfiguration;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

/**
 * Unit tests for {@link
 * org.apache.beam.sdk.extensions.sql.meta.provider.pubsub.RowToAvroPubsubMessage}.
 */
public class RowToAvroPubsubMessageTest {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  private static final String EVENT_TIMESTAMP = "event_timestamp";
  private static final String TOPIC = "test";

  private static final Schema beamSchema =
      Schema.builder()
          .addDateTimeField(EVENT_TIMESTAMP)
          .addInt32Field("id")
          .addNullableField("name", FieldType.STRING)
          .build();

  private static final Schema payloadSchema =
      Schema.builder().addInt32Field("id").addNullableField("name", FieldType.STRING).build();

  @Test
  public void testConvertsRowToPubsubMessage() throws Exception {
    PCollection<PubsubMessage> flatMessages =
        pipeline
            .apply(
                "create",
                Create.of(
                    ImmutableList.of(
                        Row.withSchema(beamSchema).addValues(ts(5), 1, "Jack").build(),
                        Row.withSchema(beamSchema).addValues(ts(6), 3, "Paul").build(),
                        Row.withSchema(beamSchema).addValues(ts(7), 5, null).build())))
            .setRowSchema(beamSchema)
            .apply(
                RowToAvroPubsubMessage.fromTableConfig(
                    PubsubIOTableConfiguration.builder()
                        .setSchema(beamSchema)
                        .setTopic(TOPIC)
                        .setUseFlatSchema(true)
                        .build()));

    String payload1 =
        new String(
            AvroUtils.toBytes(
                getGenericRecord(payloadSchema, ImmutableMap.of("id", 1, "name", "Jack"))),
            UTF_8);
    String payload2 =
        new String(
            AvroUtils.toBytes(
                getGenericRecord(payloadSchema, ImmutableMap.of("id", 3, "name", "Paul"))),
            UTF_8);
    Map<String, Object> fields = new HashMap<>();
    fields.put("id", 5);
    fields.put("name", null);
    String payload3 = new String(AvroUtils.toBytes(getGenericRecord(payloadSchema, fields)), UTF_8);

    PAssert.that(flatMessages)
        .satisfies(
            messages -> {
              assertEquals(
                  ImmutableSet.of(payload1, payload2, payload3),
                  convertToSet(messages, message -> new String(message.getPayload(), UTF_8)));
              return null;
            });

    pipeline.run();
  }

  // Construct an generic record.
  private GenericRecord getGenericRecord(Schema payloadSchema, Map<String, Object> pairs) {
    GenericRecord record = new Record(AvroUtils.toAvroSchema(payloadSchema));
    for (String key : pairs.keySet()) {
      record.put(key, pairs.get(key));
    }
    return record;
  }

  private Instant ts(long epochMills) {
    return new DateTime(epochMills).toInstant();
  }

  private static <V> Set<V> convertToSet(
      Iterable<PubsubMessage> messages, Function<? super PubsubMessage, V> mapper) {
    return StreamSupport.stream(messages.spliterator(), false).map(mapper).collect(toSet());
  }
}

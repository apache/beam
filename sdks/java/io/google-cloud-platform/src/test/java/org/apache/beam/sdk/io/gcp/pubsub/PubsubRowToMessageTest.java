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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.ATTRIBUTES_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.ATTRIBUTES_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_ATTRIBUTES_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_EVENT_TIMESTAMP_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_PAYLOAD_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.ERROR;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.OUTPUT;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.PAYLOAD_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.errorSchema;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.removeFields;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.avro.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.FieldMatcher;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.PubsubRowToMessageDoFn;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.SchemaReflection;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.CheckEnabled;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PubsubRowToMessage}. */
@RunWith(JUnit4.class)
public class PubsubRowToMessageTest {

  private static final PipelineOptions PIPELINE_OPTIONS = PipelineOptionsFactory.create();
  static final Field BOOLEAN_FIELD = Field.of("boolean", FieldType.BOOLEAN);
  static final Field BYTE_FIELD = Field.of("byte", FieldType.BYTE);
  static final Field DATETIME_FIELD = Field.of("datetime", FieldType.DATETIME);
  static final Field DECIMAL_FIELD = Field.of("decimal", FieldType.DECIMAL);
  static final Field DOUBLE_FIELD = Field.of("double", FieldType.DOUBLE);
  static final Field FLOAT_FIELD = Field.of("float", FieldType.FLOAT);
  static final Field INT16_FIELD = Field.of("int16", FieldType.INT16);
  static final Field INT32_FIELD = Field.of("int32", FieldType.INT32);
  static final Field INT64_FIELD = Field.of("int64", FieldType.INT64);
  static final Field STRING_FIELD = Field.of("string", FieldType.STRING);
  static final Schema ALL_DATA_TYPES_SCHEMA =
      Schema.of(
          BOOLEAN_FIELD,
          BYTE_FIELD,
          DATETIME_FIELD,
          DECIMAL_FIELD,
          DOUBLE_FIELD,
          FLOAT_FIELD,
          INT16_FIELD,
          INT32_FIELD,
          INT64_FIELD,
          STRING_FIELD);

  static final Schema NON_USER_WITH_BYTES_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
          Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES));

  static final Schema NON_USER_WITH_ROW_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
          Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA)));

  static final Schema NON_USER_WITHOUT_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE));

  static {
    PIPELINE_OPTIONS.setStableUniqueNames(CheckEnabled.OFF);
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TestPipeline errorsTestPipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test(expected = IllegalArgumentException.class)
  public void testExpandThrowsExceptions() {
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    Instant embeddedTimestamp = Instant.now();
    Instant mockTimestamp = Instant.ofEpochMilli(embeddedTimestamp.getMillis() + 1000000L);

    Field attributesField = Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE);
    Schema withAttributesSchema = Schema.of(attributesField);
    Row withAttributes =
        Row.withSchema(withAttributesSchema).attachValues(ImmutableMap.of("aaa", "111"));

    Field timestampField = Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE);
    Schema withTimestampSchema = Schema.of(timestampField);
    Row withEmbeddedTimestamp = Row.withSchema(withTimestampSchema).attachValues(embeddedTimestamp);

    Field payloadBytesField = Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES);
    Schema withPayloadBytesSchema = Schema.of(payloadBytesField);
    Row withPayloadBytes =
        Row.withSchema(withPayloadBytesSchema).attachValues(new byte[] {1, 2, 3, 4});

    Row withAllDataTypes =
        rowWithAllDataTypes(
            true,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(1L),
            3.12345,
            4.1f,
            (short) 5,
            2,
            7L,
            "asdfjkl;");

    Field payloadRowField =
        Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA));
    Schema withPayloadRowSchema = Schema.of(payloadRowField);
    Row withPayloadRow = Row.withSchema(withPayloadRowSchema).attachValues(withAllDataTypes);

    PubsubRowToMessage transformWithoutSerializer =
        PubsubRowToMessage.builder().setMockInstant(mockTimestamp).build();

    PubsubRowToMessage transformWithSerializer =
        PubsubRowToMessage.builder().setPayloadSerializer(jsonPayloadSerializer).build();

    // requires either payload or user fields
    errorsTestPipeline
        .apply(Create.of(withAttributes))
        .setRowSchema(withAttributesSchema)
        .apply(transformWithoutSerializer);

    // requires either payload or user fields
    errorsTestPipeline
        .apply(Create.of(withEmbeddedTimestamp))
        .setRowSchema(withTimestampSchema)
        .apply(transformWithoutSerializer);

    // payload bytes incompatible with non-null payload serializer
    errorsTestPipeline
        .apply(Create.of(withPayloadBytes))
        .setRowSchema(withPayloadBytesSchema)
        .apply(transformWithSerializer);

    // payload row requires payload serializer
    errorsTestPipeline
        .apply(Create.of(withPayloadRow))
        .setRowSchema(withPayloadRowSchema)
        .apply(transformWithoutSerializer);

    // user fields row requires payload serializer
    errorsTestPipeline
        .apply(Create.of(withAllDataTypes))
        .setRowSchema(ALL_DATA_TYPES_SCHEMA)
        .apply(transformWithoutSerializer);

    // input with both payload row and user fields incompatible
    errorsTestPipeline
        .apply(Create.of(merge(withPayloadRow, withAllDataTypes)))
        .setRowSchema(merge(withPayloadRowSchema, ALL_DATA_TYPES_SCHEMA))
        .apply(transformWithSerializer);
  }

  @Test
  public void testExpandError() {
    PayloadSerializer mismatchedSerializerProviderSchema =
        new JsonPayloadSerializerProvider()
            .getSerializer(Schema.of(STRING_FIELD), ImmutableMap.of());

    Row withAllDataTypes =
        rowWithAllDataTypes(
            true,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(1L),
            3.12345,
            4.1f,
            (short) 5,
            2,
            7L,
            "asdfjkl;");

    PubsubRowToMessage transformWithSerializer =
        PubsubRowToMessage.builder()
            .setPayloadSerializer(mismatchedSerializerProviderSchema)
            .build();

    PAssert.that(
            pipeline
                .apply(Create.of(withAllDataTypes))
                .setRowSchema(ALL_DATA_TYPES_SCHEMA)
                .apply(transformWithSerializer)
                .get(ERROR)
                .apply(Count.globally()))
        .containsInAnyOrder(1L);

    pipeline.run(PIPELINE_OPTIONS);
  }

  @Test
  public void testExpandOutput() {
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    Instant embeddedTimestamp = Instant.now();
    Instant mockTimestamp = Instant.ofEpochMilli(embeddedTimestamp.getMillis() + 1000000L);
    String embeddedTimestampString = embeddedTimestamp.toString();
    String mockTimestampString = mockTimestamp.toString();

    byte[] bytes =
        "All the 🌎's a 🎦, and all the 🚹 and 🚺 merely players. They 🈷️their 🚪and their 🎟️and 1️⃣🚹in his time ▶️many🤺🪂🏆"
            .getBytes(StandardCharsets.UTF_8);

    Field attributesField = Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE);
    Field timestampField = Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE);
    Field payloadBytesField = Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES);
    Field payloadRowField =
        Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA));

    Row withAllDataTypes =
        rowWithAllDataTypes(
            true,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(1L),
            3.12345,
            4.1f,
            (short) 5,
            2,
            7L,
            "asdfjkl;");

    byte[] serializedWithAllDataTypes = jsonPayloadSerializer.serialize(withAllDataTypes);

    Schema withAttributesSchema = Schema.of(attributesField);
    Row withAttributes =
        Row.withSchema(withAttributesSchema).attachValues(ImmutableMap.of("aaa", "111"));

    Schema withTimestampSchema = Schema.of(timestampField);
    Row withEmbeddedTimestamp = Row.withSchema(withTimestampSchema).attachValues(embeddedTimestamp);

    Schema withPayloadBytesSchema = Schema.of(payloadBytesField);
    Row withPayloadBytes = Row.withSchema(withPayloadBytesSchema).attachValues(bytes);

    Schema withPayloadRowSchema = Schema.of(payloadRowField);
    Row withPayloadRow = Row.withSchema(withPayloadRowSchema).attachValues(withAllDataTypes);

    PubsubRowToMessage transformWithoutSerializer =
        PubsubRowToMessage.builder().setMockInstant(mockTimestamp).build();

    PubsubRowToMessage transformWithSerializer =
        PubsubRowToMessage.builder()
            .setMockInstant(mockTimestamp)
            .setPayloadSerializer(jsonPayloadSerializer)
            .build();

    PCollection<Row> a0t0p0u1 =
        pipeline.apply(Create.of(withAllDataTypes)).setRowSchema(ALL_DATA_TYPES_SCHEMA);
    PubsubMessage a0t0p0u1Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString));
    PAssert.that(
            "expected: payload: user serialized row, attributes: ParDo mocked generated timestamp only",
            a0t0p0u1.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a0t0p0u1Message);

    PCollection<Row> a1t0p0u1 =
        pipeline
            .apply(Create.of(merge(withAttributes, withAllDataTypes)))
            .setRowSchema(merge(withAttributesSchema, ALL_DATA_TYPES_SCHEMA));
    PubsubMessage a1t0p0u1Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of("aaa", "111", DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString));
    PAssert.that(
            "expected: payload: user serialized row, attributes: input attributes and ParDo generated timestamp",
            a1t0p0u1.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a1t0p0u1Message);

    PCollection<Row> a0t1p0u1 =
        pipeline
            .apply(Create.of(merge(withEmbeddedTimestamp, withAllDataTypes)))
            .setRowSchema(merge(withTimestampSchema, ALL_DATA_TYPES_SCHEMA));
    PubsubMessage a0t1p0u1Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, embeddedTimestampString));
    PAssert.that(
            "expected: payload: user serialized row, attributes: timestamp from row input",
            a0t1p0u1.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a0t1p0u1Message);

    PCollection<Row> a0t0p1bu0 =
        pipeline.apply(Create.of(withPayloadBytes)).setRowSchema(withPayloadBytesSchema);
    PubsubMessage a0t0p1bu0Message =
        new PubsubMessage(
            bytes, ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString));
    PAssert.that(
            "expected: payload: raw bytes, attributes: ParDo generated timestamp",
            a0t0p1bu0.apply(transformWithoutSerializer).get(OUTPUT))
        .containsInAnyOrder(a0t0p1bu0Message);

    PCollection<Row> a0t0p1ru0 =
        pipeline.apply(Create.of(withPayloadRow)).setRowSchema(withPayloadRowSchema);
    PubsubMessage a0t0p1ru0Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString));
    PAssert.that(
            "expected: payload: non-user row, attributes: ParDo generated timestamp",
            a0t0p1ru0.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a0t0p1ru0Message);

    PCollection<Row> a1t0p1bu0 =
        pipeline
            .apply(Create.of(merge(withAttributes, withPayloadBytes)))
            .setRowSchema(merge(withAttributesSchema, withPayloadBytesSchema));
    PubsubMessage a1t0p1bu0Message =
        new PubsubMessage(
            bytes,
            ImmutableMap.of("aaa", "111", DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString));
    PAssert.that(
            "expected: raw bytes, attributes: embedded attributes and ParDo generated timestamp",
            a1t0p1bu0.apply(transformWithoutSerializer).get(OUTPUT))
        .containsInAnyOrder(a1t0p1bu0Message);

    PCollection<Row> a0t1p1bu0 =
        pipeline
            .apply(Create.of(merge(withEmbeddedTimestamp, withPayloadBytes)))
            .setRowSchema(merge(withTimestampSchema, withPayloadBytesSchema));
    PubsubMessage a0t1p1bu0Message =
        new PubsubMessage(
            bytes, ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, embeddedTimestampString));
    PAssert.that(
            "expected: payload: raw bytes, attributes: timestamp from row input",
            a0t1p1bu0.apply(transformWithoutSerializer).get(OUTPUT))
        .containsInAnyOrder(a0t1p1bu0Message);

    PCollection<Row> a1t1p1bu0 =
        pipeline
            .apply(Create.of(merge(merge(withAttributes, withEmbeddedTimestamp), withPayloadBytes)))
            .setRowSchema(
                merge(merge(withAttributesSchema, withTimestampSchema), withPayloadBytesSchema));
    PubsubMessage a1t1p1bu0Message =
        new PubsubMessage(
            bytes,
            ImmutableMap.of(
                "aaa", "111", DEFAULT_EVENT_TIMESTAMP_KEY_NAME, embeddedTimestampString));
    PAssert.that(
            "expected: payload: raw bytes, attributes: from row input including its embedded timestamp",
            a1t1p1bu0.apply(transformWithoutSerializer).get(OUTPUT))
        .containsInAnyOrder(a1t1p1bu0Message);

    PCollection<Row> a1t1p1ru0 =
        pipeline
            .apply(Create.of(merge(merge(withAttributes, withEmbeddedTimestamp), withPayloadRow)))
            .setRowSchema(
                merge(merge(withAttributesSchema, withTimestampSchema), withPayloadRowSchema));
    PubsubMessage a1t1p1ru0Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of(
                "aaa", "111", DEFAULT_EVENT_TIMESTAMP_KEY_NAME, embeddedTimestampString));
    PAssert.that(
            "expected: payload: serialized row payload input, attributes: from row input including its embedded timestamp",
            a1t1p1ru0.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a1t1p1ru0Message);

    PCollection<Row> a1t1p0u1 =
        pipeline
            .apply(Create.of(merge(merge(withAttributes, withEmbeddedTimestamp), withAllDataTypes)))
            .setRowSchema(
                merge(merge(withAttributesSchema, withTimestampSchema), ALL_DATA_TYPES_SCHEMA));
    PubsubMessage a1t1p0u1Message =
        new PubsubMessage(
            serializedWithAllDataTypes,
            ImmutableMap.of(
                "aaa", "111", DEFAULT_EVENT_TIMESTAMP_KEY_NAME, embeddedTimestampString));
    PAssert.that(
            "expected: payload: serialized user fields, attributes: from row input including embedded timestamp",
            a1t1p0u1.apply(transformWithSerializer).get(OUTPUT))
        .containsInAnyOrder(a1t1p0u1Message);

    pipeline.run(PIPELINE_OPTIONS);
  }

  @Test
  public void testKeyPrefix() {
    PubsubRowToMessage withDefaultKeyPrefix = PubsubRowToMessage.builder().build();
    assertEquals(DEFAULT_ATTRIBUTES_KEY_NAME, withDefaultKeyPrefix.getAttributesKeyName());
    assertEquals(
        DEFAULT_EVENT_TIMESTAMP_KEY_NAME, withDefaultKeyPrefix.getSourceEventTimestampKeyName());
    assertEquals(DEFAULT_PAYLOAD_KEY_NAME, withDefaultKeyPrefix.getPayloadKeyName());

    PubsubRowToMessage withKeyPrefixOverride =
        PubsubRowToMessage.builder().setKeyPrefix("_").build();
    assertEquals("_" + ATTRIBUTES_KEY_NAME, withKeyPrefixOverride.getAttributesKeyName());
    assertEquals(
        "_" + EVENT_TIMESTAMP_KEY_NAME, withKeyPrefixOverride.getSourceEventTimestampKeyName());
    assertEquals("_" + PAYLOAD_KEY_NAME, withKeyPrefixOverride.getPayloadKeyName());
  }

  @Test
  public void testSetTargetTimestampAttributeName() {
    Instant mockTimestamp = Instant.now();
    String mockTimestampString = mockTimestamp.toString();
    byte[] bytes = new byte[] {1, 2, 3, 4};

    Field payloadBytesField = Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES);

    Schema withPayloadBytesSchema = Schema.of(payloadBytesField);
    Row withPayloadBytes = Row.withSchema(withPayloadBytesSchema).attachValues(bytes);

    String customTargetTimestampAttributeName = "custom_timestamp_key";

    PubsubRowToMessage withoutSetTargetTimestampAttributeName =
        PubsubRowToMessage.builder().setMockInstant(mockTimestamp).build();
    PubsubRowToMessage withSetTargetTimestampAttributeName =
        PubsubRowToMessage.builder()
            .setMockInstant(mockTimestamp)
            .setTargetTimestampAttributeName(customTargetTimestampAttributeName)
            .build();

    PCollection<Row> input =
        pipeline.apply(Create.of(withPayloadBytes)).setRowSchema(Schema.of(payloadBytesField));

    PAssert.that(input.apply(withoutSetTargetTimestampAttributeName).get(OUTPUT))
        .containsInAnyOrder(
            new PubsubMessage(
                bytes, ImmutableMap.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, mockTimestampString)));

    PAssert.that(input.apply(withSetTargetTimestampAttributeName).get(OUTPUT))
        .containsInAnyOrder(
            new PubsubMessage(
                bytes, ImmutableMap.of(customTargetTimestampAttributeName, mockTimestampString)));

    pipeline.run(PIPELINE_OPTIONS);
  }

  @Test
  public void testInputSchemaFactory() {
    assertEquals(
        Schema.of(
            Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
            Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE)),
        PubsubRowToMessage.builder().build().inputSchemaFactory().buildSchema());

    assertEquals(
        Schema.of(
            Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
            Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
            Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES)),
        PubsubRowToMessage.builder().build().inputSchemaFactory(FieldType.BYTES).buildSchema());

    assertEquals(
        Schema.of(
            Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
            Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
            Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA))),
        PubsubRowToMessage.builder()
            .build()
            .inputSchemaFactory(FieldType.row(ALL_DATA_TYPES_SCHEMA))
            .buildSchema());

    String prefix = "_";
    assertEquals(
        Schema.of(
            Field.of(prefix + ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
            Field.of(prefix + EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE)),
        PubsubRowToMessage.builder()
            .setKeyPrefix(prefix)
            .build()
            .inputSchemaFactory()
            .buildSchema());

    Field[] userFields = ALL_DATA_TYPES_SCHEMA.getFields().toArray(new Field[0]);
    assertEquals(
        merge(
            Schema.of(
                Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
                Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE)),
            ALL_DATA_TYPES_SCHEMA),
        PubsubRowToMessage.builder().build().inputSchemaFactory().buildSchema(userFields));
  }

  @Test
  public void testValidate() {
    PubsubRowToMessage pubsubRowToMessage = PubsubRowToMessage.builder().build();
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    assertThrows(
        IllegalArgumentException.class,
        () -> pubsubRowToMessage.validate(Schema.builder().build()));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validate(
                Schema.of(Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, FieldType.STRING))));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validate(
                Schema.of(
                    Field.of(
                        DEFAULT_ATTRIBUTES_KEY_NAME,
                        FieldType.map(FieldType.STRING, FieldType.BYTES)))));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validate(
                Schema.of(
                    Field.of(
                        DEFAULT_ATTRIBUTES_KEY_NAME,
                        FieldType.map(FieldType.BYTES, FieldType.STRING)))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validate(
                Schema.of(Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, FieldType.STRING))));

    assertThrows(
        IllegalArgumentException.class,
        () -> PubsubRowToMessage.builder().build().validate(ALL_DATA_TYPES_SCHEMA));

    assertThrows(
        IllegalArgumentException.class,
        () -> PubsubRowToMessage.builder().build().validate(NON_USER_WITH_ROW_PAYLOAD));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .setPayloadSerializer(jsonPayloadSerializer)
                .build()
                .validate(NON_USER_WITH_BYTES_PAYLOAD));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validate(merge(ALL_DATA_TYPES_SCHEMA, NON_USER_WITH_BYTES_PAYLOAD)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validate(merge(ALL_DATA_TYPES_SCHEMA, NON_USER_WITH_ROW_PAYLOAD)));
  }

  @Test
  public void testValidateAttributesField() {
    PubsubRowToMessage pubsubRowToMessage = PubsubRowToMessage.builder().build();
    pubsubRowToMessage.validateAttributesField(ALL_DATA_TYPES_SCHEMA);
    pubsubRowToMessage.validateAttributesField(
        Schema.of(Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validateAttributesField(
                Schema.of(Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, FieldType.STRING))));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validateAttributesField(
                Schema.of(
                    Field.of(
                        DEFAULT_ATTRIBUTES_KEY_NAME,
                        FieldType.map(FieldType.STRING, FieldType.BYTES)))));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validateAttributesField(
                Schema.of(
                    Field.of(
                        DEFAULT_ATTRIBUTES_KEY_NAME,
                        FieldType.map(FieldType.BYTES, FieldType.STRING)))));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder().build().validateSerializableFields(ALL_DATA_TYPES_SCHEMA));
  }

  @Test
  public void testValidateSourceEventTimeStampField() {
    PubsubRowToMessage pubsubRowToMessage = PubsubRowToMessage.builder().build();
    pubsubRowToMessage.validateSourceEventTimeStampField(ALL_DATA_TYPES_SCHEMA);
    pubsubRowToMessage.validateSourceEventTimeStampField(
        Schema.of(Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            pubsubRowToMessage.validateSourceEventTimeStampField(
                Schema.of(Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, FieldType.STRING))));
  }

  @Test
  public void testShouldValidatePayloadSerializerNullState() {
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    PayloadSerializer avroPayloadSerializer =
        new AvroPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    PubsubRowToMessage.builder()
        .setPayloadSerializer(jsonPayloadSerializer)
        .build()
        .validateSerializableFields(ALL_DATA_TYPES_SCHEMA);

    PubsubRowToMessage.builder()
        .setPayloadSerializer(avroPayloadSerializer)
        .build()
        .validateSerializableFields(ALL_DATA_TYPES_SCHEMA);

    PubsubRowToMessage.builder()
        .setPayloadSerializer(jsonPayloadSerializer)
        .build()
        .validateSerializableFields(NON_USER_WITH_ROW_PAYLOAD);

    PubsubRowToMessage.builder()
        .setPayloadSerializer(avroPayloadSerializer)
        .build()
        .validateSerializableFields(NON_USER_WITH_ROW_PAYLOAD);

    PubsubRowToMessage.builder().build().validateSerializableFields(NON_USER_WITH_BYTES_PAYLOAD);

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder().build().validateSerializableFields(ALL_DATA_TYPES_SCHEMA));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validateSerializableFields(NON_USER_WITH_ROW_PAYLOAD));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .setPayloadSerializer(jsonPayloadSerializer)
                .build()
                .validateSerializableFields(NON_USER_WITH_BYTES_PAYLOAD));
  }

  @Test
  public void testShouldEitherHavePayloadOrUserFields() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validateSerializableFields(
                    Schema.of(
                        Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE))));
  }

  @Test
  public void testShouldNotAllowPayloadFieldWithUserFields() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validateSerializableFields(
                    merge(ALL_DATA_TYPES_SCHEMA, NON_USER_WITH_BYTES_PAYLOAD)));

    assertThrows(
        IllegalArgumentException.class,
        () ->
            PubsubRowToMessage.builder()
                .build()
                .validateSerializableFields(
                    merge(ALL_DATA_TYPES_SCHEMA, NON_USER_WITH_ROW_PAYLOAD)));
  }

  @Test
  public void testSchemaReflection_matchesAll() {
    SchemaReflection schemaReflection = SchemaReflection.of(ALL_DATA_TYPES_SCHEMA);

    assertTrue(
        schemaReflection.matchesAll(
            FieldMatcher.of(BOOLEAN_FIELD.getName()),
            FieldMatcher.of(BYTE_FIELD.getName()),
            FieldMatcher.of(DATETIME_FIELD.getName()),
            FieldMatcher.of(DECIMAL_FIELD.getName()),
            FieldMatcher.of(DOUBLE_FIELD.getName()),
            FieldMatcher.of(FLOAT_FIELD.getName()),
            FieldMatcher.of(INT16_FIELD.getName()),
            FieldMatcher.of(INT32_FIELD.getName()),
            FieldMatcher.of(INT64_FIELD.getName()),
            FieldMatcher.of(STRING_FIELD.getName())));

    assertTrue(
        schemaReflection.matchesAll(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), FieldType.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), FieldType.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), FieldType.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), FieldType.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), FieldType.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), FieldType.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), FieldType.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), FieldType.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), FieldType.INT64),
            FieldMatcher.of(STRING_FIELD.getName(), FieldType.STRING)));

    assertTrue(
        schemaReflection.matchesAll(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), TypeName.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), TypeName.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), TypeName.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), TypeName.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), TypeName.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), TypeName.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), TypeName.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), TypeName.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), TypeName.INT64),
            FieldMatcher.of(STRING_FIELD.getName(), TypeName.STRING)));

    assertFalse(
        schemaReflection.matchesAll(
            FieldMatcher.of("idontexist"),
            FieldMatcher.of(BOOLEAN_FIELD.getName()),
            FieldMatcher.of(BYTE_FIELD.getName()),
            FieldMatcher.of(DATETIME_FIELD.getName()),
            FieldMatcher.of(DECIMAL_FIELD.getName()),
            FieldMatcher.of(DOUBLE_FIELD.getName()),
            FieldMatcher.of(FLOAT_FIELD.getName()),
            FieldMatcher.of(INT16_FIELD.getName()),
            FieldMatcher.of(INT32_FIELD.getName()),
            FieldMatcher.of(INT64_FIELD.getName()),
            FieldMatcher.of(STRING_FIELD.getName())));

    assertFalse(
        schemaReflection.matchesAll(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), FieldType.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), FieldType.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), FieldType.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), FieldType.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), FieldType.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), FieldType.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), FieldType.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), FieldType.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), FieldType.INT64),
            // should not match type:
            FieldMatcher.of(STRING_FIELD.getName(), FieldType.BYTE)));

    assertFalse(
        schemaReflection.matchesAll(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), TypeName.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), TypeName.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), TypeName.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), TypeName.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), TypeName.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), TypeName.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), TypeName.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), TypeName.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), TypeName.INT64),
            // should not match TypeName:
            FieldMatcher.of(STRING_FIELD.getName(), TypeName.INT16)));
  }

  @Test
  public void testSchemaReflection_matchesAny() {
    SchemaReflection schemaReflection = SchemaReflection.of(ALL_DATA_TYPES_SCHEMA);
    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of(BOOLEAN_FIELD.getName()),
            FieldMatcher.of(BYTE_FIELD.getName()),
            FieldMatcher.of(DATETIME_FIELD.getName()),
            FieldMatcher.of(DECIMAL_FIELD.getName()),
            FieldMatcher.of(DOUBLE_FIELD.getName()),
            FieldMatcher.of(FLOAT_FIELD.getName()),
            FieldMatcher.of(INT16_FIELD.getName()),
            FieldMatcher.of(INT32_FIELD.getName()),
            FieldMatcher.of(INT64_FIELD.getName()),
            FieldMatcher.of(STRING_FIELD.getName())));

    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), FieldType.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), FieldType.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), FieldType.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), FieldType.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), FieldType.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), FieldType.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), FieldType.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), FieldType.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), FieldType.INT64),
            FieldMatcher.of(STRING_FIELD.getName(), FieldType.STRING)));

    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of(BOOLEAN_FIELD.getName(), TypeName.BOOLEAN),
            FieldMatcher.of(BYTE_FIELD.getName(), TypeName.BYTE),
            FieldMatcher.of(DATETIME_FIELD.getName(), TypeName.DATETIME),
            FieldMatcher.of(DECIMAL_FIELD.getName(), TypeName.DECIMAL),
            FieldMatcher.of(DOUBLE_FIELD.getName(), TypeName.DOUBLE),
            FieldMatcher.of(FLOAT_FIELD.getName(), TypeName.FLOAT),
            FieldMatcher.of(INT16_FIELD.getName(), TypeName.INT16),
            FieldMatcher.of(INT32_FIELD.getName(), TypeName.INT32),
            FieldMatcher.of(INT64_FIELD.getName(), TypeName.INT64),
            FieldMatcher.of(STRING_FIELD.getName(), TypeName.STRING)));

    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of("idontexist"), FieldMatcher.of(STRING_FIELD.getName())));

    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of(INT64_FIELD.getName(), FieldType.INT64),
            // should not match type:
            FieldMatcher.of(STRING_FIELD.getName(), FieldType.BYTE)));

    assertTrue(
        schemaReflection.matchesAny(
            FieldMatcher.of(INT64_FIELD.getName(), TypeName.INT64),
            // should not match TypeName:
            FieldMatcher.of(STRING_FIELD.getName(), TypeName.INT16)));

    assertFalse(
        schemaReflection.matchesAny(
            FieldMatcher.of("idontexist"),
            FieldMatcher.of(STRING_FIELD.getName(), FieldType.BYTE),
            FieldMatcher.of(STRING_FIELD.getName(), TypeName.INT16)));
  }

  @Test
  public void testFieldMatcher_match_NameOnly() {
    FieldMatcher fieldMatcher = FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME);
    assertTrue(fieldMatcher.match(NON_USER_WITH_ROW_PAYLOAD));
    assertTrue(fieldMatcher.match(NON_USER_WITH_BYTES_PAYLOAD));
    assertFalse(fieldMatcher.match(ALL_DATA_TYPES_SCHEMA));
  }

  @Test
  public void testFieldMatcher_match_TypeName() {
    assertTrue(
        FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME, TypeName.ROW).match(NON_USER_WITH_ROW_PAYLOAD));
    assertTrue(
        FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME, TypeName.BYTES)
            .match(NON_USER_WITH_BYTES_PAYLOAD));
    assertFalse(
        FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME, TypeName.ROW).match(NON_USER_WITH_BYTES_PAYLOAD));
    assertFalse(
        FieldMatcher.of(DEFAULT_PAYLOAD_KEY_NAME, TypeName.BYTES).match(NON_USER_WITH_ROW_PAYLOAD));
  }

  @Test
  public void testFieldMatcher_match_FieldType() {
    assertTrue(
        FieldMatcher.of(
                DEFAULT_ATTRIBUTES_KEY_NAME, FieldType.map(FieldType.STRING, FieldType.STRING))
            .match(NON_USER_WITH_BYTES_PAYLOAD));
    assertFalse(
        FieldMatcher.of(
                DEFAULT_ATTRIBUTES_KEY_NAME, FieldType.map(FieldType.STRING, FieldType.BYTES))
            .match(NON_USER_WITH_BYTES_PAYLOAD));
  }

  @Test
  public void testRemoveFields() {
    Schema input = Schema.of(STRING_FIELD, BOOLEAN_FIELD, INT64_FIELD);
    Schema expected = Schema.builder().build();
    Schema actual =
        removeFields(input, STRING_FIELD.getName(), BOOLEAN_FIELD.getName(), INT64_FIELD.getName());
    assertEquals(expected, actual);
  }

  @Test
  public void testPubsubRowToMessageParDo_attributesWithoutTimestamp() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("a", "1");
    attributes.put("b", "2");
    attributes.put("c", "3");

    Row withAttributes =
        Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
            .addValues(attributes, Instant.now(), new byte[] {})
            .build();

    assertEquals(
        attributes,
        doFn(NON_USER_WITH_BYTES_PAYLOAD, null).attributesWithoutTimestamp(withAttributes));

    Schema withoutAttributesSchema =
        removeFields(NON_USER_WITH_BYTES_PAYLOAD, DEFAULT_ATTRIBUTES_KEY_NAME);
    Row withoutAttributes =
        Row.withSchema(withoutAttributesSchema).addValues(Instant.now(), new byte[] {}).build();

    assertEquals(
        ImmutableMap.of(),
        doFn(withoutAttributesSchema, null).attributesWithoutTimestamp(withoutAttributes));
  }

  @Test
  public void testPubsubRowToMessageParDo_timestamp() {
    Instant timestamp = Instant.now();
    Row withTimestamp =
        Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
            .addValues(ImmutableMap.of(), timestamp, new byte[] {})
            .build();
    assertEquals(
        timestamp.toString(),
        doFn(NON_USER_WITH_BYTES_PAYLOAD, null).timestamp(withTimestamp).toString());

    Schema withoutTimestampSchema =
        removeFields(NON_USER_WITH_BYTES_PAYLOAD, DEFAULT_EVENT_TIMESTAMP_KEY_NAME);
    Row withoutTimestamp =
        Row.withSchema(withoutTimestampSchema).addValues(ImmutableMap.of(), new byte[] {}).build();
    ReadableDateTime actual = doFn(withoutTimestampSchema, null).timestamp(withoutTimestamp);
    assertNotNull(actual);
  }

  @Test
  public void testPubsubRowToMessageParDo_payload() {
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    PayloadSerializer avroPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    byte[] bytes = "abcdefg".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(
        bytes,
        doFn(NON_USER_WITH_BYTES_PAYLOAD, null)
            .payload(
                Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
                    .addValues(ImmutableMap.of(), Instant.now(), bytes)
                    .build()));

    Row withoutUserFields =
        Row.withSchema(NON_USER_WITH_ROW_PAYLOAD)
            .addValues(
                ImmutableMap.of(),
                Instant.now(),
                rowWithAllDataTypes(
                    true,
                    (byte) 1,
                    Instant.now().toDateTime(),
                    BigDecimal.valueOf(100L),
                    1.12345,
                    1.1f,
                    (short) 1,
                    1,
                    1L,
                    "abcdefg"))
            .build();

    assertArrayEquals(
        jsonPayloadSerializer.serialize(withoutUserFields.getRow(DEFAULT_PAYLOAD_KEY_NAME)),
        doFn(NON_USER_WITH_ROW_PAYLOAD, jsonPayloadSerializer).payload(withoutUserFields));

    assertArrayEquals(
        avroPayloadSerializer.serialize(withoutUserFields.getRow(DEFAULT_PAYLOAD_KEY_NAME)),
        doFn(NON_USER_WITH_ROW_PAYLOAD, avroPayloadSerializer).payload(withoutUserFields));

    Map<String, String> attributes = new HashMap<>();
    attributes.put("a", "1");
    attributes.put("b", "2");
    attributes.put("c", "3");

    Row withAttributesAndTimestamp =
        Row.withSchema(NON_USER_WITHOUT_PAYLOAD).addValues(attributes, Instant.now()).build();

    Row withUserFields =
        rowWithAllDataTypes(
            false,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(200L),
            2.12345,
            2.1f,
            (short) 2,
            2,
            2L,
            "1234567");

    Row withUserFieldsAndAttributesTimestamp = merge(withAttributesAndTimestamp, withUserFields);

    assertArrayEquals(
        jsonPayloadSerializer.serialize(withUserFields),
        doFn(withUserFieldsAndAttributesTimestamp.getSchema(), jsonPayloadSerializer)
            .payload(withUserFieldsAndAttributesTimestamp));

    assertArrayEquals(
        avroPayloadSerializer.serialize(withUserFields),
        doFn(withUserFieldsAndAttributesTimestamp.getSchema(), avroPayloadSerializer)
            .payload(withUserFieldsAndAttributesTimestamp));
  }

  @Test
  public void testPubsubRowToMessageParDo_serializableRow() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            doFn(NON_USER_WITH_BYTES_PAYLOAD, null)
                .serializableRow(
                    Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
                        .attachValues(ImmutableMap.of(), Instant.now(), new byte[] {})));

    Map<String, String> attributes = new HashMap<>();
    attributes.put("a", "1");
    attributes.put("b", "2");
    attributes.put("c", "3");

    Row withAllDataTypes =
        rowWithAllDataTypes(
            true,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(1L),
            3.12345,
            4.1f,
            (short) 5,
            2,
            7L,
            "asdfjkl;");

    Row nonUserWithRowPayload =
        Row.withSchema(NON_USER_WITH_ROW_PAYLOAD)
            .attachValues(attributes, Instant.now(), withAllDataTypes);

    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());

    assertEquals(
        withAllDataTypes,
        doFn(NON_USER_WITH_ROW_PAYLOAD, jsonPayloadSerializer)
            .serializableRow(nonUserWithRowPayload));

    Row withAttributesAndTimestamp =
        Row.withSchema(NON_USER_WITHOUT_PAYLOAD).addValues(attributes, Instant.now()).build();

    Row withUserFieldsAttributesAndTimestamp = merge(withAttributesAndTimestamp, withAllDataTypes);

    assertEquals(
        withAllDataTypes,
        doFn(withUserFieldsAttributesAndTimestamp.getSchema(), jsonPayloadSerializer)
            .serializableRow(withUserFieldsAttributesAndTimestamp));
  }

  private static PubsubRowToMessageDoFn doFn(Schema schema, PayloadSerializer payloadSerializer) {
    return new PubsubRowToMessageDoFn(
        DEFAULT_ATTRIBUTES_KEY_NAME,
        DEFAULT_EVENT_TIMESTAMP_KEY_NAME,
        DEFAULT_PAYLOAD_KEY_NAME,
        errorSchema(schema),
        DEFAULT_EVENT_TIMESTAMP_KEY_NAME,
        null,
        payloadSerializer);
  }

  static Row rowWithAllDataTypes(
      boolean boolean0,
      byte byte0,
      ReadableDateTime datetime,
      BigDecimal decimal,
      Double double0,
      Float float0,
      Short int16,
      Integer int32,
      Long int64,
      String string) {
    return Row.withSchema(ALL_DATA_TYPES_SCHEMA)
        .addValues(boolean0, byte0, datetime, decimal, double0, float0, int16, int32, int64, string)
        .build();
  }

  private static Schema merge(Schema a, Schema b) {
    List<Field> fields = new ArrayList<>(a.getFields());
    fields.addAll(b.getFields());
    Schema.Builder builder = Schema.builder();
    for (Field field : fields) {
      builder = builder.addField(field);
    }
    return builder.build();
  }

  private static Row merge(Row a, Row b) {
    Schema schema = merge(a.getSchema(), b.getSchema());
    Map<String, Object> values = new HashMap<>();
    for (String name : a.getSchema().getFieldNames()) {
      values.put(name, a.getValue(name));
    }
    for (String name : b.getSchema().getFieldNames()) {
      values.put(name, b.getValue(name));
    }
    return Row.withSchema(schema).withFieldValues(values).build();
  }
}

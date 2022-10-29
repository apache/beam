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
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_KEY_PREFIX;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_KEY_NAME;
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
import org.apache.beam.sdk.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.Row;
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
  private static final String DEFAULT_ATTRIBUTES_KEY_NAME =
      DEFAULT_KEY_PREFIX + ATTRIBUTES_KEY_NAME;
  private static final String DEFAULT_EVENT_TIMESTAMP_KEY_NAME =
      DEFAULT_KEY_PREFIX + EVENT_TIMESTAMP_KEY_NAME;
  private static final String DEFAULT_PAYLOAD_KEY_NAME = DEFAULT_KEY_PREFIX + PAYLOAD_KEY_NAME;
  private static final Field BOOLEAN_FIELD = Field.of("boolean", FieldType.BOOLEAN);
  private static final Field BYTE_FIELD = Field.of("byte", FieldType.BYTE);
  private static final Field DATETIME_FIELD = Field.of("datetime", FieldType.DATETIME);
  private static final Field DECIMAL_FIELD = Field.of("decimal", FieldType.DECIMAL);
  private static final Field DOUBLE_FIELD = Field.of("double", FieldType.DOUBLE);
  private static final Field FLOAT_FIELD = Field.of("float", FieldType.FLOAT);
  private static final Field INT16_FIELD = Field.of("int16", FieldType.INT16);
  private static final Field INT32_FIELD = Field.of("int32", FieldType.INT32);
  private static final Field INT64_FIELD = Field.of("int64", FieldType.INT64);
  private static final Field STRING_FIELD = Field.of("string", FieldType.STRING);
  private static final Schema ALL_DATA_TYPES_SCHEMA =
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

  private static final Schema NON_USER_WITH_BYTES_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
          Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES));

  private static final Schema NON_USER_WITH_ROW_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE),
          Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA)));

  private static final Schema NON_USER_WITHOUT_PAYLOAD =
      Schema.of(
          Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE),
          Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE));

  // private static final Field ROW_FIELD = Field.of("row", FieldType.row(ALL_DATA_TYPES_SCHEMA));

  static {
    PIPELINE_OPTIONS.setStableUniqueNames(CheckEnabled.OFF);
  }

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testExpand() {
    pipeline.run(PIPELINE_OPTIONS);
  }

  @Test
  public void testPubsubRowToMessageParDo_Process() {
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
  public void testValidate() {}

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
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, new HashMap<>());
    PayloadSerializer avroPayloadSerializer =
        new AvroPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, new HashMap<>());

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
        new HashMap<>(),
        doFn(withoutAttributesSchema, null).attributesWithoutTimestamp(withoutAttributes));
  }

  @Test
  public void testPubsubRowToMessageParDo_timestamp() {
    Instant timestamp = Instant.now();
    Row withTimestamp =
        Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
            .addValues(new HashMap<>(), timestamp, new byte[] {})
            .build();
    assertEquals(
        timestamp.toString(),
        doFn(NON_USER_WITH_BYTES_PAYLOAD, null).timestamp(withTimestamp).toString());

    Schema withoutTimestampSchema =
        removeFields(NON_USER_WITH_BYTES_PAYLOAD, DEFAULT_EVENT_TIMESTAMP_KEY_NAME);
    Row withoutTimestamp =
        Row.withSchema(withoutTimestampSchema).addValues(new HashMap<>(), new byte[] {}).build();
    ReadableDateTime actual = doFn(withoutTimestampSchema, null).timestamp(withoutTimestamp);
    assertNotNull(actual);
    assertTrue(actual.isAfter(timestamp));
  }

  @Test
  public void testPubsubRowToMessageParDo_payload() {
    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, new HashMap<>());
    PayloadSerializer avroPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, new HashMap<>());

    byte[] bytes = "abcdefg".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(
        bytes,
        doFn(NON_USER_WITH_BYTES_PAYLOAD, null)
            .payload(
                Row.withSchema(NON_USER_WITH_BYTES_PAYLOAD)
                    .addValues(new HashMap<>(), Instant.now(), bytes)
                    .build()));

    Row withoutUserFields =
        Row.withSchema(NON_USER_WITH_ROW_PAYLOAD)
            .addValues(
                new HashMap<>(),
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
                        .attachValues(new HashMap<>(), Instant.now(), new byte[] {})));

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
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, new HashMap<>());

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
        payloadSerializer);
  }

  private static Row rowWithAllDataTypes(
      boolean _boolean,
      byte _byte,
      ReadableDateTime datetime,
      BigDecimal decimal,
      Double _double,
      Float _float,
      Short int16,
      Integer int32,
      Long int64,
      String string) {
    return Row.withSchema(ALL_DATA_TYPES_SCHEMA)
        .addValues(_boolean, _byte, datetime, decimal, _double, _float, int16, int32, int64, string)
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

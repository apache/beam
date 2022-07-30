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

import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.ATTRIBUTES_FIELD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageToRow.PAYLOAD_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link PubsubSchemaTransformMessageToRowFactory}. */
@RunWith(JUnit4.class)
public class PubsubSchemaTransformMessageToRowFactoryTest {

  List<TestCase> cases =
      Arrays.asList(
          testCase(PubsubSchemaTransformReadConfiguration.builder().setDataSchema(SCHEMA))
              .expectPayloadSerializerProvider(JSON_PAYLOAD_SERIALIZER_PROVIDER)
              .withSerializerInput(),
          testCase(PubsubSchemaTransformReadConfiguration.builder().setDataSchema(SCHEMA))
              .expectPubsubToRow(
                  PubsubMessageToRow.builder()
                      .messageSchema(SCHEMA)
                      .useFlatSchema(true)
                      .useDlq(false)),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(SCHEMA)
                      .setDeadLetterQueue("projects/project/topics/topic"))
              .expectPubsubToRow(
                  PubsubMessageToRow.builder()
                      .messageSchema(SCHEMA)
                      .useFlatSchema(true)
                      .useDlq(true)),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(SCHEMA)
                      .setFormat("avro"))
              .expectPayloadSerializerProvider(AVRO_PAYLOAD_SERIALIZER_PROVIDER)
              .withSerializerInput(),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(Schema.of(ATTRIBUTES_FIELD_ARRAY)))
              .schemaShouldHaveValidAttributesField()
              .fieldShouldBePresent(
                  ATTRIBUTES_FIELD_ARRAY.getName(), ATTRIBUTES_FIELD_ARRAY.getType()),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(Schema.of(ATTRIBUTES_FIELD_MAP)))
              .schemaShouldHaveValidAttributesField()
              .fieldShouldBePresent(ATTRIBUTES_FIELD_MAP.getName(), ATTRIBUTES_FIELD_MAP.getType()),
          testCase(
              PubsubSchemaTransformReadConfiguration.builder()
                  .setDataSchema(Schema.of(ATTRIBUTES_FIELD_SHOULD_NOT_MATCH))),
          testCase(
              PubsubSchemaTransformReadConfiguration.builder()
                  .setDataSchema(Schema.of(PAYLOAD_FIELD_SHOULD_NOT_MATCH))),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(Schema.of(PAYLOAD_FIELD_BYTES)))
              .schemaShouldHaveValidPayloadField()
              .fieldShouldBePresent(PAYLOAD_FIELD_BYTES.getName(), PAYLOAD_FIELD_BYTES.getType()),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(Schema.of(PAYLOAD_FIELD_ROW)))
              .schemaShouldHaveValidPayloadField()
              .fieldShouldBePresent(PAYLOAD_FIELD_ROW.getName(), PAYLOAD_FIELD_ROW.getType()),
          testCase(
                  PubsubSchemaTransformReadConfiguration.builder()
                      .setDataSchema(Schema.of(ATTRIBUTES_FIELD_ARRAY, PAYLOAD_FIELD_BYTES)))
              .schemaShouldHaveValidAttributesField()
              .schemaShouldHaveValidPayloadField()
              .shouldUseNestedSchema()
              .shouldNotNeedSerializer()
              .expectPubsubToRow(
                  PubsubMessageToRow.builder()
                      .messageSchema(Schema.of(ATTRIBUTES_FIELD_ARRAY, PAYLOAD_FIELD_BYTES))
                      .useFlatSchema(false)
                      .useDlq(false)));

  static final Schema.FieldType ATTRIBUTE_MAP_FIELD_TYPE =
      Schema.FieldType.map(Schema.FieldType.STRING.withNullable(false), Schema.FieldType.STRING);
  static final Schema ATTRIBUTE_ARRAY_ENTRY_SCHEMA =
      Schema.builder().addStringField("key").addStringField("value").build();

  static final Schema.FieldType ATTRIBUTE_ARRAY_FIELD_TYPE =
      Schema.FieldType.array(Schema.FieldType.row(ATTRIBUTE_ARRAY_ENTRY_SCHEMA));

  private static final Schema.Field ATTRIBUTES_FIELD_SHOULD_NOT_MATCH =
      Schema.Field.of(ATTRIBUTES_FIELD, Schema.FieldType.STRING);

  private static final Schema.Field ATTRIBUTES_FIELD_MAP =
      Schema.Field.of(ATTRIBUTES_FIELD, ATTRIBUTE_MAP_FIELD_TYPE);

  private static final Schema.Field ATTRIBUTES_FIELD_ARRAY =
      Schema.Field.of(ATTRIBUTES_FIELD, ATTRIBUTE_ARRAY_FIELD_TYPE);

  private static final Schema.Field PAYLOAD_FIELD_SHOULD_NOT_MATCH =
      Schema.Field.of(PAYLOAD_FIELD, Schema.FieldType.STRING);

  private static final Schema.Field PAYLOAD_FIELD_BYTES =
      Schema.Field.of(PAYLOAD_FIELD, Schema.FieldType.BYTES);

  private static final Schema.Field PAYLOAD_FIELD_ROW =
      Schema.Field.of(PAYLOAD_FIELD, Schema.FieldType.row(Schema.of()));

  private static final PayloadSerializerProvider JSON_PAYLOAD_SERIALIZER_PROVIDER =
      new JsonPayloadSerializerProvider();

  private static final AvroPayloadSerializerProvider AVRO_PAYLOAD_SERIALIZER_PROVIDER =
      new AvroPayloadSerializerProvider();

  private static final Schema SCHEMA =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("number", Schema.FieldType.INT64));

  private static final Row ROW =
      Row.withSchema(SCHEMA).withFieldValue("name", "a").withFieldValue("number", 1L).build();

  @Test
  public void testBuildMessageToRow() {
    for (TestCase testCase : cases) {
      if (testCase.expectPubsubToRow == null) {
        continue;
      }

      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      PubsubMessageToRow expected = testCase.expectPubsubToRow;
      PubsubMessageToRow actual = factory.buildMessageToRow();

      assertEquals("messageSchema", expected.messageSchema(), actual.messageSchema());
      assertEquals("useFlatSchema", expected.useFlatSchema(), actual.useFlatSchema());
      assertEquals("useDlq", expected.useDlq(), actual.useDlq());
    }
  }

  @Test
  public void serializer() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      if (testCase.expectPayloadSerializerProvider == null) {
        continue;
      }

      Row serializerInput = testCase.serializerInput;

      byte[] expectedBytes =
          testCase
              .expectSerializerProvider()
              .apply(testCase.dataSchema())
              .serialize(serializerInput);

      byte[] actualBytes =
          factory.serializer().apply(testCase.dataSchema()).serialize(serializerInput);

      String expected = new String(expectedBytes, StandardCharsets.UTF_8);
      String actual = new String(actualBytes, StandardCharsets.UTF_8);

      assertEquals(expected, actual);
    }
  }

  @Test
  public void needsSerializer() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      boolean expected = testCase.shouldNeedSerializer;
      boolean actual = factory.needsSerializer();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void shouldUseNestedSchema() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      boolean expected = testCase.shouldUseNestedSchema;
      boolean actual = factory.shouldUseNestedSchema();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void schemaHasValidPayloadField() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      boolean expected = testCase.shouldSchemaHaveValidPayloadField;
      boolean actual = factory.schemaHasValidPayloadField();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void schemaHasValidAttributesField() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();

      boolean expected = testCase.shouldSchemaHaveValidAttributesField;
      boolean actual = factory.schemaHasValidAttributesField();

      assertEquals(expected, actual);
    }
  }

  @Test
  public void fieldPresent() {
    for (TestCase testCase : cases) {
      PubsubSchemaTransformMessageToRowFactory factory = testCase.factory();
      for (Entry<String, FieldType> entry : testCase.shouldFieldPresent.entrySet()) {

        boolean actual = factory.fieldPresent(entry.getKey(), entry.getValue());

        assertTrue(actual);
      }
    }
  }

  static TestCase testCase(PubsubSchemaTransformReadConfiguration.Builder configurationBuilder) {
    return new TestCase(configurationBuilder);
  }

  private static class TestCase {
    private final PubsubSchemaTransformReadConfiguration configuration;

    private PubsubMessageToRow expectPubsubToRow;

    private PayloadSerializerProvider expectPayloadSerializerProvider;

    private boolean shouldUseNestedSchema = false;
    private boolean shouldNeedSerializer = true;
    private boolean shouldSchemaHaveValidPayloadField = false;
    private boolean shouldSchemaHaveValidAttributesField = false;
    private final Map<String, FieldType> shouldFieldPresent = new HashMap<>();

    private Row serializerInput;

    TestCase(PubsubSchemaTransformReadConfiguration.Builder configurationBuilder) {
      this.configuration = configurationBuilder.build();
    }

    PubsubSchemaTransformMessageToRowFactory factory() {
      return PubsubSchemaTransformMessageToRowFactory.from(configuration);
    }

    Schema dataSchema() {
      return configuration.getDataSchema();
    }

    TestCase expectPubsubToRow(PubsubMessageToRow.Builder pubsubMessageToRowBuilder) {
      this.expectPubsubToRow = pubsubMessageToRowBuilder.build();
      return this;
    }

    TestCase withSerializerInput() {
      this.serializerInput = PubsubSchemaTransformMessageToRowFactoryTest.ROW;
      return this;
    }

    TestCase expectPayloadSerializerProvider(PayloadSerializerProvider value) {
      this.expectPayloadSerializerProvider = value;
      return this;
    }

    PubsubMessageToRow.SerializerProvider expectSerializerProvider() {
      Map<String, Object> params = new HashMap<>();
      PayloadSerializer payloadSerializer =
          expectPayloadSerializerProvider.getSerializer(configuration.getDataSchema(), params);

      return (input -> payloadSerializer);
    }

    TestCase shouldUseNestedSchema() {
      this.shouldUseNestedSchema = true;
      return this;
    }

    TestCase shouldNotNeedSerializer() {
      this.shouldNeedSerializer = false;
      return this;
    }

    TestCase schemaShouldHaveValidPayloadField() {
      this.shouldSchemaHaveValidPayloadField = true;
      return this;
    }

    TestCase schemaShouldHaveValidAttributesField() {
      this.shouldSchemaHaveValidAttributesField = true;
      return this;
    }

    TestCase fieldShouldBePresent(String name, Schema.FieldType expectedType) {
      this.shouldFieldPresent.put(name, expectedType);
      return this;
    }
  }
}

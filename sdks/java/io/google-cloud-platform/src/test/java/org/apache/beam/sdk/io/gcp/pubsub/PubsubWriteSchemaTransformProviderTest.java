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
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_ATTRIBUTES_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_EVENT_TIMESTAMP_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.DEFAULT_PAYLOAD_KEY_NAME;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessage.EVENT_TIMESTAMP_FIELD_TYPE;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessageTest.ALL_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessageTest.NON_USER_WITH_BYTES_PAYLOAD;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubRowToMessageTest.rowWithAllDataTypes;
import static org.apache.beam.sdk.io.gcp.pubsub.PubsubWriteSchemaTransformProvider.INPUT_TAG;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import com.google.api.client.util.Clock;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.SchemaParseException;
import org.apache.beam.sdk.extensions.avro.schemas.io.payloads.AvroPayloadSerializerProvider;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.SchemaPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubClient.TopicPath;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubTestClient.PubsubTestClientFactory;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubWriteSchemaTransformProvider.PubsubWriteSchemaTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.io.payloads.JsonPayloadSerializerProvider;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.RowJson.UnsupportedRowJsonException;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PubsubWriteSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class PubsubWriteSchemaTransformProviderTest {

  private static final String ID_ATTRIBUTE = "id_attribute";
  private static final String TOPIC = "projects/project/topics/topic";
  private static final MockClock CLOCK = new MockClock(Instant.now());
  private static final AutoValueSchema AUTO_VALUE_SCHEMA = new AutoValueSchema();
  private static final TypeDescriptor<PubsubWriteSchemaTransformConfiguration> TYPE_DESCRIPTOR =
      TypeDescriptor.of(PubsubWriteSchemaTransformConfiguration.class);
  private static final SerializableFunction<PubsubWriteSchemaTransformConfiguration, Row> TO_ROW =
      AUTO_VALUE_SCHEMA.toRowFunction(TYPE_DESCRIPTOR);

  private static final PipelineOptions OPTIONS = PipelineOptionsFactory.create();

  static {
    OPTIONS.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testBuildPubsubWrite() {
    assertEquals(
        "default configuration should yield a topic Pub/Sub write",
        pubsubWrite(),
        transform(configurationBuilder()).buildPubsubWrite());

    assertEquals(
        "idAttribute in configuration should yield a idAttribute set Pub/Sub write",
        pubsubWrite().withIdAttribute(ID_ATTRIBUTE),
        transform(configurationBuilder().setIdAttribute(ID_ATTRIBUTE)).buildPubsubWrite());
  }

  @Test
  public void testBuildPubsubRowToMessage() {
    assertEquals(
        "override timestamp attribute on configuration should yield a PubsubRowToMessage with target timestamp",
        rowToMessageBuilder().setTargetTimestampAttributeName("custom_timestamp_attribute").build(),
        transform(
                configurationBuilder()
                    .setTarget(
                        PubsubWriteSchemaTransformConfiguration.targetConfigurationBuilder()
                            .setTimestampAttributeKey("custom_timestamp_attribute")
                            .build()))
            .buildPubsubRowToMessage(NON_USER_WITH_BYTES_PAYLOAD));

    assertNull(
        "failing to set format should yield a null payload serializer",
        transform(configurationBuilder())
            .buildPubsubRowToMessage(ALL_DATA_TYPES_SCHEMA)
            .getPayloadSerializer());

    assertThrows(
        "setting 'json' format for a unsupported field containing Schema should throw an Exception",
        UnsupportedRowJsonException.class,
        () ->
            transform(configurationBuilder().setFormat("json"))
                .buildPubsubRowToMessage(
                    Schema.of(Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE))));

    assertThrows(
        "setting 'avro' format for a unsupported field containing Schema should throw an Exception",
        SchemaParseException.class,
        () ->
            transform(configurationBuilder().setFormat("avro"))
                .buildPubsubRowToMessage(
                    Schema.of(Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE))));

    assertNotNull(
        "setting 'json' format for valid schema should yield PayloadSerializer",
        transform(configurationBuilder().setFormat("json"))
            .buildPubsubRowToMessage(ALL_DATA_TYPES_SCHEMA)
            .getPayloadSerializer());

    assertNotNull(
        "setting 'avro' format for valid schema should yield PayloadSerializer",
        transform(configurationBuilder().setFormat("avro"))
            .buildPubsubRowToMessage(ALL_DATA_TYPES_SCHEMA)
            .getPayloadSerializer());
  }

  @Test
  public void testInvalidTaggedInput() {
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

    PCollection<Row> rows =
        pipeline.apply(Create.of(withAllDataTypes)).setRowSchema(ALL_DATA_TYPES_SCHEMA);

    assertThrows(
        "empty input should not be allowed",
        IllegalArgumentException.class,
        () -> transform(configurationBuilder()).expand(PCollectionRowTuple.empty(pipeline)));

    assertThrows(
        "input with >1 tagged rows should not be allowed",
        IllegalArgumentException.class,
        () ->
            transform(configurationBuilder())
                .expand(PCollectionRowTuple.of(INPUT_TAG, rows).and("somethingelse", rows)));

    assertThrows(
        "input missing INPUT tag should not be allowed",
        IllegalArgumentException.class,
        () ->
            transform(configurationBuilder())
                .expand(PCollectionRowTuple.of("somethingelse", rows)));

    pipeline.run(OPTIONS);
  }

  @Test
  public void testValidateSourceSchemaAgainstConfiguration() {
    // Only containing user fields and no configuration details should be valid
    transform(configurationBuilder())
        .validateSourceSchemaAgainstConfiguration(ALL_DATA_TYPES_SCHEMA);

    // Matching attributes, timestamp, and payload (bytes) fields configured with expected types
    // should be valid
    transform(
            configurationBuilder()
                .setSource(
                    PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                        .setAttributesFieldName("attributes")
                        .setTimestampFieldName("timestamp")
                        .setPayloadFieldName("payload")
                        .build()))
        .validateSourceSchemaAgainstConfiguration(
            Schema.of(
                Field.of("attributes", ATTRIBUTES_FIELD_TYPE),
                Field.of("timestamp", EVENT_TIMESTAMP_FIELD_TYPE),
                Field.of("payload", Schema.FieldType.BYTES)));

    // Matching attributes, timestamp, and payload (ROW) fields configured with expected types
    // should be valid
    transform(
            configurationBuilder()
                .setSource(
                    PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                        .setAttributesFieldName("attributes")
                        .setTimestampFieldName("timestamp")
                        .setPayloadFieldName("payload")
                        .build()))
        .validateSourceSchemaAgainstConfiguration(
            Schema.of(
                Field.of("attributes", ATTRIBUTES_FIELD_TYPE),
                Field.of("timestamp", EVENT_TIMESTAMP_FIELD_TYPE),
                Field.of("payload", Schema.FieldType.row(ALL_DATA_TYPES_SCHEMA))));

    assertThrows(
        "empty Schema should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(configurationBuilder())
                .validateSourceSchemaAgainstConfiguration(Schema.of()));

    assertThrows(
        "attributes field in configuration but not in schema should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setAttributesFieldName("attributes")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(ALL_DATA_TYPES_SCHEMA));

    assertThrows(
        "timestamp field in configuration but not in schema should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setTimestampFieldName("timestamp")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(ALL_DATA_TYPES_SCHEMA));

    assertThrows(
        "payload field in configuration but not in schema should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setPayloadFieldName("payload")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(ALL_DATA_TYPES_SCHEMA));

    assertThrows(
        "attributes field in configuration but mismatching attributes type should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setAttributesFieldName("attributes")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(
                    // should be FieldType.map(FieldType.STRING, FieldType.STRING)
                    Schema.of(
                        Field.of("attributes", FieldType.map(FieldType.BYTES, FieldType.STRING)))));

    assertThrows(
        "timestamp field in configuration but mismatching timestamp type should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setAttributesFieldName("timestamp")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(
                    // should be FieldType.DATETIME
                    Schema.of(Field.of("timestamp", FieldType.STRING))));

    assertThrows(
        "payload field in configuration but mismatching payload type should be invalid",
        IllegalArgumentException.class,
        () ->
            transform(
                    configurationBuilder()
                        .setSource(
                            PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                                .setAttributesFieldName("payload")
                                .build()))
                .validateSourceSchemaAgainstConfiguration(
                    // should be FieldType.BYTES or FieldType.row(...)
                    Schema.of(Field.of("payload", FieldType.STRING))));
  }

  @Test
  public void testValidateTargetSchemaAgainstPubsubSchema() throws IOException {
    TopicPath topicPath = PubsubClient.topicPathFromPath(TOPIC);
    PubsubTestClientFactory noSchemaFactory =
        PubsubTestClient.createFactoryForGetSchema(topicPath, null, null);

    PubsubTestClientFactory schemaDeletedFactory =
        PubsubTestClient.createFactoryForGetSchema(topicPath, SchemaPath.DELETED_SCHEMA, null);

    PubsubTestClientFactory mismatchingSchemaFactory =
        PubsubTestClient.createFactoryForGetSchema(
            topicPath,
            PubsubClient.schemaPathFromId("testProject", "misMatch"),
            Schema.of(Field.of("StringField", FieldType.STRING)));

    PubsubTestClientFactory matchingSchemaFactory =
        PubsubTestClient.createFactoryForGetSchema(
            topicPath,
            PubsubClient.schemaPathFromId("testProject", "match"),
            ALL_DATA_TYPES_SCHEMA);

    // Should pass validation exceptions if Pub/Sub topic lacks schema
    transform(configurationBuilder())
        .withPubsubClientFactory(noSchemaFactory)
        .validateTargetSchemaAgainstPubsubSchema(ALL_DATA_TYPES_SCHEMA, OPTIONS);
    noSchemaFactory.close();

    // Should pass validation if Pub/Sub topic schema deleted
    transform(configurationBuilder())
        .withPubsubClientFactory(schemaDeletedFactory)
        .validateTargetSchemaAgainstPubsubSchema(ALL_DATA_TYPES_SCHEMA, OPTIONS);
    schemaDeletedFactory.close();

    assertThrows(
        "mismatched schema should be detected from Pub/Sub topic",
        IllegalStateException.class,
        () ->
            transform(configurationBuilder())
                .withPubsubClientFactory(mismatchingSchemaFactory)
                .validateTargetSchemaAgainstPubsubSchema(ALL_DATA_TYPES_SCHEMA, OPTIONS));
    mismatchingSchemaFactory.close();

    // Should pass validation if Pub/Sub topic schema matches
    transform(configurationBuilder())
        .withPubsubClientFactory(matchingSchemaFactory)
        .validateTargetSchemaAgainstPubsubSchema(ALL_DATA_TYPES_SCHEMA, OPTIONS);
    matchingSchemaFactory.close();
  }

  @Test
  public void testBuildTargetSchema() {

    Field sourceAttributesField = Field.of("attributes", ATTRIBUTES_FIELD_TYPE);
    Field sourceTimestampField = Field.of("timestamp", EVENT_TIMESTAMP_FIELD_TYPE);
    Field sourcePayloadBytesField = Field.of("payload", FieldType.BYTES);
    Field sourcePayloadRowField = Field.of("payload", FieldType.row(ALL_DATA_TYPES_SCHEMA));

    Field targetAttributesField = Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE);
    Field targetTimestampField =
        Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE);
    Field targetPayloadBytesField = Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES);
    Field targetPayloadRowField =
        Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA));

    assertEquals(
        "attributes and timestamp field should append to user fields",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .build()))
            .buildTargetSchema(ALL_DATA_TYPES_SCHEMA));

    assertEquals(
        "timestamp field should append to user fields; attributes field name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setAttributesFieldName("attributes")
                            .build()))
            .buildTargetSchema(
                Schema.builder()
                    .addField(sourceAttributesField)
                    .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                    .build()));

    assertEquals(
        "attributes field should append to user fields; timestamp field name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setTimestampFieldName("timestamp")
                            .build()))
            .buildTargetSchema(
                Schema.builder()
                    .addField(sourceTimestampField)
                    .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                    .build()));

    assertEquals(
        "attributes and timestamp field appended to user payload bytes field; payload field name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addField(targetPayloadBytesField)
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setPayloadFieldName("payload")
                            .build()))
            .buildTargetSchema(Schema.builder().addField(sourcePayloadBytesField).build()));

    assertEquals(
        "attributes and timestamp field appended to user payload row field; payload field name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addField(targetPayloadRowField)
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setPayloadFieldName("payload")
                            .build()))
            .buildTargetSchema(Schema.builder().addField(sourcePayloadRowField).build()));

    assertEquals(
        "attributes and timestamp fields name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setAttributesFieldName("attributes")
                            .setTimestampFieldName("timestamp")
                            .build()))
            .buildTargetSchema(
                Schema.builder()
                    .addField(sourceAttributesField)
                    .addField(sourceTimestampField)
                    .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                    .build()));

    assertEquals(
        "attributes, timestamp, payload bytes fields name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(targetPayloadBytesField)
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setAttributesFieldName("attributes")
                            .setTimestampFieldName("timestamp")
                            .setPayloadFieldName("payload")
                            .build()))
            .buildTargetSchema(
                Schema.builder()
                    .addField(sourceAttributesField)
                    .addField(sourceTimestampField)
                    .addField(sourcePayloadBytesField)
                    .build()));

    assertEquals(
        "attributes, timestamp, payload row fields name changed",
        Schema.builder()
            .addField(targetAttributesField)
            .addField(targetTimestampField)
            .addFields(targetPayloadRowField)
            .build(),
        transform(
                configurationBuilder()
                    .setSource(
                        PubsubWriteSchemaTransformConfiguration.sourceConfigurationBuilder()
                            .setAttributesFieldName("attributes")
                            .setTimestampFieldName("timestamp")
                            .setPayloadFieldName("payload")
                            .build()))
            .buildTargetSchema(
                Schema.builder()
                    .addField(sourceAttributesField)
                    .addField(sourceTimestampField)
                    .addField(sourcePayloadRowField)
                    .build()));
  }

  @Test
  public void testConvertForRowToMessageTransform() {
    Row userRow =
        rowWithAllDataTypes(
            false,
            (byte) 0,
            Instant.ofEpochMilli(CLOCK.currentTimeMillis()).toDateTime(),
            BigDecimal.valueOf(1L),
            1.12345,
            1.1f,
            (short) 1,
            1,
            1L,
            "吃葡萄不吐葡萄皮，不吃葡萄倒吐葡萄皮");

    Field sourceAttributes = Field.of("attributes", ATTRIBUTES_FIELD_TYPE);
    Field targetAttributes = Field.of(DEFAULT_ATTRIBUTES_KEY_NAME, ATTRIBUTES_FIELD_TYPE);

    Field sourceTimestamp = Field.of("timestamp", EVENT_TIMESTAMP_FIELD_TYPE);
    Field targetTimestamp = Field.of(DEFAULT_EVENT_TIMESTAMP_KEY_NAME, EVENT_TIMESTAMP_FIELD_TYPE);

    Field sourcePayloadBytes = Field.of("payload", FieldType.BYTES);
    Field targetPayloadBytes = Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.BYTES);

    Field sourcePayloadRow = Field.of("payload", FieldType.row(ALL_DATA_TYPES_SCHEMA));
    Field targetPayloadRow =
        Field.of(DEFAULT_PAYLOAD_KEY_NAME, FieldType.row(ALL_DATA_TYPES_SCHEMA));

    Map<String, String> attributes = ImmutableMap.of("a", "1");
    Instant generatedTimestamp = Instant.ofEpochMilli(CLOCK.currentTimeMillis());
    Instant timestampFromSource = Instant.ofEpochMilli(CLOCK.currentTimeMillis() + 10000L);
    byte[] payloadBytes = "吃葡萄不吐葡萄皮，不吃葡萄倒吐葡萄皮".getBytes(StandardCharsets.UTF_8);

    PAssert.that(
            "attributes only source yields attributes + timestamp target",
            pipeline
                .apply(
                    Create.of(Row.withSchema(Schema.of(sourceAttributes)).attachValues(attributes)))
                .setRowSchema(Schema.of(sourceAttributes))
                .apply(
                    transform(
                            configurationBuilder()
                                .setSource(
                                    PubsubWriteSchemaTransformConfiguration
                                        .sourceConfigurationBuilder()
                                        .setAttributesFieldName(sourceAttributes.getName())
                                        .build()))
                        .convertForRowToMessage(
                            Schema.of(targetAttributes, targetTimestamp), CLOCK))
                .setRowSchema(Schema.of(targetAttributes, targetTimestamp)))
        .containsInAnyOrder(
            Row.withSchema(Schema.of(targetAttributes, targetTimestamp))
                .attachValues(attributes, generatedTimestamp));

    PAssert.that(
            "timestamp only source yields attributes + timestamp target",
            pipeline
                .apply(
                    Create.of(
                        Row.withSchema(Schema.of(sourceTimestamp))
                            .attachValues(timestampFromSource)))
                .setRowSchema(Schema.of(sourceTimestamp))
                .apply(
                    transform(
                            configurationBuilder()
                                .setSource(
                                    PubsubWriteSchemaTransformConfiguration
                                        .sourceConfigurationBuilder()
                                        .setTimestampFieldName(sourceTimestamp.getName())
                                        .build()))
                        .convertForRowToMessage(
                            Schema.of(targetAttributes, targetTimestamp), CLOCK))
                .setRowSchema(Schema.of(targetAttributes, targetTimestamp)))
        .containsInAnyOrder(
            Row.withSchema(Schema.of(targetAttributes, targetTimestamp))
                .attachValues(ImmutableMap.of(), timestampFromSource));

    PAssert.that(
            "timestamp and attributes source yields renamed fields in target",
            pipeline
                .apply(
                    Create.of(
                        Row.withSchema(Schema.of(sourceAttributes, sourceTimestamp))
                            .attachValues(attributes, timestampFromSource)))
                .setRowSchema(Schema.of(sourceAttributes, sourceTimestamp))
                .apply(
                    transform(
                            configurationBuilder()
                                .setSource(
                                    PubsubWriteSchemaTransformConfiguration
                                        .sourceConfigurationBuilder()
                                        .setAttributesFieldName(sourceAttributes.getName())
                                        .setTimestampFieldName(sourceTimestamp.getName())
                                        .build()))
                        .convertForRowToMessage(
                            Schema.of(targetAttributes, targetTimestamp), CLOCK))
                .setRowSchema(Schema.of(targetAttributes, targetTimestamp)))
        .containsInAnyOrder(
            Row.withSchema(Schema.of(targetAttributes, targetTimestamp))
                .attachValues(attributes, timestampFromSource));

    PAssert.that(
            "bytes payload only source yields attributes + timestamp + renamed bytes payload target",
            pipeline
                .apply(
                    Create.of(
                        Row.withSchema(Schema.of(sourcePayloadBytes))
                            .withFieldValue(sourcePayloadBytes.getName(), payloadBytes)
                            .build()))
                .setRowSchema(Schema.of(sourcePayloadBytes))
                .apply(
                    transform(
                            configurationBuilder()
                                .setSource(
                                    PubsubWriteSchemaTransformConfiguration
                                        .sourceConfigurationBuilder()
                                        .setPayloadFieldName(sourcePayloadBytes.getName())
                                        .build()))
                        .convertForRowToMessage(
                            Schema.of(targetAttributes, targetTimestamp, targetPayloadBytes),
                            CLOCK))
                .setRowSchema(Schema.of(targetAttributes, targetTimestamp, targetPayloadBytes)))
        .containsInAnyOrder(
            Row.withSchema(Schema.of(targetAttributes, targetTimestamp, targetPayloadBytes))
                .attachValues(ImmutableMap.of(), generatedTimestamp, payloadBytes));

    PAssert.that(
            "row payload only source yields attributes + timestamp + renamed row payload target",
            pipeline
                .apply(Create.of(Row.withSchema(Schema.of(sourcePayloadRow)).attachValues(userRow)))
                .setRowSchema(Schema.of(sourcePayloadRow))
                .apply(
                    transform(
                            configurationBuilder()
                                .setSource(
                                    PubsubWriteSchemaTransformConfiguration
                                        .sourceConfigurationBuilder()
                                        .setPayloadFieldName(sourcePayloadRow.getName())
                                        .build()))
                        .convertForRowToMessage(
                            Schema.of(targetAttributes, targetTimestamp, targetPayloadRow), CLOCK))
                .setRowSchema(Schema.of(targetAttributes, targetTimestamp, targetPayloadRow)))
        .containsInAnyOrder(
            Row.withSchema(Schema.of(targetAttributes, targetTimestamp, targetPayloadRow))
                .attachValues(ImmutableMap.of(), generatedTimestamp, userRow));

    PAssert.that(
            "user only fields source yields attributes + timestamp + user fields target",
            pipeline
                .apply(Create.of(userRow))
                .setRowSchema(ALL_DATA_TYPES_SCHEMA)
                .apply(
                    transform(configurationBuilder())
                        .convertForRowToMessage(
                            Schema.builder()
                                .addField(targetAttributes)
                                .addField(targetTimestamp)
                                .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                                .build(),
                            CLOCK))
                .setRowSchema(
                    Schema.builder()
                        .addField(targetAttributes)
                        .addField(targetTimestamp)
                        .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                        .build()))
        .containsInAnyOrder(
            Row.withSchema(
                    Schema.builder()
                        .addField(targetAttributes)
                        .addField(targetTimestamp)
                        .addFields(ALL_DATA_TYPES_SCHEMA.getFields())
                        .build())
                .addValue(ImmutableMap.of())
                .addValue(generatedTimestamp)
                .addValues(userRow.getValues())
                .build());

    pipeline.run(OPTIONS);
  }

  @Test
  public void testGetPayloadSerializer() {
    Row withAllDataTypes =
        rowWithAllDataTypes(
            false,
            (byte) 0,
            Instant.now().toDateTime(),
            BigDecimal.valueOf(-1L),
            -3.12345,
            -4.1f,
            (short) -5,
            -2,
            -7L,
            "吃葡萄不吐葡萄皮，不吃葡萄倒吐葡萄皮");

    PayloadSerializer jsonPayloadSerializer =
        new JsonPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    byte[] expectedJson = jsonPayloadSerializer.serialize(withAllDataTypes);
    byte[] actualJson =
        transform(configurationBuilder().setFormat("json"))
            .getPayloadSerializer(ALL_DATA_TYPES_SCHEMA)
            .serialize(withAllDataTypes);

    PayloadSerializer avroPayloadSerializer =
        new AvroPayloadSerializerProvider().getSerializer(ALL_DATA_TYPES_SCHEMA, ImmutableMap.of());
    byte[] expectedAvro = avroPayloadSerializer.serialize(withAllDataTypes);
    byte[] actualAvro =
        transform(configurationBuilder().setFormat("avro"))
            .getPayloadSerializer(ALL_DATA_TYPES_SCHEMA)
            .serialize(withAllDataTypes);

    assertArrayEquals(
        "configuration with json format should yield JSON PayloadSerializer",
        expectedJson,
        actualJson);

    assertArrayEquals(
        "configuration with avro format should yield Avro PayloadSerializer",
        expectedAvro,
        actualAvro);
  }

  private static PubsubWriteSchemaTransformConfiguration.Builder configurationBuilder() {
    return PubsubWriteSchemaTransformConfiguration.builder()
        .setTopic(TOPIC)
        .setTarget(PubsubWriteSchemaTransformConfiguration.targetConfigurationBuilder().build());
  }

  private static PubsubRowToMessage.Builder rowToMessageBuilder() {
    return PubsubRowToMessage.builder();
  }

  private static PubsubIO.Write<PubsubMessage> pubsubWrite() {
    return PubsubIO.writeMessages().to(TOPIC);
  }

  private static PubsubWriteSchemaTransformProvider.PubsubWriteSchemaTransform transform(
      PubsubWriteSchemaTransformConfiguration.Builder configurationBuilder) {
    Row configurationRow = TO_ROW.apply(configurationBuilder.build());
    PubsubWriteSchemaTransformProvider provider = new PubsubWriteSchemaTransformProvider();
    return (PubsubWriteSchemaTransform) provider.from(configurationRow);
  }

  private static class MockClock implements Clock, Serializable {
    private final Long millis;

    private MockClock(Instant timestamp) {
      this.millis = timestamp.getMillis();
    }

    @Override
    public long currentTimeMillis() {
      return millis;
    }
  }
}

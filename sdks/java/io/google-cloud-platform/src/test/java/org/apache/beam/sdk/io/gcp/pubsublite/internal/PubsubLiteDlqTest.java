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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider.getRawBytesToRowFunction;
import static org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider.getUuidFromMessage;

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteIO;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteReadSchemaTransformProvider.ErrorFn;
import org.apache.beam.sdk.io.gcp.pubsublite.UuidDeduplicationOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PubsubLiteDlqTest {

  private static final TupleTag<Row> OUTPUT_TAG = PubsubLiteReadSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = PubsubLiteReadSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAM_RAW_SCHEMA =
      Schema.builder().addField("payload", Schema.FieldType.BYTES).build();
  private static final Schema BEAM_SCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));

  private static final Schema BEAM_SCHEMA_ATTRIBUTES =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("key1", Schema.FieldType.STRING),
          Schema.Field.of("key2", Schema.FieldType.STRING));

  private static final Schema BEAM_SCHEMA_ATTRIBUTES_AND_MAP =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("key1", Schema.FieldType.STRING),
          Schema.Field.of("key2", Schema.FieldType.STRING),
          Schema.Field.of(
              "attrs", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING)));

  private static final Schema BEAM_SCHEMA_ATTRIBUTES_MAP =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of(
              "attrs", Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING)));

  private static final Map<String, String> STATIC_MAP;

  static {
    Map<String, String> tempMap = new HashMap<>();
    tempMap.put("key1", "first_key");
    tempMap.put("key2", "second_key");
    STATIC_MAP = Collections.unmodifiableMap(tempMap);
  }

  private static final List<Row> RAW_ROWS;

  static {
    try {
      RAW_ROWS =
          Arrays.asList(
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "a".getBytes("UTF-8"))
                  .build(),
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "b".getBytes("UTF-8"))
                  .build(),
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "c".getBytes("UTF-8"))
                  .build());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static final List<Row> ROWS_WITH_ATTRIBUTES =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES)
              .withFieldValue("name", "a")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES)
              .withFieldValue("name", "b")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES)
              .withFieldValue("name", "c")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .build());
  private static final List<Row> ROWS_WITH_ATTRIBUTES_MAP =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_MAP)
              .withFieldValue("name", "a")
              .withFieldValue("attrs", STATIC_MAP)
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_MAP)
              .withFieldValue("name", "b")
              .withFieldValue("attrs", STATIC_MAP)
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_MAP)
              .withFieldValue("name", "c")
              .withFieldValue("attrs", STATIC_MAP)
              .build());
  private static final List<Row> ROWS_WITH_ATTRIBUTES_AND_MAP =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_AND_MAP)
              .withFieldValue("name", "a")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .withFieldValue("attrs", STATIC_MAP)
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_AND_MAP)
              .withFieldValue("name", "b")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .withFieldValue("attrs", STATIC_MAP)
              .build(),
          Row.withSchema(BEAM_SCHEMA_ATTRIBUTES_AND_MAP)
              .withFieldValue("name", "c")
              .withFieldValue("key1", "first_key")
              .withFieldValue("key2", "second_key")
              .withFieldValue("attrs", STATIC_MAP)
              .build());

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "c").build());

  private static final Map<String, AttributeValues> ATTRIBUTE_VALUES_MAP = new HashMap<>();

  static {
    ATTRIBUTE_VALUES_MAP.put(
        "key1",
        AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("first_key")).build());
    ATTRIBUTE_VALUES_MAP.put(
        "key2",
        AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("second_key")).build());
  }

  private static final List<SequencedMessage> MESSAGES =
      Arrays.asList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"a\"}"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"b\"}"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"name\":\"c\"}"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build());

  private static final List<SequencedMessage> RAW_MESSAGES =
      Arrays.asList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("a"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("b"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("c"))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build());

  private static final List<SequencedMessage> MESSAGESWITHERROR =
      Arrays.asList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"a\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"b\"}"))
                      .build())
              .build(),
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFromUtf8("{\"error\":\"c\"}"))
                      .build())
              .build());

  private static final String PROTO_STRING_SCHEMA =
      "syntax = \"proto3\";\n"
          + "package com.test.proto;"
          + "\n"
          + "message MyMessage {\n"
          + "  int32 id = 1;\n"
          + "  string name = 2;\n"
          + "  bool active = 3;\n"
          + "\n"
          + "  // Nested field\n"
          + "  message Address {\n"
          + "    string street = 1;\n"
          + "    string city = 2;\n"
          + "    string state = 3;\n"
          + "    string zip_code = 4;\n"
          + "  }\n"
          + "\n"
          + "  Address address = 4;\n"
          + "}";

  private static final Schema BEAM_PROTO_SCHEMA =
      Schema.builder()
          .addField("id", Schema.FieldType.INT32)
          .addField("name", Schema.FieldType.STRING)
          .addField("active", Schema.FieldType.BOOLEAN)
          .addField(
              "address",
              Schema.FieldType.row(
                  Schema.builder()
                      .addField("city", Schema.FieldType.STRING)
                      .addField("street", Schema.FieldType.STRING)
                      .addField("state", Schema.FieldType.STRING)
                      .addField("zip_code", Schema.FieldType.STRING)
                      .build()))
          .build();

  private static final Row INPUT_ROW =
      Row.withSchema(BEAM_PROTO_SCHEMA)
          .withFieldValue("id", 1234)
          .withFieldValue("name", "Doe")
          .withFieldValue("active", false)
          .withFieldValue("address.city", "seattle")
          .withFieldValue("address.street", "fake street")
          .withFieldValue("address.zip_code", "TO-1234")
          .withFieldValue("address.state", "wa")
          .build();
  private static final SerializableFunction<Row, byte[]> INPUT_MAPPER =
      ProtoByteUtils.getRowToProtoBytesFromSchema(PROTO_STRING_SCHEMA, "com.test.proto.MyMessage");

  private static final byte[] INPUT_SOURCE = INPUT_MAPPER.apply(INPUT_ROW);

  private static final List<SequencedMessage> INPUT_MESSAGES =
      Collections.singletonList(
          SequencedMessage.newBuilder()
              .setMessage(
                  PubSubMessage.newBuilder()
                      .setData(ByteString.copyFrom(INPUT_SOURCE))
                      .putAllAttributes(ATTRIBUTE_VALUES_MAP)
                      .build())
              .build());

  final SerializableFunction<byte[], Row> valueMapper =
      JsonUtils.getJsonBytesToRowFunction(BEAM_SCHEMA);

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPubsubLiteErrorFnSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(BEAM_SCHEMA);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(ROWS);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnFailure() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGESWITHERROR));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(BEAM_SCHEMA);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PCollection<Long> count = output.get(ERROR_TAG).apply("error_count", Count.globally());

    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));

    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnRawSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();

    List<String> attributes = new ArrayList<>();
    String attributesMap = "";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_RAW_SCHEMA, attributes, attributesMap);
    SerializableFunction<byte[], Row> rawValueMapper = getRawBytesToRowFunction(BEAM_RAW_SCHEMA);
    PCollection<SequencedMessage> input = p.apply(Create.of(RAW_MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", rawValueMapper, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(RAW_ROWS);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnWithAttributesSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    List<String> attributes = new ArrayList<>();
    attributes.add("key1");
    attributes.add("key2");
    String attributeMap = "";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_SCHEMA, attributes, attributeMap);

    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorFn(
                        "Read-Error-Counter",
                        valueMapper,
                        errorSchema,
                        attributes,
                        attributeMap,
                        beamAttributeSchema,
                        Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(ROWS_WITH_ATTRIBUTES);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnWithAttributeMapSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    // empty list of attributes
    List<String> attributes = new ArrayList<>();
    String attributeMap = "attrs";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_SCHEMA, attributes, attributeMap);

    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorFn(
                        "Read-Error-Counter",
                        valueMapper,
                        errorSchema,
                        attributes,
                        attributeMap,
                        beamAttributeSchema,
                        Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(ROWS_WITH_ATTRIBUTES_MAP);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnWithAttributesAndAttributeMapSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    List<String> attributes = new ArrayList<>();
    attributes.add("key1");
    attributes.add("key2");
    String attributeMap = "attrs";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_SCHEMA, attributes, attributeMap);

    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorFn(
                        "Read-Error-Counter",
                        valueMapper,
                        errorSchema,
                        attributes,
                        attributeMap,
                        beamAttributeSchema,
                        Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(ROWS_WITH_ATTRIBUTES_AND_MAP);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnWithAttributesFailure() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    List<String> attributes = new ArrayList<>();
    attributes.add("randomKey1");
    attributes.add("randomKey2");
    String attributeMap = "";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_SCHEMA, attributes, attributeMap);

    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorFn(
                        "Read-Error-Counter",
                        valueMapper,
                        errorSchema,
                        attributes,
                        attributeMap,
                        beamAttributeSchema,
                        Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PCollection<Long> count = output.get(ERROR_TAG).apply("error_count", Count.globally());

    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));

    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnWithDedupingSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();

    PCollection<SequencedMessage> input = p.apply(Create.of(MESSAGES));
    UuidDeduplicationOptions.Builder uuidExtractor =
        UuidDeduplicationOptions.newBuilder().setUuidExtractor(getUuidFromMessage("key1"));
    PCollectionTuple output =
        input
            .apply(PubsubLiteIO.deduplicate(uuidExtractor.build()))
            .apply(
                ParDo.of(new ErrorFn("Read-Error-Counter", valueMapper, errorSchema, Boolean.TRUE))
                    .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(BEAM_SCHEMA);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PCollection<Long> count = output.get(OUTPUT_TAG).apply("error_count", Count.globally());

    // We are deduping so we should only have 1 value
    PAssert.that(count).containsInAnyOrder(Collections.singletonList(1L));

    p.run().waitUntilFinish();
  }

  @Test
  public void testPubSubLiteErrorFnReadProto() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();

    List<String> attributes = new ArrayList<>();
    String attributesMap = "";
    Schema beamAttributeSchema =
        PubsubLiteReadSchemaTransformProvider.buildSchemaWithAttributes(
            BEAM_PROTO_SCHEMA, attributes, attributesMap);

    SerializableFunction<byte[], Row> protoValueMapper =
        ProtoByteUtils.getProtoBytesToRowFromSchemaFunction(
            PROTO_STRING_SCHEMA, "com.test.proto.MyMessage");

    PCollection<SequencedMessage> input = p.apply(Create.of(INPUT_MESSAGES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorFn("Read-Error-Counter", protoValueMapper, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(OUTPUT_TAG).setRowSchema(beamAttributeSchema);
    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(INPUT_ROW);
    p.run().waitUntilFinish();
  }
}

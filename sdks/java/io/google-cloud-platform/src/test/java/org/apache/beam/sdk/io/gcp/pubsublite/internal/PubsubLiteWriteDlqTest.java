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

import com.google.cloud.pubsublite.proto.AttributeValues;
import com.google.cloud.pubsublite.proto.PubSubMessage;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider;
import org.apache.beam.sdk.io.gcp.pubsublite.PubsubLiteWriteSchemaTransformProvider.ErrorCounterFn;
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
public class PubsubLiteWriteDlqTest {

  private static final TupleTag<PubSubMessage> OUTPUT_TAG =
      PubsubLiteWriteSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = PubsubLiteWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAM_SCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));

  private static final Schema BEAM_RAW_SCHEMA =
      Schema.of(Schema.Field.of("payload", Schema.FieldType.BYTES));

  private static final Schema BEAM_SCHEMA_ATTRIBUTES =
      Schema.of(
          Schema.Field.of("name", Schema.FieldType.STRING),
          Schema.Field.of("key1", Schema.FieldType.STRING),
          Schema.Field.of("key2", Schema.FieldType.STRING));

  private static final List<Row> RAW_ROWS;

  static {
    try {
      RAW_ROWS =
          Arrays.asList(
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "a".getBytes("UTF8"))
                  .build(),
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "b".getBytes("UTF8"))
                  .build(),
              Row.withSchema(BEAM_RAW_SCHEMA)
                  .withFieldValue("payload", "c".getBytes("UTF8"))
                  .build());
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAM_SCHEMA).withFieldValue("name", "c").build());

  private static final List<Row> ROWSATTRIBUTES =
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

  private static final String PROTO_STRING_SCHEMA =
      "syntax = \"proto3\";\n"
          + "package com.test.proto;"
          + "\n"
          + "message MyMessage {\n"
          + " string name = 1;\n"
          + "}";

  private static final Map<String, AttributeValues> ATTRIBUTE_VALUES_MAP = new HashMap<>();

  static {
    ATTRIBUTE_VALUES_MAP.put(
        "key1",
        AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("first_key")).build());
    ATTRIBUTE_VALUES_MAP.put(
        "key2",
        AttributeValues.newBuilder().addValues(ByteString.copyFromUtf8("second_key")).build());
  }

  private static final List<PubSubMessage> MESSAGES_RAW =
      Arrays.asList(
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("a")).build(),
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("b")).build(),
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("c")).build());

  private static final List<PubSubMessage> MESSAGES =
      Arrays.asList(
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("{\"name\":\"a\"}")).build(),
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("{\"name\":\"b\"}")).build(),
          PubSubMessage.newBuilder().setData(ByteString.copyFromUtf8("{\"name\":\"c\"}")).build());
  private static final List<PubSubMessage> MESSAGES_WITH_ATTRIBUTES =
      Arrays.asList(
          PubSubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("{\"name\":\"a\"}"))
              .putAllAttributes(ATTRIBUTE_VALUES_MAP)
              .build(),
          PubSubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("{\"name\":\"b\"}"))
              .putAllAttributes(ATTRIBUTE_VALUES_MAP)
              .build(),
          PubSubMessage.newBuilder()
              .setData(ByteString.copyFromUtf8("{\"name\":\"c\"}"))
              .putAllAttributes(ATTRIBUTE_VALUES_MAP)
              .build());

  final SerializableFunction<Row, byte[]> valueMapper =
      JsonUtils.getRowToJsonBytesFunction(BEAM_SCHEMA);

  final SerializableFunction<Row, byte[]> valueMapperRaw =
      PubsubLiteWriteSchemaTransformProvider.getRowToRawBytesFunction("payload");

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPubsubLiteErrorFnSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("ErrorCounter", valueMapper, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(MESSAGES);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnSuccessRawEvents() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    PCollection<Row> input = p.apply(Create.of(RAW_ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("ErrorCounter", valueMapperRaw, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(MESSAGES_RAW);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnSuccessWithAttributes() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    List<String> attributes = new ArrayList<>();
    attributes.add("key1");
    attributes.add("key2");
    Schema schema =
        PubsubLiteWriteSchemaTransformProvider.getSchemaWithoutAttributes(
            BEAM_SCHEMA_ATTRIBUTES, attributes);
    PCollection<Row> input = p.apply(Create.of(ROWSATTRIBUTES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn(
                        "ErrorCounter", valueMapper, errorSchema, Boolean.TRUE, attributes, schema))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(MESSAGES_WITH_ATTRIBUTES);
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnSuccessWithAttributesAndDedupingSuccess() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();
    List<String> attributes = new ArrayList<>();
    attributes.add("key1");
    attributes.add("key2");
    Schema schema =
        PubsubLiteWriteSchemaTransformProvider.getSchemaWithoutAttributes(
            BEAM_SCHEMA_ATTRIBUTES, attributes);
    PCollection<Row> input = p.apply(Create.of(ROWSATTRIBUTES));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn(
                        "ErrorCounter", valueMapper, errorSchema, Boolean.TRUE, attributes, schema))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PCollection<Long> count =
        output
            .get(OUTPUT_TAG)
            .apply(
                ParDo.of(
                    new PubsubLiteWriteSchemaTransformProvider.SetUuidFromPubSubMessage.SetUuidFn(
                        "unique_key")))
            .apply("error_count", Count.globally());
    PAssert.that(count).containsInAnyOrder(Collections.singletonList(3L));
    p.run().waitUntilFinish();
  }

  @Test
  public void testPubsubLiteErrorFnSuccessProto() {
    Schema errorSchema = ErrorHandling.errorSchemaBytes();

    SerializableFunction<Row, byte[]> valueMapperProto =
        ProtoByteUtils.getRowToProtoBytesFromSchema(
            PROTO_STRING_SCHEMA, "com.test.proto.MyMessage");

    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn("ErrorCounter", valueMapperProto, errorSchema, Boolean.TRUE))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG).apply(Count.globally()))
        .containsInAnyOrder(Collections.singletonList(3L));
    p.run().waitUntilFinish();
  }
}

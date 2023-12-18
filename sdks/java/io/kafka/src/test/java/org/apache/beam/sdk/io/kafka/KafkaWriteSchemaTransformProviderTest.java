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
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider.getRowToRawBytesFunction;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransform.ErrorCounterFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
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
public class KafkaWriteSchemaTransformProviderTest {

  private static final TupleTag<KV<byte[], byte[]>> OUTPUT_TAG =
      KafkaWriteSchemaTransformProvider.OUTPUT_TAG;
  private static final TupleTag<Row> ERROR_TAG = KafkaWriteSchemaTransformProvider.ERROR_TAG;

  private static final Schema BEAMSCHEMA =
      Schema.of(Schema.Field.of("name", Schema.FieldType.STRING));

  private static final Schema BEAM_RAW_SCHEMA =
      Schema.of(Schema.Field.of("payload", Schema.FieldType.BYTES));
  private static final Schema ERRORSCHEMA = KafkaWriteSchemaTransformProvider.ERROR_SCHEMA;

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

  private static final List<Row> PROTO_ROWS =
      Collections.singletonList(
          Row.withSchema(BEAM_PROTO_SCHEMA)
              .withFieldValue("id", 1234)
              .withFieldValue("name", "Doe")
              .withFieldValue("active", false)
              .withFieldValue("address.city", "seattle")
              .withFieldValue("address.street", "fake street")
              .withFieldValue("address.zip_code", "TO-1234")
              .withFieldValue("address.state", "wa")
              .build());

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "a").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "b").build(),
          Row.withSchema(BEAMSCHEMA).withFieldValue("name", "c").build());

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

  final SerializableFunction<Row, byte[]> valueMapper =
      JsonUtils.getRowToJsonBytesFunction(BEAMSCHEMA);

  final SerializableFunction<Row, byte[]> valueRawMapper = getRowToRawBytesFunction("payload");

  final SerializableFunction<Row, byte[]> protoValueRawMapper =
      ProtoByteUtils.getRowToProtoBytes(
          Objects.requireNonNull(
                  getClass().getResource("/proto_byte/file_descriptor/proto_byte_utils.pb"))
              .getPath(),
          "MyMessage");

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testKafkaErrorFnSuccess() throws Exception {
    List<KV<byte[], byte[]>> msg =
        Arrays.asList(
            KV.of(new byte[1], "{\"name\":\"a\"}".getBytes("UTF8")),
            KV.of(new byte[1], "{\"name\":\"b\"}".getBytes("UTF8")),
            KV.of(new byte[1], "{\"name\":\"c\"}".getBytes("UTF8")));

    PCollection<Row> input = p.apply(Create.of(ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("Kafka-write-error-counter", valueMapper))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(ERRORSCHEMA);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(msg);
    p.run().waitUntilFinish();
  }

  @Test
  public void testKafkaErrorFnRawSuccess() throws Exception {
    List<KV<byte[], byte[]>> msg =
        Arrays.asList(
            KV.of(new byte[1], "a".getBytes("UTF8")),
            KV.of(new byte[1], "b".getBytes("UTF8")),
            KV.of(new byte[1], "c".getBytes("UTF8")));

    PCollection<Row> input = p.apply(Create.of(RAW_ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("Kafka-write-error-counter", valueRawMapper))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(ERRORSCHEMA);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(msg);
    p.run().waitUntilFinish();
  }

  @Test
  public void testKafkaErrorFnProtoSuccess() {
    PCollection<Row> input = p.apply(Create.of(PROTO_ROWS));
    PCollectionTuple output =
        input.apply(
            ParDo.of(new ErrorCounterFn("Kafka-write-error-counter", protoValueRawMapper))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    PAssert.that(output.get(ERROR_TAG).setRowSchema(ERRORSCHEMA)).empty();
    p.run().waitUntilFinish();
  }
}

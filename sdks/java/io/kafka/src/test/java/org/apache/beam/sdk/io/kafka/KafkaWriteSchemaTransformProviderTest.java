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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoByteUtils;
import org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransform.ErrorCounterFn;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.managed.ManagedTransformConstants;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
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
    Schema errorSchema = ErrorHandling.errorSchema(BEAMSCHEMA);
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn("Kafka-write-error-counter", valueMapper, errorSchema, true))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

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
    Schema errorSchema = ErrorHandling.errorSchema(BEAM_RAW_SCHEMA);
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn(
                        "Kafka-write-error-counter", valueRawMapper, errorSchema, true))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);

    PAssert.that(output.get(OUTPUT_TAG)).containsInAnyOrder(msg);
    p.run().waitUntilFinish();
  }

  @Test
  public void testKafkaErrorFnProtoSuccess() {
    PCollection<Row> input = p.apply(Create.of(PROTO_ROWS));
    Schema errorSchema = ErrorHandling.errorSchema(BEAM_PROTO_SCHEMA);
    PCollectionTuple output =
        input.apply(
            ParDo.of(
                    new ErrorCounterFn(
                        "Kafka-write-error-counter", protoValueRawMapper, errorSchema, true))
                .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

    output.get(ERROR_TAG).setRowSchema(errorSchema);
    p.run().waitUntilFinish();
  }

  private static final String PROTO_SCHEMA =
      "syntax = \"proto3\";\n"
          + "\n"
          + "message MyMessage {\n"
          + "  int32 id = 1;\n"
          + "  string name = 2;\n"
          + "  bool active = 3;\n"
          + "}";

  @Test
  public void testBuildTransformWithManaged() {
    List<String> configs =
        Arrays.asList(
            "topic: topic_1\n" + "bootstrap_servers: some bootstrap\n" + "data_format: RAW",
            "topic: topic_2\n"
                + "bootstrap_servers: some bootstrap\n"
                + "producer_config_updates: {\"foo\": \"bar\"}\n"
                + "data_format: AVRO",
            "topic: topic_3\n"
                + "bootstrap_servers: some bootstrap\n"
                + "data_format: PROTO\n"
                + "schema: '"
                + PROTO_SCHEMA
                + "'\n"
                + "message_name: MyMessage");

    for (String config : configs) {
      // Kafka Write SchemaTransform gets built in ManagedSchemaTransformProvider's expand
      Managed.write(Managed.KAFKA)
          .withConfig(YamlUtils.yamlStringToMap(config))
          .expand(
              Pipeline.create()
                  .apply(Create.empty(Schema.builder().addByteArrayField("bytes").build())));
    }
  }

  @Test
  public void testManagedMappings() {
    KafkaWriteSchemaTransformProvider provider = new KafkaWriteSchemaTransformProvider();
    Map<String, String> mapping = ManagedTransformConstants.MAPPINGS.get(provider.identifier());

    assertNotNull(mapping);

    List<String> configSchemaFieldNames = provider.configurationSchema().getFieldNames();
    for (String paramName : mapping.values()) {
      assertTrue(configSchemaFieldNames.contains(paramName));
    }
  }
}

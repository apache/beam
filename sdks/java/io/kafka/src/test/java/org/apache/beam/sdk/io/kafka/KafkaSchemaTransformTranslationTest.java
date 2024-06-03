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

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.io.kafka.KafkaReadSchemaTransformProvider.KafkaReadSchemaTransform;
import static org.apache.beam.sdk.io.kafka.KafkaSchemaTransformTranslation.KafkaReadSchemaTransformTranslator;
import static org.apache.beam.sdk.io.kafka.KafkaSchemaTransformTranslation.KafkaWriteSchemaTransformTranslator;
import static org.apache.beam.sdk.io.kafka.KafkaWriteSchemaTransformProvider.KafkaWriteSchemaTransform;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class KafkaSchemaTransformTranslationTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static final KafkaWriteSchemaTransformProvider WRITE_PROVIDER =
      new KafkaWriteSchemaTransformProvider();
  static final KafkaReadSchemaTransformProvider READ_PROVIDER =
      new KafkaReadSchemaTransformProvider();

  @Test
  public void testRecreateWriteTransformFromRow() {
    Row transformConfigRow =
        Row.withSchema(WRITE_PROVIDER.configurationSchema())
            .withFieldValue("format", "RAW")
            .withFieldValue("topic", "test_topic")
            .withFieldValue("bootstrapServers", "host:port")
            .withFieldValue("producerConfigUpdates", ImmutableMap.<String, String>builder().build())
            .withFieldValue("errorHandling", null)
            .withFieldValue("fileDescriptorPath", "testPath")
            .withFieldValue("messageName", "test_message")
            .withFieldValue("schema", "test_schema")
            .build();
    KafkaWriteSchemaTransform writeTransform =
        (KafkaWriteSchemaTransform) WRITE_PROVIDER.from(transformConfigRow);

    KafkaWriteSchemaTransformTranslator translator = new KafkaWriteSchemaTransformTranslator();
    Row translatedRow = translator.toConfigRow(writeTransform);

    KafkaWriteSchemaTransform writeTransformFromRow =
        translator.fromConfigRow(translatedRow, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, writeTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testWriteTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    Schema inputSchema = Schema.builder().addByteArrayField("b").build();
    PCollection<Row> input =
        p.apply(
                Create.of(
                    Collections.singletonList(
                        Row.withSchema(inputSchema).addValue(new byte[] {1, 2, 3}).build())))
            .setRowSchema(inputSchema);

    Row transformConfigRow =
        Row.withSchema(WRITE_PROVIDER.configurationSchema())
            .withFieldValue("format", "RAW")
            .withFieldValue("topic", "test_topic")
            .withFieldValue("bootstrapServers", "host:port")
            .withFieldValue("producerConfigUpdates", ImmutableMap.<String, String>builder().build())
            .withFieldValue("errorHandling", null)
            .withFieldValue("fileDescriptorPath", "testPath")
            .withFieldValue("messageName", "test_message")
            .withFieldValue("schema", "test_schema")
            .build();

    KafkaWriteSchemaTransform writeTransform =
        (KafkaWriteSchemaTransform) WRITE_PROVIDER.from(transformConfigRow);
    PCollectionRowTuple.of("input", input).apply(writeTransform);

    // Then translate the pipeline to a proto and extract KafkaWriteSchemaTransform proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> writeTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr -> {
                  RunnerApi.FunctionSpec spec = tr.getSpec();
                  try {
                    return spec.getUrn().equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))
                        && SchemaTransformPayload.parseFrom(spec.getPayload())
                            .getIdentifier()
                            .equals(WRITE_PROVIDER.identifier());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    assertEquals(1, writeTransformProto.size());
    RunnerApi.FunctionSpec spec = writeTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    SchemaTransformPayload payload = SchemaTransformPayload.parseFrom(spec.getPayload());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getConfigurationSchema());
    assertEquals(WRITE_PROVIDER.configurationSchema(), schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getConfigurationRow().newInput());

    assertEquals(transformConfigRow, rowFromSpec);

    // Use the information in the proto to recreate the KafkaWriteSchemaTransform
    KafkaWriteSchemaTransformTranslator translator = new KafkaWriteSchemaTransformTranslator();
    KafkaWriteSchemaTransform writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, writeTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    // setting a subset of fields here.
    Row transformConfigRow =
        Row.withSchema(READ_PROVIDER.configurationSchema())
            .withFieldValue("format", "RAW")
            .withFieldValue("topic", "test_topic")
            .withFieldValue("bootstrapServers", "host:port")
            .withFieldValue("confluentSchemaRegistryUrl", "test_url")
            .withFieldValue("confluentSchemaRegistrySubject", "test_subject")
            .withFieldValue("schema", "test_schema")
            .withFieldValue("fileDescriptorPath", "testPath")
            .withFieldValue("messageName", "test_message")
            .withFieldValue("autoOffsetResetConfig", "earliest")
            .withFieldValue("consumerConfigUpdates", ImmutableMap.<String, String>builder().build())
            .withFieldValue("errorHandling", null)
            .build();

    KafkaReadSchemaTransform readTransform =
        (KafkaReadSchemaTransform) READ_PROVIDER.from(transformConfigRow);

    KafkaReadSchemaTransformTranslator translator = new KafkaReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    KafkaReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    Row transformConfigRow =
        Row.withSchema(READ_PROVIDER.configurationSchema())
            .withFieldValue("format", "RAW")
            .withFieldValue("topic", "test_topic")
            .withFieldValue("bootstrapServers", "host:port")
            .withFieldValue("confluentSchemaRegistryUrl", null)
            .withFieldValue("confluentSchemaRegistrySubject", null)
            .withFieldValue("schema", null)
            .withFieldValue("fileDescriptorPath", "testPath")
            .withFieldValue("messageName", "test_message")
            .withFieldValue("autoOffsetResetConfig", "earliest")
            .withFieldValue("consumerConfigUpdates", ImmutableMap.<String, String>builder().build())
            .withFieldValue("errorHandling", null)
            .build();

    KafkaReadSchemaTransform readTransform =
        (KafkaReadSchemaTransform) READ_PROVIDER.from(transformConfigRow);

    PCollectionRowTuple.empty(p).apply(readTransform);

    // Then translate the pipeline to a proto and extract KafkaReadSchemaTransform proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> readTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr -> {
                  RunnerApi.FunctionSpec spec = tr.getSpec();
                  try {
                    return spec.getUrn().equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))
                        && SchemaTransformPayload.parseFrom(spec.getPayload())
                            .getIdentifier()
                            .equals(READ_PROVIDER.identifier());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    assertEquals(1, readTransformProto.size());
    RunnerApi.FunctionSpec spec = readTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    SchemaTransformPayload payload = SchemaTransformPayload.parseFrom(spec.getPayload());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getConfigurationSchema());
    assertEquals(READ_PROVIDER.configurationSchema(), schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getConfigurationRow().newInput());
    assertEquals(transformConfigRow, rowFromSpec);

    // Use the information in the proto to recreate the KafkaReadSchemaTransform
    KafkaReadSchemaTransformTranslator translator = new KafkaReadSchemaTransformTranslator();
    KafkaReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, readTransformFromSpec.getConfigurationRow());
  }
}

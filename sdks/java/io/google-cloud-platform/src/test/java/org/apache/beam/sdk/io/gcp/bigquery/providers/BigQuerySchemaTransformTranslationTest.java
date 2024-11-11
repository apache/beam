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
package org.apache.beam.sdk.io.gcp.bigquery.providers;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryDirectReadSchemaTransformProvider.BigQueryDirectReadSchemaTransform;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQuerySchemaTransformTranslation.BigQueryStorageReadSchemaTransformTranslator;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQuerySchemaTransformTranslation.BigQueryWriteSchemaTransformTranslator;
import static org.apache.beam.sdk.io.gcp.bigquery.providers.BigQueryWriteSchemaTransformProvider.BigQueryWriteSchemaTransform;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigQuerySchemaTransformTranslationTest {
  static final BigQueryWriteSchemaTransformProvider WRITE_PROVIDER =
      new BigQueryWriteSchemaTransformProvider();
  static final BigQueryDirectReadSchemaTransformProvider READ_PROVIDER =
      new BigQueryDirectReadSchemaTransformProvider();
  static final Row WRITE_CONFIG_ROW =
      Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("table", "project:dataset.table")
          .withFieldValue("create_disposition", "create_never")
          .withFieldValue("write_disposition", "write_append")
          .withFieldValue("triggering_frequency_seconds", 5L)
          .withFieldValue("use_at_least_once_semantics", false)
          .withFieldValue("auto_sharding", false)
          .withFieldValue("num_streams", 5)
          .withFieldValue("error_handling", null)
          .build();
  static final Row READ_CONFIG_ROW =
      Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("query", null)
          .withFieldValue("table_spec", "apache-beam-testing.samples.weather_stations")
          .withFieldValue("row_restriction", "col < 5")
          .withFieldValue("selected_fields", Arrays.asList("col1", "col2", "col3"))
          .build();

  @Test
  public void testRecreateWriteTransformFromRow() {
    BigQueryWriteSchemaTransform writeTransform =
        (BigQueryWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG_ROW);

    BigQueryWriteSchemaTransformTranslator translator =
        new BigQueryWriteSchemaTransformTranslator();
    Row translatedRow = translator.toConfigRow(writeTransform);

    BigQueryWriteSchemaTransform writeTransformFromRow =
        translator.fromConfigRow(translatedRow, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG_ROW, writeTransformFromRow.getConfigurationRow());
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

    BigQueryWriteSchemaTransform writeTransform =
        (BigQueryWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG_ROW);
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

    assertEquals(WRITE_CONFIG_ROW, rowFromSpec);

    // Use the information in the proto to recreate the KafkaWriteSchemaTransform
    BigQueryWriteSchemaTransformTranslator translator =
        new BigQueryWriteSchemaTransformTranslator();
    BigQueryWriteSchemaTransform writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG_ROW, writeTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    BigQueryDirectReadSchemaTransform readTransform =
        (BigQueryDirectReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG_ROW);

    BigQueryStorageReadSchemaTransformTranslator translator =
        new BigQueryStorageReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    BigQueryDirectReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG_ROW, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();

    BigQueryDirectReadSchemaTransform readTransform =
        (BigQueryDirectReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG_ROW);

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
    assertEquals(READ_CONFIG_ROW, rowFromSpec);

    // Use the information in the proto to recreate the KafkaReadSchemaTransform
    BigQueryStorageReadSchemaTransformTranslator translator =
        new BigQueryStorageReadSchemaTransformTranslator();
    BigQueryDirectReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG_ROW, readTransformFromSpec.getConfigurationRow());
  }
}

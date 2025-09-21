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
package org.apache.beam.sdk.io.jdbc.providers;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.io.jdbc.providers.PostgresSchemaTransformTranslation.PostgresReadSchemaTransformTranslator;
import static org.apache.beam.sdk.io.jdbc.providers.PostgresSchemaTransformTranslation.PostgresWriteSchemaTransformTranslator;
import static org.apache.beam.sdk.io.jdbc.providers.ReadFromPostgresSchemaTransformProvider.PostgresReadSchemaTransform;
import static org.apache.beam.sdk.io.jdbc.providers.WriteToPostgresSchemaTransformProvider.PostgresWriteSchemaTransform;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class PostgresSchemaTransformTranslationTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static final WriteToPostgresSchemaTransformProvider WRITE_PROVIDER =
      new WriteToPostgresSchemaTransformProvider();
  static final ReadFromPostgresSchemaTransformProvider READ_PROVIDER =
      new ReadFromPostgresSchemaTransformProvider();

  static final Row READ_CONFIG =
      Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("jdbc_url", "jdbc:postgresql://host:port/database")
          .withFieldValue("location", "test_table")
          .withFieldValue("connection_properties", "some_property")
          .withFieldValue("connection_init_sql", ImmutableList.<String>builder().build())
          .withFieldValue("driver_class_name", null)
          .withFieldValue("driver_jars", null)
          .withFieldValue("disable_auto_commit", true)
          .withFieldValue("fetch_size", 10)
          .withFieldValue("num_partitions", 5)
          .withFieldValue("output_parallelization", true)
          .withFieldValue("partition_column", "col")
          .withFieldValue("read_query", null)
          .withFieldValue("username", "my_user")
          .withFieldValue("password", "my_pass")
          .build();

  static final Row WRITE_CONFIG =
      Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("jdbc_url", "jdbc:postgresql://host:port/database")
          .withFieldValue("location", "test_table")
          .withFieldValue("autosharding", true)
          .withFieldValue("connection_init_sql", ImmutableList.<String>builder().build())
          .withFieldValue("connection_properties", "some_property")
          .withFieldValue("driver_class_name", null)
          .withFieldValue("driver_jars", null)
          .withFieldValue("batch_size", 100L)
          .withFieldValue("username", "my_user")
          .withFieldValue("password", "my_pass")
          .withFieldValue("write_statement", null)
          .build();

  @Test
  public void testRecreateWriteTransformFromRow() {
    PostgresWriteSchemaTransform writeTransform =
        (PostgresWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG);

    PostgresWriteSchemaTransformTranslator translator =
        new PostgresWriteSchemaTransformTranslator();
    Row translatedRow = translator.toConfigRow(writeTransform);

    PostgresWriteSchemaTransform writeTransformFromRow =
        translator.fromConfigRow(translatedRow, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG, writeTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testWriteTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    Schema inputSchema = Schema.builder().addStringField("name").build();
    PCollection<Row> input =
        p.apply(
                Create.of(
                    Collections.singletonList(
                        Row.withSchema(inputSchema).addValue("test").build())))
            .setRowSchema(inputSchema);

    PostgresWriteSchemaTransform writeTransform =
        (PostgresWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG);
    PCollectionRowTuple.of("input", input).apply(writeTransform);

    // Then translate the pipeline to a proto and extract PostgresWriteSchemaTransform proto
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

    assertEquals(WRITE_CONFIG, rowFromSpec);

    // Use the information in the proto to recreate the PostgresWriteSchemaTransform
    PostgresWriteSchemaTransformTranslator translator =
        new PostgresWriteSchemaTransformTranslator();
    PostgresWriteSchemaTransform writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG, writeTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    // setting a subset of fields here.
    PostgresReadSchemaTransform readTransform =
        (PostgresReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG);

    PostgresReadSchemaTransformTranslator translator = new PostgresReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    PostgresReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();

    PostgresReadSchemaTransform readTransform =
        (PostgresReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG);

    // Mock inferBeamSchema since it requires database connection.
    Schema expectedSchema = Schema.builder().addStringField("name").build();
    try (MockedStatic<JdbcIO.ReadRows> mock = Mockito.mockStatic(JdbcIO.ReadRows.class)) {
      mock.when(() -> JdbcIO.ReadRows.inferBeamSchema(Mockito.any(), Mockito.any()))
          .thenReturn(expectedSchema);
      PCollectionRowTuple.empty(p).apply(readTransform);
    }

    // Then translate the pipeline to a proto and extract PostgresReadSchemaTransform proto
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
    assertEquals(READ_CONFIG, rowFromSpec);

    // Use the information in the proto to recreate the PostgresReadSchemaTransform
    PostgresReadSchemaTransformTranslator translator = new PostgresReadSchemaTransformTranslator();
    PostgresReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG, readTransformFromSpec.getConfigurationRow());
  }
}

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
import static org.apache.beam.sdk.io.jdbc.providers.MySqlSchemaTransformTranslation.MySqlReadSchemaTransformTranslator;
import static org.apache.beam.sdk.io.jdbc.providers.MySqlSchemaTransformTranslation.MySqlWriteSchemaTransformTranslator;
import static org.apache.beam.sdk.io.jdbc.providers.ReadFromMySqlSchemaTransformProvider.MySqlReadSchemaTransform;
import static org.apache.beam.sdk.io.jdbc.providers.WriteToMySqlSchemaTransformProvider.MySqlWriteSchemaTransform;
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

public class MysqlSchemaTransformTranslationTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static final WriteToMySqlSchemaTransformProvider WRITE_PROVIDER =
      new WriteToMySqlSchemaTransformProvider();
  static final ReadFromMySqlSchemaTransformProvider READ_PROVIDER =
      new ReadFromMySqlSchemaTransformProvider();

  static final Row READ_CONFIG =
      Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("jdbc_url", "jdbc:mysql://host:port/database")
          .withFieldValue("location", "test_table")
          .withFieldValue("connection_properties", "some_property")
          .withFieldValue("connection_init_sql", ImmutableList.<String>builder().build())
          .withFieldValue("driver_class_name", null)
          .withFieldValue("driver_jars", null)
          .withFieldValue("disable_auto_commit", true)
          .withFieldValue("fetch_size", null)
          .withFieldValue("num_partitions", 5)
          .withFieldValue("output_parallelization", true)
          .withFieldValue("partition_column", "col")
          .withFieldValue("read_query", null)
          .withFieldValue("username", "my_user")
          .withFieldValue("password", "my_pass")
          .build();

  static final Row WRITE_CONFIG =
      Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("jdbc_url", "jdbc:mysql://host:port/database")
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
    MySqlWriteSchemaTransform writeTransform =
        (MySqlWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG);

    MySqlWriteSchemaTransformTranslator translator = new MySqlWriteSchemaTransformTranslator();
    Row translatedRow = translator.toConfigRow(writeTransform);

    MySqlWriteSchemaTransform writeTransformFromRow =
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

    MySqlWriteSchemaTransform writeTransform =
        (MySqlWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG);
    PCollectionRowTuple.of("input", input).apply(writeTransform);

    // Then translate the pipeline to a proto and extract MySqlWriteSchemaTransform proto
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

    // Use the information in the proto to recreate the MySqlWriteSchemaTransform
    MySqlWriteSchemaTransformTranslator translator = new MySqlWriteSchemaTransformTranslator();
    MySqlWriteSchemaTransform writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG, writeTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    // setting a subset of fields here.
    MySqlReadSchemaTransform readTransform =
        (MySqlReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG);

    MySqlReadSchemaTransformTranslator translator = new MySqlReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    MySqlReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();

    MySqlReadSchemaTransform readTransform =
        (MySqlReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG);

    // Mock inferBeamSchema since it requires database connection.
    Schema expectedSchema = Schema.builder().addStringField("name").build();
    try (MockedStatic<JdbcIO.ReadRows> mock = Mockito.mockStatic(JdbcIO.ReadRows.class)) {
      mock.when(() -> JdbcIO.ReadRows.inferBeamSchema(Mockito.any(), Mockito.any()))
          .thenReturn(expectedSchema);
      PCollectionRowTuple.empty(p).apply(readTransform);
    }

    // Then translate the pipeline to a proto and extract MySqlReadSchemaTransform proto
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

    // Use the information in the proto to recreate the MySqlReadSchemaTransform
    MySqlReadSchemaTransformTranslator translator = new MySqlReadSchemaTransformTranslator();
    MySqlReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG, readTransformFromSpec.getConfigurationRow());
  }
}

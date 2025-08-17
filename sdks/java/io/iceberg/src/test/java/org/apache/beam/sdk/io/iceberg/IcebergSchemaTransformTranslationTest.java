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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.sdk.io.iceberg.IcebergReadSchemaTransformProvider.IcebergReadSchemaTransform;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.INPUT_TAG;
import static org.apache.beam.sdk.io.iceberg.IcebergWriteSchemaTransformProvider.IcebergWriteSchemaTransform;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.iceberg.IcebergCdcReadSchemaTransformProvider.IcebergCdcReadSchemaTransform;
import org.apache.beam.sdk.io.iceberg.IcebergSchemaTransformTranslation.IcebergCdcReadSchemaTransformTranslator;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class IcebergSchemaTransformTranslationTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  static final IcebergWriteSchemaTransformProvider WRITE_PROVIDER =
      new IcebergWriteSchemaTransformProvider();
  static final IcebergReadSchemaTransformProvider READ_PROVIDER =
      new IcebergReadSchemaTransformProvider();
  static final IcebergCdcReadSchemaTransformProvider READ_CDC_PROVIDER =
      new IcebergCdcReadSchemaTransformProvider();

  private static final Map<String, String> CATALOG_PROPERTIES =
      ImmutableMap.<String, String>builder()
          .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
          .put("warehouse", "test_location")
          .build();
  private static final Map<String, String> CONFIG_PROPERTIES =
      ImmutableMap.<String, String>builder().put("key", "value").put("key2", "value2").build();
  private static final Row WRITE_CONFIG_ROW =
      Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("table", "test_table_identifier")
          .withFieldValue("catalog_name", "test-name")
          .withFieldValue("catalog_properties", CATALOG_PROPERTIES)
          .withFieldValue("config_properties", CONFIG_PROPERTIES)
          .withFieldValue("keep", Collections.singletonList("str"))
          .build();

  private static final Row READ_CONFIG_ROW =
      Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("table", "test_table_identifier")
          .withFieldValue("catalog_name", "test-name")
          .withFieldValue("catalog_properties", CATALOG_PROPERTIES)
          .withFieldValue("config_properties", CONFIG_PROPERTIES)
          .build();

  private static final Row READ_CDC_CONFIG_ROW =
      Row.withSchema(READ_CDC_PROVIDER.configurationSchema())
          .withFieldValue("table", "test_table_identifier")
          .withFieldValue("catalog_name", "test-name")
          .withFieldValue("catalog_properties", CATALOG_PROPERTIES)
          .withFieldValue("config_properties", CONFIG_PROPERTIES)
          .withFieldValue("from_snapshot", 123L)
          .withFieldValue("to_timestamp", 456L)
          .withFieldValue("poll_interval_seconds", 123)
          .withFieldValue("streaming", true)
          .build();

  @Test
  public void testReCreateWriteTransformFromRow() {
    IcebergWriteSchemaTransform writeTransform =
        (IcebergWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG_ROW);

    IcebergSchemaTransformTranslation.IcebergWriteSchemaTransformTranslator translator =
        new IcebergSchemaTransformTranslation.IcebergWriteSchemaTransformTranslator();
    Row row = translator.toConfigRow(writeTransform);

    IcebergWriteSchemaTransform writeTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG_ROW, writeTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testWriteTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    Schema inputSchema = Schema.builder().addStringField("str").build();
    PCollection<Row> input =
        p.apply(
                Create.of(
                    Collections.singletonList(Row.withSchema(inputSchema).addValue("a").build())))
            .setRowSchema(inputSchema);

    IcebergWriteSchemaTransform writeTransform =
        (IcebergWriteSchemaTransform) WRITE_PROVIDER.from(WRITE_CONFIG_ROW);
    PCollectionRowTuple.of(INPUT_TAG, input).apply(writeTransform);

    // Then translate the pipeline to a proto and extract IcebergWriteSchemaTransform proto
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

    // Use the information in the proto to recreate the IcebergWriteSchemaTransform
    IcebergSchemaTransformTranslation.IcebergWriteSchemaTransformTranslator translator =
        new IcebergSchemaTransformTranslation.IcebergWriteSchemaTransformTranslator();
    IcebergWriteSchemaTransform writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(WRITE_CONFIG_ROW, writeTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    IcebergReadSchemaTransform readTransform =
        (IcebergReadSchemaTransform) READ_PROVIDER.from(READ_CONFIG_ROW);

    IcebergSchemaTransformTranslation.IcebergReadSchemaTransformTranslator translator =
        new IcebergSchemaTransformTranslation.IcebergReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    IcebergReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(READ_CONFIG_ROW, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    warehouse.createTable(TableIdentifier.parse(identifier), TestFixtures.SCHEMA);

    Map<String, String> properties = new HashMap<>(CATALOG_PROPERTIES);
    properties.put("warehouse", warehouse.location);

    Row transformConfigRow =
        Row.fromRow(READ_CONFIG_ROW)
            .withFieldValue("table", identifier)
            .withFieldValue("catalog_properties", properties)
            .build();

    IcebergReadSchemaTransform readTransform =
        (IcebergReadSchemaTransform) READ_PROVIDER.from(transformConfigRow);

    PCollectionRowTuple.empty(p).apply(readTransform);

    // Then translate the pipeline to a proto and extract IcebergReadSchemaTransform proto
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

    // Use the information in the proto to recreate the IcebergReadSchemaTransform
    IcebergSchemaTransformTranslation.IcebergReadSchemaTransformTranslator translator =
        new IcebergSchemaTransformTranslation.IcebergReadSchemaTransformTranslator();
    IcebergReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, readTransformFromSpec.getConfigurationRow());
  }

  @Test
  public void testReCreateCdcReadTransformFromRow() {
    // setting a subset of fields here.
    IcebergCdcReadSchemaTransform readTransform =
        (IcebergCdcReadSchemaTransform) READ_CDC_PROVIDER.from(READ_CDC_CONFIG_ROW);

    IcebergCdcReadSchemaTransformTranslator translator =
        new IcebergCdcReadSchemaTransformTranslator();
    Row row = translator.toConfigRow(readTransform);

    IcebergCdcReadSchemaTransform readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());

    assertEquals(READ_CDC_CONFIG_ROW, readTransformFromRow.getConfigurationRow());
  }

  @Test
  public void testCdcReadTransformProtoTranslation()
      throws InvalidProtocolBufferException, IOException {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    warehouse.createTable(TableIdentifier.parse(identifier), TestFixtures.SCHEMA);

    Map<String, String> properties = new HashMap<>(CATALOG_PROPERTIES);
    properties.put("warehouse", warehouse.location);

    Row transformConfigRow =
        Row.fromRow(READ_CDC_CONFIG_ROW)
            .withFieldValue("table", identifier)
            .withFieldValue("catalog_properties", properties)
            .build();

    IcebergCdcReadSchemaTransform readTransform =
        (IcebergCdcReadSchemaTransform) READ_CDC_PROVIDER.from(transformConfigRow);

    PCollectionRowTuple.empty(p).apply(readTransform);

    // Then translate the pipeline to a proto and extract IcebergReadSchemaTransform proto
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
                            .equals(READ_CDC_PROVIDER.identifier());
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
    assertEquals(READ_CDC_PROVIDER.configurationSchema(), schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getConfigurationRow().newInput());
    assertEquals(transformConfigRow, rowFromSpec);

    // Use the information in the proto to recreate the IcebergReadSchemaTransform
    IcebergCdcReadSchemaTransformTranslator translator =
        new IcebergCdcReadSchemaTransformTranslator();
    IcebergCdcReadSchemaTransform readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(transformConfigRow, readTransformFromSpec.getConfigurationRow());
  }
}

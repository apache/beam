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
package org.apache.beam.io.iceberg;

import static org.apache.beam.io.iceberg.IcebergIOTranslation.IcebergIOReadTranslator.READ_SCHEMA;
import static org.apache.beam.io.iceberg.IcebergIOTranslation.IcebergIOWriteTranslator.WRITE_SCHEMA;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaAwareTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.TransformUpgrader;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class IcebergIOTranslationTest {
  // A mapping from WriteRows transform builder methods to the corresponding schema fields in
  // IcebergIOTranslation.
  static final Map<String, String> WRITE_TRANSFORM_SCHEMA_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("getCatalogConfig", "catalog_config")
          .put("getTableIdentifier", "table_identifier")
          .put("getDynamicDestinations", "dynamic_destinations")
          .build();

  // A mapping from ReadRows transform builder methods to the corresponding schema fields in
  // IcebergIOTranslation.
  static final Map<String, String> READ_TRANSFORM_SCHEMA_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("getCatalogConfig", "catalog_config")
          .put("getTableIdentifier", "table_identifier")
          .build();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public transient TestDataWarehouse warehouse = new TestDataWarehouse(TEMPORARY_FOLDER, "default");

  @Test
  public void testReCreateWriteTransformFromRow() {
    // setting a subset of fields here.
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    IcebergIO.WriteRows writeTransform =
        IcebergIO.writeRows(config).to(TableIdentifier.of("test_namespace", "test_table"));

    IcebergIOTranslation.IcebergIOWriteTranslator translator =
        new IcebergIOTranslation.IcebergIOWriteTranslator();
    Row row = translator.toConfigRow(writeTransform);

    IcebergIO.WriteRows writeTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(writeTransformFromRow.getTableIdentifier());
    assertEquals(
        "test_namespace", writeTransformFromRow.getTableIdentifier().namespace().levels()[0]);
    assertEquals("test_table", writeTransformFromRow.getTableIdentifier().name());
    assertEquals("test_catalog", writeTransformFromRow.getCatalogConfig().getName());
    assertEquals(
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
        writeTransformFromRow.getCatalogConfig().getIcebergCatalogType());
    assertEquals(
        warehouse.location, writeTransformFromRow.getCatalogConfig().getWarehouseLocation());
  }

  @Test
  public void testWriteTransformProtoTranslation() throws Exception {
    // First build a pipeline
    Pipeline p = Pipeline.create();
    Schema inputSchema = Schema.builder().addStringField("str").build();
    PCollection<Row> input =
        p.apply(
                Create.of(
                    Arrays.asList(
                        Row.withSchema(inputSchema).addValue("a").build(),
                        Row.withSchema(inputSchema).addValue("b").build(),
                        Row.withSchema(inputSchema).addValue("c").build())))
            .setRowSchema(inputSchema);

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);

    IcebergIO.WriteRows writeTransform =
        IcebergIO.writeRows(catalogConfig).to(TableIdentifier.parse(identifier));

    input.apply(writeTransform);

    // Then translate the pipeline to a proto and extract IcebergIO.WriteRows proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> writeTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr ->
                    tr.getSpec()
                        .getUrn()
                        .equals(
                            IcebergIOTranslation.IcebergIOWriteTranslator
                                .ICEBERG_WRITE_TRANSFORM_URN))
            .collect(Collectors.toList());
    assertEquals(1, writeTransformProto.size());
    RunnerApi.FunctionSpec spec = writeTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    SchemaAwareTransforms.SchemaAwareTransformPayload payload =
        SchemaAwareTransforms.SchemaAwareTransformPayload.parseFrom(spec.getPayload());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getExpansionSchema());
    assertEquals(WRITE_SCHEMA, schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getExpansionPayload().newInput());
    Row expectedRow =
        Row.withSchema(WRITE_SCHEMA)
            .withFieldValue("table_identifier", identifier)
            .withFieldValue("catalog_config", TransformUpgrader.toByteArray(catalogConfig))
            .withFieldValue("dynamic_destinations", null)
            .build();
    assertEquals(expectedRow, rowFromSpec);

    // Use the information in the proto to recreate the IcebergIO.WriteRows transform
    IcebergIOTranslation.IcebergIOWriteTranslator translator =
        new IcebergIOTranslation.IcebergIOWriteTranslator();
    IcebergIO.WriteRows writeTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(TableIdentifier.parse(identifier), writeTransformFromSpec.getTableIdentifier());
    assertEquals(catalogConfig, writeTransformFromSpec.getCatalogConfig());
    assertNull(writeTransformFromSpec.getDynamicDestinations());
  }

  @Test
  public void testWriteTransformRowIncludesAllFields() {
    List<String> getMethodNames =
        Arrays.stream(IcebergIO.WriteRows.class.getDeclaredMethods())
            .map(method -> method.getName())
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'IcebergIO.WriteRows' transform. Please update"
              + "'IcebergIOTranslation.IcebergIOWriteTranslator' to track the new method "
              + "and update this test.",
          WRITE_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    WRITE_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "IcebergIOTranslation.IcebergIOWriteTranslator.",
                  WRITE_SCHEMA.getFieldNames().contains(fieldName));
            });
  }

  @Test
  public void testReCreateReadTransformFromRow() {
    // setting a subset of fields here.
    IcebergCatalogConfig config =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    IcebergIO.ReadRows readTransform =
        IcebergIO.readRows(config).from(TableIdentifier.of("test_namespace", "test_table"));

    IcebergIOTranslation.IcebergIOReadTranslator translator =
        new IcebergIOTranslation.IcebergIOReadTranslator();
    Row row = translator.toConfigRow(readTransform);

    IcebergIO.ReadRows readTransformFromRow =
        translator.fromConfigRow(row, PipelineOptionsFactory.create());
    assertNotNull(readTransformFromRow.getTableIdentifier());
    assertEquals(
        "test_namespace", readTransformFromRow.getTableIdentifier().namespace().levels()[0]);
    assertEquals("test_table", readTransformFromRow.getTableIdentifier().name());
    assertEquals("test_catalog", readTransformFromRow.getCatalogConfig().getName());
    assertEquals(
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
        readTransformFromRow.getCatalogConfig().getIcebergCatalogType());
    assertEquals(
        warehouse.location, readTransformFromRow.getCatalogConfig().getWarehouseLocation());
  }

  @Test
  public void testReadTransformProtoTranslation() throws Exception {
    // First build a pipeline
    Pipeline p = Pipeline.create();

    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .setName("test_catalog")
            .setIcebergCatalogType(CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
            .setWarehouseLocation(warehouse.location)
            .build();
    String identifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
    TableIdentifier tableId = TableIdentifier.parse(identifier);

    warehouse.createTable(tableId, TestFixtures.SCHEMA);

    IcebergIO.ReadRows readTransform = IcebergIO.readRows(catalogConfig).from(tableId);

    p.apply(readTransform);

    // Then translate the pipeline to a proto and extract IcebergIO.ReadRows proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> readTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr ->
                    tr.getSpec()
                        .getUrn()
                        .equals(
                            IcebergIOTranslation.IcebergIOReadTranslator
                                .ICEBERG_READ_TRANSFORM_URN))
            .collect(Collectors.toList());
    assertEquals(1, readTransformProto.size());
    RunnerApi.FunctionSpec spec = readTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    SchemaAwareTransforms.SchemaAwareTransformPayload payload =
        SchemaAwareTransforms.SchemaAwareTransformPayload.parseFrom(spec.getPayload());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getExpansionSchema());
    assertEquals(READ_SCHEMA, schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getExpansionPayload().newInput());
    Row expectedRow =
        Row.withSchema(READ_SCHEMA)
            .withFieldValue("table_identifier", identifier)
            .withFieldValue("catalog_config", TransformUpgrader.toByteArray(catalogConfig))
            .build();
    assertEquals(expectedRow, rowFromSpec);

    // Use the information in the proto to recreate the IcebergIO.ReadRows transform
    IcebergIOTranslation.IcebergIOReadTranslator translator =
        new IcebergIOTranslation.IcebergIOReadTranslator();
    IcebergIO.ReadRows readTransformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(tableId, readTransformFromSpec.getTableIdentifier());
    assertEquals(catalogConfig, readTransformFromSpec.getCatalogConfig());
  }

  @Test
  public void testReadTransformRowIncludesAllFields() {
    List<String> getMethodNames =
        Arrays.stream(IcebergIO.ReadRows.class.getDeclaredMethods())
            .map(method -> method.getName())
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'IcebergIO.ReadRows' transform. Please update"
              + "'IcebergIOTranslation.IcebergIOReadTranslator' to track the new method "
              + "and update this test.",
          READ_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    READ_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "IcebergIOTranslation.IcebergIOReadTranslator.",
                  IcebergIOTranslation.IcebergIOReadTranslator.READ_SCHEMA
                      .getFieldNames()
                      .contains(fieldName));
            });
  }
}

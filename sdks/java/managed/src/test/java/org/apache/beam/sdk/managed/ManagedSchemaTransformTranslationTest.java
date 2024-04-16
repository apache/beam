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
package org.apache.beam.sdk.managed;

import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedConfig;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedSchemaTransform;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformTranslation.ManagedSchemaTransformTranslator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaAwareTransforms;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.construction.PTransformTranslation;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class ManagedSchemaTransformTranslationTest {
  // A mapping from ManagedConfig builder methods to the corresponding schema fields in
  // ManagedSchemaTransformTranslation.
  static final Map<String, String> MANAGED_TRANSFORM_SCHEMA_MAPPING =
      ImmutableMap.<String, String>builder()
          .put("getTransformIdentifier", "transformIdentifier")
          .put("getConfigUrl", "configUrl")
          .put("getConfig", "config")
          .build();

  @Test
  public void testReCreateTransformFromRowWithConfigUrl() throws URISyntaxException {
    String yamlConfigPath =
        Paths.get(getClass().getClassLoader().getResource("test_config.yaml").toURI())
            .toFile()
            .getAbsolutePath();

    ManagedConfig originalConfig =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfigUrl(yamlConfigPath)
            .build();

    ManagedSchemaTransformProvider provider = new ManagedSchemaTransformProvider(null);
    ManagedSchemaTransform originalTransform =
        (ManagedSchemaTransform) provider.from(originalConfig);

    ManagedSchemaTransformTranslator translator = new ManagedSchemaTransformTranslator();
    Row configRow = translator.toConfigRow(originalTransform);

    ManagedSchemaTransform transformFromRow =
        translator.fromConfigRow(configRow, PipelineOptionsFactory.create());
    ManagedConfig configFromRow = transformFromRow.getManagedConfig();

    assertNotNull(transformFromRow.getManagedConfig());
    assertEquals(originalConfig.getTransformIdentifier(), configFromRow.getTransformIdentifier());
    assertEquals(originalConfig.getConfigUrl(), configFromRow.getConfigUrl());
    assertNull(configFromRow.getConfig());
  }

  @Test
  public void testReCreateTransformFromRowWithConfig() {
    String yamlString = "extra_string: abc\n" + "extra_integer: 123";

    ManagedConfig originalConfig =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(yamlString)
            .build();

    ManagedSchemaTransformProvider provider = new ManagedSchemaTransformProvider(null);
    ManagedSchemaTransform originalTransform =
        (ManagedSchemaTransform) provider.from(originalConfig);

    ManagedSchemaTransformTranslator translator = new ManagedSchemaTransformTranslator();
    Row configRow = translator.toConfigRow(originalTransform);

    ManagedSchemaTransform transformFromRow =
        translator.fromConfigRow(configRow, PipelineOptionsFactory.create());
    ManagedConfig configFromRow = transformFromRow.getManagedConfig();

    assertNotNull(transformFromRow.getManagedConfig());
    assertEquals(originalConfig.getTransformIdentifier(), configFromRow.getTransformIdentifier());
    assertEquals(configFromRow.getConfig(), yamlString);
    assertNull(originalConfig.getConfigUrl());
  }

  @Test
  public void testManagedTransformRowIncludesAllFields() {
    List<String> getMethodNames =
        Arrays.stream(ManagedConfig.class.getDeclaredMethods())
            .map(method -> method.getName())
            .filter(methodName -> methodName.startsWith("get"))
            .collect(Collectors.toList());

    // Just to make sure that this does not pass trivially.
    assertTrue(getMethodNames.size() > 0);

    for (String getMethodName : getMethodNames) {
      assertTrue(
          "Method "
              + getMethodName
              + " will not be tracked when upgrading the 'ManagedSchemaTransform'. Please update"
              + "'ManagedSchemaTransformTranslation.ManagedSchemaTransformTranslator' to track the "
              + "new method and update this test.",
          MANAGED_TRANSFORM_SCHEMA_MAPPING.keySet().contains(getMethodName));
    }

    // Confirming that all fields mentioned in `WRITE_TRANSFORM_SCHEMA_MAPPING` are
    // actually available in the schema.
    MANAGED_TRANSFORM_SCHEMA_MAPPING.values().stream()
        .forEach(
            fieldName -> {
              assertTrue(
                  "Field name "
                      + fieldName
                      + " was not found in the transform schema defined in "
                      + "ManagedSchemaTransformTranslation.ManagedSchemaTransformTranslator.",
                  ManagedSchemaTransformTranslator.SCHEMA.getFieldNames().contains(fieldName));
            });
  }

  @Test
  public void testProtoTranslation() throws Exception {
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
    Map<String, Object> underlyingConfig =
        ImmutableMap.<String, Object>builder()
            .put("extra_string", "abc")
            .put("extra_integer", 123)
            .build();
    String yamlStringConfig = YamlUtils.yamlStringFromMap(underlyingConfig);
    Managed.ManagedTransform transform =
        Managed.read(Managed.ICEBERG) // give a supported source to get around the check
            .withSupportedIdentifiers(Arrays.asList(TestSchemaTransformProvider.IDENTIFIER))
            .toBuilder()
            .setIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .build()
            .withConfig(underlyingConfig);
    PCollectionRowTuple.of("input", input).apply(transform).get("output");

    // Then translate the pipeline to a proto and extract the ManagedSchemaTransform's proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> managedTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(tr -> tr.getSpec().getUrn().equals(PTransformTranslation.MANAGED_TRANSFORM_URN))
            .collect(Collectors.toList());
    assertEquals(1, managedTransformProto.size());
    RunnerApi.FunctionSpec spec = managedTransformProto.get(0).getSpec();

    // Check that the proto contains correct values
    SchemaAwareTransforms.ManagedTransformPayload payload =
        SchemaAwareTransforms.ManagedTransformPayload.parseFrom(spec.getPayload());
    assertEquals(TestSchemaTransformProvider.IDENTIFIER, payload.getUnderlyingTransformUrn());
    assertEquals(yamlStringConfig, payload.getYamlConfig());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getExpansionSchema());
    assertEquals(ManagedSchemaTransformTranslator.SCHEMA, schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getExpansionPayload().newInput());
    Row expectedRow =
        Row.withSchema(ManagedSchemaTransformTranslator.SCHEMA)
            .withFieldValue("transformIdentifier", TestSchemaTransformProvider.IDENTIFIER)
            .withFieldValue("configUrl", null)
            .withFieldValue("config", yamlStringConfig)
            .build();
    assertEquals(expectedRow, rowFromSpec);

    // Use the information in the proto to recreate the ManagedSchemaTransform
    ManagedSchemaTransformTranslator translator = new ManagedSchemaTransformTranslator();
    ManagedSchemaTransform transformFromSpec =
        translator.fromConfigRow(rowFromSpec, PipelineOptionsFactory.create());

    assertEquals(
        TestSchemaTransformProvider.IDENTIFIER,
        transformFromSpec.getManagedConfig().getTransformIdentifier());
    assertEquals(yamlStringConfig, transformFromSpec.getManagedConfig().getConfig());
    assertNull(transformFromSpec.getManagedConfig().getConfigUrl());
  }
}

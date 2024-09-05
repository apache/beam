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

import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.CONFIG_ROW_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.CONFIG_ROW_SCHEMA_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.MANAGED_UNDERLYING_TRANSFORM_URN_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.Annotations.Enum.SCHEMATRANSFORM_URN_KEY;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.ExpansionMethods.Enum.SCHEMA_TRANSFORM;
import static org.apache.beam.model.pipeline.v1.ExternalTransforms.SchemaTransformPayload;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedConfig;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformProvider.ManagedSchemaTransform;
import static org.apache.beam.sdk.managed.ManagedSchemaTransformTranslation.ManagedSchemaTransformTranslator;
import static org.apache.beam.sdk.util.construction.PTransformTranslation.MANAGED_TRANSFORM_URN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.managed.testing.TestSchemaTransformProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.construction.BeamUrns;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

public class ManagedSchemaTransformTranslationTest {
  static final ManagedSchemaTransformProvider PROVIDER = new ManagedSchemaTransformProvider(null);

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

    ManagedSchemaTransform originalTransform =
        (ManagedSchemaTransform) PROVIDER.from(originalConfig);

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

    ManagedSchemaTransform originalTransform =
        (ManagedSchemaTransform) PROVIDER.from(originalConfig);

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
    input.apply(transform);

    // Then translate the pipeline to a proto and extract the ManagedSchemaTransform's proto
    RunnerApi.Pipeline pipelineProto = PipelineTranslation.toProto(p);
    List<RunnerApi.PTransform> managedTransformProto =
        pipelineProto.getComponents().getTransformsMap().values().stream()
            .filter(
                tr -> {
                  RunnerApi.FunctionSpec spec = tr.getSpec();
                  try {
                    return spec.getUrn().equals(BeamUrns.getUrn(SCHEMA_TRANSFORM))
                        && SchemaTransformPayload.parseFrom(spec.getPayload())
                            .getIdentifier()
                            .equals(PROVIDER.identifier());
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());
    assertEquals(1, managedTransformProto.size());
    RunnerApi.PTransform convertedTransform = managedTransformProto.get(0);

    // Check the transform proto contains the correct annotations.
    // These annotations can be accessed and used by the runner to make decisions
    Row managedConfigRow =
        Row.withSchema(PROVIDER.configurationSchema())
            .withFieldValue("transform_identifier", TestSchemaTransformProvider.IDENTIFIER)
            .withFieldValue("config", yamlStringConfig)
            .build();
    assertEquals(
        ImmutableSet.of(
            BeamUrns.getConstant(SCHEMATRANSFORM_URN_KEY),
            BeamUrns.getConstant(MANAGED_UNDERLYING_TRANSFORM_URN_KEY),
            BeamUrns.getConstant(CONFIG_ROW_KEY),
            BeamUrns.getConstant(CONFIG_ROW_SCHEMA_KEY)),
        convertedTransform.getAnnotationsMap().keySet());
    assertEquals(
        ByteString.copyFromUtf8(MANAGED_TRANSFORM_URN),
        convertedTransform.getAnnotationsMap().get(BeamUrns.getConstant(SCHEMATRANSFORM_URN_KEY)));
    assertEquals(
        ByteString.copyFromUtf8(TestSchemaTransformProvider.IDENTIFIER),
        convertedTransform
            .getAnnotationsMap()
            .get(BeamUrns.getConstant(MANAGED_UNDERLYING_TRANSFORM_URN_KEY)));
    Schema annotationSchema =
        SchemaTranslation.schemaFromProto(
            SchemaApi.Schema.parseFrom(
                convertedTransform
                    .getAnnotationsMap()
                    .get(BeamUrns.getConstant(CONFIG_ROW_SCHEMA_KEY))));
    assertEquals(PROVIDER.configurationSchema(), annotationSchema);
    assertEquals(
        managedConfigRow,
        CoderUtils.decodeFromByteString(
            RowCoder.of(annotationSchema),
            convertedTransform.getAnnotationsMap().get(BeamUrns.getConstant(CONFIG_ROW_KEY))));

    // Check that the spec proto contains correct values
    RunnerApi.FunctionSpec spec = convertedTransform.getSpec();
    SchemaTransformPayload payload = SchemaTransformPayload.parseFrom(spec.getPayload());
    assertEquals(PROVIDER.identifier(), payload.getIdentifier());
    Schema schemaFromSpec = SchemaTranslation.schemaFromProto(payload.getConfigurationSchema());
    assertEquals(PROVIDER.configurationSchema(), schemaFromSpec);
    Row rowFromSpec = RowCoder.of(schemaFromSpec).decode(payload.getConfigurationRow().newInput());
    // Translation logic outputs a Row with snake_case naming convention
    Row expectedRow =
        Row.withSchema(PROVIDER.configurationSchema())
            .withFieldValue("transform_identifier", TestSchemaTransformProvider.IDENTIFIER)
            .withFieldValue("config_url", null)
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

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.testing.TestSchemaTransformProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.YamlUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManagedSchemaTransformProviderTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  private static final Schema EMPTY_SCHEMA = Schema.builder().build();
  private static final Row EMPTY_ROW = Row.nullRow(EMPTY_SCHEMA);

  @Test
  public void testFailWhenNoConfigSpecified() {
    ManagedSchemaTransformProvider.ManagedConfig config =
        ManagedSchemaTransformProvider.ManagedConfig.builder()
            .setTransformIdentifier("some identifier")
            .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Please specify a config or a config URL, but not both");
    config.validate();
  }

  @Test
  public void testFailWhenUnknownFieldsSpecified() {
    Map<String, Object> config =
        ImmutableMap.of(
            "extra_string",
            "str",
            "extra_integer",
            123,
            "toggle_uppercase",
            true,
            "unknown_field",
            "unknown");
    ManagedSchemaTransformProvider.ManagedConfig managedConfig =
        ManagedSchemaTransformProvider.ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(YamlUtils.yamlStringFromMap(config))
            .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid config for transform");
    thrown.expectMessage(TestSchemaTransformProvider.IDENTIFIER);
    thrown.expectMessage("Contains unknown fields");
    thrown.expectMessage("unknown_field");
    Pipeline p = Pipeline.create();
    new ManagedSchemaTransformProvider(null)
        .from(managedConfig)
        .expand(
            PCollectionRowTuple.of(
                "input", p.apply(Create.of(EMPTY_ROW).withRowSchema(EMPTY_SCHEMA))));
  }

  @Test
  public void testFailWhenMissingRequiredFields() {
    Map<String, Object> config = ImmutableMap.of("extra_string", "str", "toggle_uppercase", true);
    ManagedSchemaTransformProvider.ManagedConfig managedConfig =
        ManagedSchemaTransformProvider.ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(YamlUtils.yamlStringFromMap(config))
            .build();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid config for transform");
    thrown.expectMessage(TestSchemaTransformProvider.IDENTIFIER);
    thrown.expectMessage("Missing required fields");
    thrown.expectMessage("extra_integer");
    Pipeline p = Pipeline.create();
    new ManagedSchemaTransformProvider(null)
        .from(managedConfig)
        .expand(
            PCollectionRowTuple.of(
                "input", p.apply(Create.of(EMPTY_ROW).withRowSchema(EMPTY_SCHEMA))));
  }

  @Test
  public void testPassWhenMissingNullableFields() {
    Map<String, Object> config = ImmutableMap.of("extra_string", "str", "extra_integer", 123);
    ManagedSchemaTransformProvider.ManagedConfig managedConfig =
        ManagedSchemaTransformProvider.ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(YamlUtils.yamlStringFromMap(config))
            .build();

    Pipeline p = Pipeline.create();
    new ManagedSchemaTransformProvider(null)
        .from(managedConfig)
        .expand(
            PCollectionRowTuple.of(
                "input", p.apply(Create.of(EMPTY_ROW).withRowSchema(EMPTY_SCHEMA))));
  }

  @Test
  public void testSkipConfigValidationWithUnknownFields() {
    Map<String, Object> config =
        ImmutableMap.of(
            "extra_string",
            "str",
            "extra_integer",
            123,
            "toggle_uppercase",
            true,
            "unknown_field",
            "unknown");
    ManagedSchemaTransformProvider.ManagedConfig managedConfig =
        ManagedSchemaTransformProvider.ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(YamlUtils.yamlStringFromMap(config))
            .setSkipConfigValidation(true)
            .build();

    Pipeline p = Pipeline.create();
    new ManagedSchemaTransformProvider(null)
        .from(managedConfig)
        .expand(
            PCollectionRowTuple.of(
                "input", p.apply(Create.of(EMPTY_ROW).withRowSchema(EMPTY_SCHEMA))));
  }

  @Test
  public void testGetConfigRowFromYamlString() {
    String yamlString = "extra_string: abc\n" + "extra_integer: 123";
    ManagedConfig config =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(yamlString)
            .build();

    Row expectedRow =
        Row.withSchema(TestSchemaTransformProvider.SCHEMA)
            .withFieldValue("extra_string", "abc")
            .withFieldValue("extra_integer", 123)
            .build();

    Row returnedRow =
        ManagedSchemaTransformProvider.getRowConfig(
            config, TestSchemaTransformProvider.SCHEMA, PipelineOptionsFactory.create());

    assertEquals(expectedRow, returnedRow);
  }

  @Test
  public void testGetConfigRowFromYamlFile() throws URISyntaxException {
    String yamlConfigPath =
        Paths.get(getClass().getClassLoader().getResource("test_config.yaml").toURI())
            .toFile()
            .getAbsolutePath();
    ManagedConfig config =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfigUrl(yamlConfigPath)
            .build();
    Schema configSchema = new TestSchemaTransformProvider().configurationSchema();
    Row expectedRow =
        Row.withSchema(configSchema)
            .withFieldValue("extra_string", "abc")
            .withFieldValue("extra_integer", 123)
            .build();
    Row configRow =
        ManagedSchemaTransformProvider.getRowConfig(
            config, TestSchemaTransformProvider.SCHEMA, PipelineOptionsFactory.create());

    assertEquals(expectedRow, configRow);
  }

  @Test
  public void testBuildWithYamlString() {
    String yamlString = "extra_string: abc\n" + "extra_integer: 123";

    ManagedConfig config =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfig(yamlString)
            .build();

    new ManagedSchemaTransformProvider(null).from(config);
  }

  @Test
  public void testBuildWithYamlFile() throws URISyntaxException {
    String yamlConfigPath =
        Paths.get(getClass().getClassLoader().getResource("test_config.yaml").toURI())
            .toFile()
            .getAbsolutePath();

    ManagedConfig config =
        ManagedConfig.builder()
            .setTransformIdentifier(TestSchemaTransformProvider.IDENTIFIER)
            .setConfigUrl(yamlConfigPath)
            .build();

    new ManagedSchemaTransformProvider(null).from(config);
  }

  @Test
  public void testDiscoverTestProvider() {
    ManagedSchemaTransformProvider provider =
        new ManagedSchemaTransformProvider(Arrays.asList(TestSchemaTransformProvider.IDENTIFIER));

    assertTrue(provider.getAllProviders().containsKey(TestSchemaTransformProvider.IDENTIFIER));
  }
}

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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.managed.testing.TestSchemaTransformProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ManagedSchemaTransformProviderTest {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

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
            config.getTransformIdentifier(),
            config.resolveUnderlyingConfig(PipelineOptionsFactory.create()),
            TestSchemaTransformProvider.SCHEMA);

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
            config.getTransformIdentifier(),
            config.resolveUnderlyingConfig(PipelineOptionsFactory.create()),
            new TestSchemaTransformProvider().configurationSchema());

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

  @Test
  public void testResolveBigQueryWrite() {
    String yamlString = "table: test-table";
    ManagedConfig config =
        ManagedConfig.builder()
            .setTransformIdentifier(ManagedTransformConstants.BIGQUERY_STORAGE_WRITE)
            .setConfig(yamlString)
            .build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    Pipeline p = Pipeline.create();

    // streaming case, pick Storage Write API
    PCollection<Row> unboundedInput =
        p.apply(Create.of(Row.nullRow(Schema.builder().build())))
            .setIsBoundedInternal(PCollection.IsBounded.UNBOUNDED);
    String identifier =
        config.resolveUnderlyingTransform(PCollectionRowTuple.of("input", unboundedInput));
    assertEquals(ManagedTransformConstants.BIGQUERY_STORAGE_WRITE, identifier);

    // batch case, pick File Loads
    PCollection<Row> boundedInput =
        p.apply(Create.of(Row.nullRow(Schema.builder().build())))
            .setIsBoundedInternal(PCollection.IsBounded.BOUNDED);
    identifier = config.resolveUnderlyingTransform(PCollectionRowTuple.of("input", boundedInput));
    assertEquals(ManagedTransformConstants.BIGQUERY_FILE_LOADS, identifier);

    // "streaming_mode_at_least_once" dataflow service option is not set: config is unaffected
    Map<String, Object> modifiedConfig = config.resolveUnderlyingConfig(options);
    assertFalse(modifiedConfig.containsKey("at_least_once"));

    // "streaming_mode_at_least_once" dataflow service option is not set: inject
    // "at_least_once=true" to user STORAGE_API_AT_LEAST_ONCE
    options.setDataflowServiceOptions(Collections.singletonList("streaming_mode_at_least_once"));
    modifiedConfig = config.resolveUnderlyingConfig(options);
    assertEquals(true, modifiedConfig.get("at_least_once"));
  }
}

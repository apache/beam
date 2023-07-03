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
package org.apache.beam.sdk.schemas.transforms;

import static org.junit.Assert.assertEquals;

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for {@link Select}. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class TypedSchemaTransformProviderTest {

  /** flat schema to select from. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class Configuration {
    abstract String getField1();

    abstract Integer getField2();

    static Configuration create(String field1, int field2) {
      return new AutoValue_TypedSchemaTransformProviderTest_Configuration(field1, field2);
    }
  };

  private static class FakeTypedSchemaIOProvider
      extends TypedSchemaTransformProvider<Configuration> {
    private FakeTypedSchemaIOProvider() {}

    @Override
    public String identifier() {
      return "fake:v1";
    }

    @Override
    protected Class<Configuration> configurationClass() {
      return Configuration.class;
    }

    @Override
    public SchemaTransform from(Configuration config) {
      return new FakeSchemaTransform(config);
    }

    @Override
    public List<String> inputCollectionNames() {
      return null;
    }

    @Override
    public List<String> outputCollectionNames() {
      return null;
    }

    @Override
    public Optional<List<String>> dependencies(
        Configuration configuration, PipelineOptions options) {
      return Optional.of(
          Arrays.asList(configuration.getField1(), String.valueOf(configuration.getField2())));
    }
  }

  public static class FakeSchemaTransform extends SchemaTransform {

    public Configuration config;

    public FakeSchemaTransform(Configuration config) {
      this.config = config;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      return null;
    }
  }

  @Test
  public void testFrom() {
    SchemaTransformProvider provider = new FakeTypedSchemaIOProvider();
    Row inputConfig =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("field1", "field1")
            .withFieldValue("field2", Integer.valueOf(13))
            .build();

    Configuration outputConfig = ((FakeSchemaTransform) provider.from(inputConfig)).config;
    assertEquals("field1", outputConfig.getField1());
    assertEquals(13, outputConfig.getField2().intValue());
  }

  @Test
  public void testDependencies() {
    SchemaTransformProvider provider = new FakeTypedSchemaIOProvider();
    Row inputConfig =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("field1", "field1")
            .withFieldValue("field2", Integer.valueOf(13))
            .build();

    assertEquals(Arrays.asList("field1", "13"), provider.dependencies(inputConfig, null).get());
  }
}

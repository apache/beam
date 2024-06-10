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

import static org.apache.beam.sdk.schemas.transforms.SchemaTransformProviderTranslation.SchemaTransformTranslator;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.providers.FlattenTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.JavaExplodeTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.LoggingTransformProvider;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;

public class SchemaTransformProviderTranslationTest {
  public void translateAndRunChecks(SchemaTransformProvider provider, Row originalRow) {
    SchemaTransform<?> transform = provider.from(originalRow);

    SchemaTransformTranslator translator = new SchemaTransformTranslator(provider.identifier());
    Row rowFromTransform = translator.toConfigRow(transform);

    SchemaTransform<?> transformFromRow =
        translator.fromConfigRow(rowFromTransform, PipelineOptionsFactory.create());

    assertEquals(originalRow, rowFromTransform);
    assertEquals(originalRow, transformFromRow.getConfigurationRow());
    assertEquals(
        provider.configurationSchema(), transformFromRow.getConfigurationRow().getSchema());
  }

  @Test
  public void testReCreateJavaExplodeTransform() {
    JavaExplodeTransformProvider provider = new JavaExplodeTransformProvider();

    Row originalRow =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("crossProduct", true)
            .withFieldValue("fields", Arrays.asList("a", "c"))
            .build();

    translateAndRunChecks(provider, originalRow);
  }

  @Test
  public void testReCreateFlattenTransform() {
    FlattenTransformProvider provider = new FlattenTransformProvider();
    Row originalRow = Row.withSchema(provider.configurationSchema()).build();
    translateAndRunChecks(provider, originalRow);
  }

  @Test
  public void testReCreateLoggingTransform() {
    LoggingTransformProvider provider = new LoggingTransformProvider();
    Row originalRow =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("level", "INFO")
            .withFieldValue("prefix", "some_prefix")
            .build();
    translateAndRunChecks(provider, originalRow);
  }
}

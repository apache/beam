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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import org.apache.beam.sdk.io.iceberg.AddFilesSchemaTransformProvider.Configuration;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AddFilesSchemaTransformProviderTest {

  private static Configuration.Builder base() {
    return Configuration.builder()
        .setTable("default.t")
        .setCatalogProperties(ImmutableMap.of("type", "hadoop", "warehouse", "/tmp/w"));
  }

  @Test
  public void testNoEvolutionSettingsMeanEvolutionDisabled() {
    assertNull(base().build().getSchemaEvolution());
    assertNull(
        base().setSchemaEvolutionOptions(Collections.emptyList()).build().getSchemaEvolution());
  }

  @Test
  public void testOptionsParsedCaseInsensitively() {
    SchemaEvolutionConfig config =
        base()
            .setSchemaEvolutionOptions(
                Arrays.asList("allow_field_addition", " ALLOW_TYPE_PROMOTION "))
            .build()
            .getSchemaEvolution();
    assertNotNull(config);
    assertEquals(
        EnumSet.of(
            SchemaEvolutionOption.ALLOW_FIELD_ADDITION, SchemaEvolutionOption.ALLOW_TYPE_PROMOTION),
        config.getOptions());
    assertNull(config.getIncompatibleSchemaHandling());
  }

  @Test
  public void testPinsAndHandlingParsed() {
    SchemaEvolutionConfig config =
        base()
            .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_RELAXATION"))
            .setRequiredColumns(Arrays.asList("id", "address.city"))
            .setIncompatibleSchemaHandling("route_to_errors")
            .setErrorHandling(ErrorHandling.builder().setOutput("errors").build())
            .build()
            .getSchemaEvolution();
    assertNotNull(config);
    assertTrue(config.isPinned("address.city"));
    assertEquals(
        SchemaEvolutionConfig.IncompatibleSchemaHandling.ROUTE_TO_ERRORS,
        config.getIncompatibleSchemaHandling());
  }

  @Test
  public void testUnverifiableFileHandlingParsedAndDefaultsToReject() {
    SchemaEvolutionConfig config =
        base()
            .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_ADDITION"))
            .setUnverifiableFileHandling(" accept ")
            .build()
            .getSchemaEvolution();
    assertNotNull(config);
    assertEquals(
        SchemaEvolutionConfig.UnverifiableFileHandling.ACCEPT,
        config.getUnverifiableFileHandling());
    assertEquals(
        SchemaEvolutionConfig.UnverifiableFileHandling.REJECT,
        base()
            .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_ADDITION"))
            .build()
            .getSchemaEvolution()
            .getUnverifiableFileHandling());
  }

  @Test
  public void testInvalidOptionListsValidValues() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                base()
                    .setSchemaEvolutionOptions(Arrays.asList("ALLOW_EVERYTHING"))
                    .build()
                    .getSchemaEvolution());
    assertTrue(e.getMessage(), e.getMessage().contains("ALLOW_FIELD_ADDITION"));
    assertTrue(e.getMessage(), e.getMessage().contains("ALLOW_EVERYTHING"));
  }

  @Test
  public void testInvalidHandlingValuesRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            base()
                .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_ADDITION"))
                .setIncompatibleSchemaHandling("ignore")
                .build()
                .getSchemaEvolution());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            base()
                .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_ADDITION"))
                .setUnverifiableFileHandling("trust")
                .build()
                .getSchemaEvolution());
  }

  @Test
  public void testRouteToErrorsWithoutErrorHandlingRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                base()
                    .setSchemaEvolutionOptions(Arrays.asList("ALLOW_FIELD_ADDITION"))
                    .setIncompatibleSchemaHandling("ROUTE_TO_ERRORS")
                    .build()
                    .getSchemaEvolution());

    assertTrue(e.getMessage(), e.getMessage().contains("error_handling"));
  }

  /** The config's "needs options" rule, reported under the YAML key the user must set. */
  @Test
  public void testSettingsWithoutOptionsRejectedNamingTheOptionsKey() {
    IllegalArgumentException pins =
        assertThrows(
            IllegalArgumentException.class,
            () -> base().setRequiredColumns(Arrays.asList("id")).build().getSchemaEvolution());
    IllegalArgumentException unverifiable =
        assertThrows(
            IllegalArgumentException.class,
            () -> base().setUnverifiableFileHandling("ACCEPT").build().getSchemaEvolution());

    assertTrue(pins.getMessage(), pins.getMessage().contains("schema_evolution_options"));
    assertTrue(
        unverifiable.getMessage(), unverifiable.getMessage().contains("schema_evolution_options"));
  }
}

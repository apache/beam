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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import org.apache.beam.sdk.io.iceberg.SchemaEvolutionConfig.IncompatibleSchemaHandling;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaEvolutionConfigTest {

  @Test
  public void testEnabledOnlyWithOptions() {
    assertFalse(SchemaEvolutionConfig.disabled().isEnabled());

    SchemaEvolutionConfig config =
        SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION);

    assertTrue(config.isEnabled());
    assertTrue(config.allows(SchemaEvolutionOption.ALLOW_FIELD_ADDITION));
    assertFalse(config.allows(SchemaEvolutionOption.ALLOW_FIELD_RELAXATION));
  }

  @Test
  public void testBlankOrPaddedPinIsRejected() {
    for (String bad : new String[] {"  ", " id", "address.city\t"}) {
      SchemaEvolutionConfig.Builder builder =
          SchemaEvolutionConfig.builder()
              .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
              .setRequiredColumns(new HashSet<>(Collections.singletonList(bad)));

      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, builder::build);

      assertTrue(e.getMessage(), e.getMessage().contains("'" + bad + "'"));
    }
  }

  @Test
  public void testPinsAndHandlingRequireAnOption() {
    SchemaEvolutionConfig.Builder pinsOnly =
        SchemaEvolutionConfig.builder().setRequiredColumns(Collections.singleton("id"));
    SchemaEvolutionConfig.Builder handlingOnly =
        SchemaEvolutionConfig.builder()
            .setIncompatibleSchemaHandling(IncompatibleSchemaHandling.ROUTE_TO_ERRORS);

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, pinsOnly::build);
    assertThrows(IllegalArgumentException.class, handlingOnly::build);

    assertTrue(e.getMessage(), e.getMessage().contains("at least one schema evolution option"));
  }

  @Test
  public void testIncompatibleSchemaHandlingDefaultsByMode() {
    SchemaEvolutionConfig unset =
        SchemaEvolutionConfig.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION);
    SchemaEvolutionConfig forced =
        SchemaEvolutionConfig.builder()
            .setOptions(EnumSet.of(SchemaEvolutionOption.ALLOW_FIELD_ADDITION))
            .setIncompatibleSchemaHandling(IncompatibleSchemaHandling.ROUTE_TO_ERRORS)
            .build();

    assertEquals(IncompatibleSchemaHandling.FAIL_PIPELINE, unset.incompatibleSchemaHandling(true));
    assertEquals(
        IncompatibleSchemaHandling.ROUTE_TO_ERRORS, unset.incompatibleSchemaHandling(false));
    assertEquals(
        IncompatibleSchemaHandling.ROUTE_TO_ERRORS, forced.incompatibleSchemaHandling(true));
  }
}

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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.iceberg.SchemaChange.Kind;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SchemaChangeTest {

  private static final Pins NO_PINS = new Pins(Collections.emptyList());
  private static final Pins CITY_PINNED = new Pins(Arrays.asList("address.city"));

  @Test
  public void testDisallowedReasonPerKind() {
    // A conflict's description stands alone: no option unblocks it, so no "(needs ...)" suffix.
    assertEquals(
        "type changed on name",
        new SchemaChange(Kind.CONFLICT, "", "type changed on name").disallowedReason(NO_PINS));
    assertEquals(
        "add optional email string (needs ALLOW_FIELD_ADDITION)",
        new SchemaChange(Kind.FIELD_ADDITION, "email", "add optional email string")
            .disallowedReason(NO_PINS));
    assertEquals(
        "relax name to optional (needs ALLOW_FIELD_RELAXATION)",
        new SchemaChange(Kind.FIELD_RELAXATION, "name", "relax name to optional")
            .disallowedReason(CITY_PINNED));
    assertEquals(
        "relax address.city to optional (pinned as required)",
        new SchemaChange(Kind.FIELD_RELAXATION, "address.city", "relax address.city to optional")
            .disallowedReason(CITY_PINNED));
    assertEquals(
        "relax address to optional (ancestor of pinned column address.city)",
        new SchemaChange(Kind.FIELD_RELAXATION, "address", "relax address to optional")
            .disallowedReason(CITY_PINNED));
  }

  @Test
  public void testAllowedBy() {
    SchemaEvolutionConfig all = SchemaEvolutionConfig.of(SchemaEvolutionOption.values());
    assertFalse(new SchemaChange(Kind.CONFLICT, "", "boom").allowedBy(all, NO_PINS));
    assertTrue(new SchemaChange(Kind.FIELD_ADDITION, "email", "add").allowedBy(all, NO_PINS));
    assertFalse(
        new SchemaChange(Kind.FIELD_ADDITION, "email", "add")
            .allowedBy(SchemaEvolutionConfig.disabled(), NO_PINS));
    assertFalse(
        new SchemaChange(Kind.FIELD_RELAXATION, "address.city", "relax")
            .allowedBy(all, CITY_PINNED));
    assertFalse(
        new SchemaChange(Kind.FIELD_RELAXATION, "address", "relax").allowedBy(all, CITY_PINNED));
    assertTrue(
        new SchemaChange(Kind.FIELD_RELAXATION, "name", "relax").allowedBy(all, CITY_PINNED));
  }
}

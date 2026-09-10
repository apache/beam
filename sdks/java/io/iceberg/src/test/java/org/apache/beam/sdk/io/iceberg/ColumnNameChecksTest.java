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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.io.iceberg.SchemaChange.Kind;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ColumnNameChecksTest {

  private static List<String> invalidNames(Types.StructType fileStruct) {
    List<SchemaChange> changes = new ArrayList<>();
    ColumnNameChecks.findInvalidNames(fileStruct, "", changes);
    return conflictDescriptions(changes);
  }

  private static List<String> caseCollisions(
      Types.StructType tableStruct, Types.StructType fileStruct) {
    List<SchemaChange> changes = new ArrayList<>();
    ColumnNameChecks.findCaseCollisions(tableStruct, fileStruct, "", changes);
    return conflictDescriptions(changes);
  }

  private static List<String> conflictDescriptions(List<SchemaChange> changes) {
    List<String> descriptions = new ArrayList<>();
    for (SchemaChange change : changes) {
      assertEquals(Kind.CONFLICT, change.kind);
      descriptions.add(change.description);
    }
    return descriptions;
  }

  @Test
  public void testFindInvalidNamesFlagsDottedNamesAtEveryLevel() {
    Types.StructType file =
        Types.StructType.of(
            optional(1, "a.b", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "c.d", Types.IntegerType.get()))),
            optional(
                4,
                "l",
                Types.ListType.ofOptional(
                    5, Types.StructType.of(optional(6, "e.f", Types.StringType.get())))),
            optional(
                7,
                "m",
                Types.MapType.ofOptional(
                    8,
                    9,
                    Types.StringType.get(),
                    Types.StructType.of(optional(10, "g.h", Types.StringType.get())))));
    List<String> conflicts = invalidNames(file);
    assertEquals(conflicts.toString(), 4, conflicts.size());
    for (String name : Arrays.asList("`a.b`", "`c.d`", "`e.f`", "`g.h`")) {
      assertTrue(conflicts.toString(), conflicts.toString().contains(name));
    }
  }

  @Test
  public void testFindInvalidNamesFlagsEmptyNamesAtEveryLevel() {
    Types.StructType file =
        Types.StructType.of(
            optional(1, "", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "", Types.IntegerType.get()))),
            optional(
                4,
                "l",
                Types.ListType.ofOptional(
                    5, Types.StructType.of(optional(6, "", Types.StringType.get())))),
            optional(
                7,
                "m",
                Types.MapType.ofOptional(
                    8,
                    9,
                    Types.StringType.get(),
                    Types.StructType.of(optional(10, "", Types.StringType.get())))));
    assertEquals(
        Arrays.asList(
            "empty column name",
            "empty column name under s",
            "empty column name under l.element",
            "empty column name under m.value"),
        invalidNames(file));
  }

  @Test
  public void testFindInvalidNamesFlagsCaseDuplicatesPerLevel() {
    Types.StructType file =
        Types.StructType.of(
            optional(1, "email", Types.StringType.get()),
            optional(2, "EMAIL", Types.StringType.get()),
            optional(
                3,
                "l",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        optional(5, "lat", Types.DoubleType.get()),
                        optional(6, "LAT", Types.DoubleType.get())))));
    List<String> conflicts = invalidNames(file);
    assertEquals(conflicts.toString(), 2, conflicts.size());
    assertTrue(
        conflicts.toString(), conflicts.get(0).contains("email and EMAIL differ only in case"));
    assertTrue(
        conflicts.toString(),
        conflicts.get(1).contains("l.element.lat and l.element.LAT differ only in case"));
  }

  /** The duplicate rule is per level: the same name at different levels is fine. */
  @Test
  public void testFindInvalidNamesAcceptsCleanSchemas() {
    Types.StructType file =
        Types.StructType.of(
            optional(1, "name", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "NAME", Types.StringType.get()))));
    assertEquals(Collections.emptyList(), invalidNames(file));
  }

  @Test
  public void testFindCaseCollisionsAtEveryLevel() {
    Types.StructType table =
        Types.StructType.of(
            optional(1, "name", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "city", Types.StringType.get()))),
            optional(
                4,
                "l",
                Types.ListType.ofOptional(
                    5, Types.StructType.of(optional(6, "sku", Types.StringType.get())))),
            optional(
                7,
                "m",
                Types.MapType.ofOptional(
                    8,
                    9,
                    Types.StringType.get(),
                    Types.StructType.of(optional(10, "v", Types.StringType.get())))));
    Types.StructType file =
        Types.StructType.of(
            optional(1, "NAME", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "CITY", Types.StringType.get()))),
            optional(
                4,
                "l",
                Types.ListType.ofOptional(
                    5, Types.StructType.of(optional(6, "SKU", Types.StringType.get())))),
            optional(
                7,
                "m",
                Types.MapType.ofOptional(
                    8,
                    9,
                    Types.StringType.get(),
                    Types.StructType.of(optional(10, "V", Types.StringType.get())))));
    assertEquals(
        Arrays.asList(
            "column NAME differs only in case from table column name;"
                + " rename it or map it with a column alias",
            "column s.CITY differs only in case from table column city;"
                + " rename it or map it with a column alias",
            "column l.element.SKU differs only in case from table column sku;"
                + " rename it or map it with a column alias",
            "column m.value.V differs only in case from table column v;"
                + " rename it or map it with a column alias"),
        caseCollisions(table, file));
  }

  @Test
  public void testFindCaseCollisionsPassesExactNewAndKindMismatchedNames() {
    Types.StructType table =
        Types.StructType.of(
            optional(1, "name", Types.StringType.get()),
            optional(2, "s", Types.StructType.of(optional(3, "x", Types.IntegerType.get()))));
    Types.StructType file =
        Types.StructType.of(
            optional(1, "name", Types.StringType.get()),
            optional(2, "email", Types.StringType.get()),
            optional(3, "s", Types.StringType.get()));
    assertEquals(Collections.emptyList(), caseCollisions(table, file));
  }
}

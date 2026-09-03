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
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FileSchemasTest {

  @Test
  public void testSortsTopLevelFieldsAndRenumbers() {
    Schema input =
        new Schema(
            optional(7, "name", Types.StringType.get()), required(3, "id", Types.LongType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "name", Types.StringType.get()));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testSortsNestedStructFields() {
    Schema input =
        new Schema(
            optional(
                1,
                "address",
                Types.StructType.of(
                    optional(2, "zip", Types.IntegerType.get()),
                    optional(3, "city", Types.StringType.get()))));
    Schema expected =
        new Schema(
            optional(
                1,
                "address",
                Types.StructType.of(
                    optional(2, "city", Types.StringType.get()),
                    optional(3, "zip", Types.IntegerType.get()))));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testPermutationsProduceIdenticalJson() {
    Schema a =
        new Schema(
            optional(1, "b", Types.StringType.get()),
            optional(2, "a", Types.StructType.of(optional(3, "y", Types.LongType.get()))),
            optional(4, "c", Types.ListType.ofOptional(5, Types.StringType.get())));
    Schema b =
        new Schema(
            optional(1, "c", Types.ListType.ofOptional(2, Types.StringType.get())),
            optional(3, "a", Types.StructType.of(optional(4, "y", Types.LongType.get()))),
            optional(5, "b", Types.StringType.get()));

    assertEquals(
        SchemaParser.toJson(FileSchemas.canonical(a)),
        SchemaParser.toJson(FileSchemas.canonical(b)));
  }

  /** Ids number every field of a struct before descending into nested types. */
  @Test
  public void testPreservesListMapStructNestingAndOptionality() {
    Schema input =
        new Schema(
            required(
                1,
                "m",
                Types.MapType.ofRequired(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        optional(4, "z", Types.IntegerType.get()),
                        required(5, "a", Types.ListType.ofRequired(6, Types.DoubleType.get()))))));
    Schema expected =
        new Schema(
            required(
                1,
                "m",
                Types.MapType.ofRequired(
                    2,
                    3,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(4, "a", Types.ListType.ofRequired(6, Types.DoubleType.get())),
                        optional(5, "z", Types.IntegerType.get())))));

    assertSame(expected, FileSchemas.canonical(input));
  }

  @Test
  public void testCanonicalSchemaIsUnchanged() {
    Schema canonical =
        new Schema(
            required(1, "a", Types.LongType.get()),
            optional(2, "b", Types.StructType.of(optional(3, "x", Types.StringType.get()))));

    assertSame(canonical, FileSchemas.canonical(canonical));
  }

  private static void assertSame(Schema expected, Schema actual) {
    assertTrue("expected " + expected + " but was " + actual, expected.sameSchema(actual));
  }
}

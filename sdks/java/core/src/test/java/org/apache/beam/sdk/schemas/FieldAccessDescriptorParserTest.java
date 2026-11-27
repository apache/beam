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
package org.apache.beam.sdk.schemas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FieldAccessDescriptorParser}. */
@RunWith(JUnit4.class)
public class FieldAccessDescriptorParserTest {
  @Test
  public void testSmoke() {
    FieldAccessDescriptorParser.parse("a.b[][*].c.*");
  }

  @Test
  public void testIsSimpleIdentifier() {
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("a"));
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("foo_bar"));
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("$field123"));
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("123"));
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("azAZ09_$"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(null));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(""));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("*"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a.b"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a[]"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a b"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a-b"));
    // ASCII neighbors right outside [0-9], [A-Z], [a-z] and non-ASCII digits.
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("/"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(":"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("@"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("["));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("`"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("{"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("\u0660"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a\tb"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a\nb"));
  }

  @Test
  public void testIsSimpleIdentifierSliceBounds() {
    assertTrue(FieldAccessDescriptorParser.isSimpleIdentifier("a.foo_1.b", 2, 7));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a.foo_1.b", 1, 7));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("abc", 2, 2));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("abc", 2, 1));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("abc", -1, 2));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("abc", 0, 4));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(null, 0, 1));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(null, 0, 0));
  }

  @Test
  public void testSimpleIdentifierAndWildcardParseParityWithAntlr() {
    FieldAccessDescriptor expected =
        FieldAccessDescriptor.withFields(
            FieldDescriptor.builder().setFieldName("simple_field").build());
    assertEquals(expected, FieldAccessDescriptorParser.parse("simple_field"));
    // Whitespace forces ANTLR4 parsing so this validates exact parity against the fast path.
    assertEquals(expected, FieldAccessDescriptorParser.parse(" simple_field "));

    assertEquals(FieldAccessDescriptor.withAllFields(), FieldAccessDescriptorParser.parse("*"));
    assertEquals(FieldAccessDescriptor.withAllFields(), FieldAccessDescriptorParser.parse(" * "));

    FieldAccessDescriptor expectedNumeric =
        FieldAccessDescriptor.withFields(FieldDescriptor.builder().setFieldName("123").build());
    assertEquals(expectedNumeric, FieldAccessDescriptorParser.parse("123"));
    assertEquals(expectedNumeric, FieldAccessDescriptorParser.parse(" 123 "));
  }

  @Test
  public void testDottedPathAndTerminalWildcardParse() {
    FieldAccessDescriptor expectedTwo =
        FieldAccessDescriptor.create()
            .withNestedField(
                FieldDescriptor.builder().setFieldName("a").build(),
                FieldAccessDescriptor.withFields(
                    FieldDescriptor.builder().setFieldName("b").build()));
    assertEquals(expectedTwo, FieldAccessDescriptorParser.parse("a.b"));
    assertEquals(expectedTwo, FieldAccessDescriptorParser.parse(" a . b "));

    FieldAccessDescriptor expectedThree =
        FieldAccessDescriptor.create()
            .withNestedField(
                FieldDescriptor.builder().setFieldName("a").build(),
                FieldAccessDescriptor.create()
                    .withNestedField(
                        FieldDescriptor.builder().setFieldName("b").build(),
                        FieldAccessDescriptor.withFields(
                            FieldDescriptor.builder().setFieldName("c").build())));
    assertEquals(expectedThree, FieldAccessDescriptorParser.parse("a.b.c"));
    assertEquals(expectedThree, FieldAccessDescriptorParser.parse(" a.b.c "));

    FieldAccessDescriptor expectedOneWildcard =
        FieldAccessDescriptor.create()
            .withNestedField(
                FieldDescriptor.builder().setFieldName("a").build(),
                FieldAccessDescriptor.withAllFields());
    assertEquals(expectedOneWildcard, FieldAccessDescriptorParser.parse("a.*"));
    assertEquals(expectedOneWildcard, FieldAccessDescriptorParser.parse(" a.* "));

    FieldAccessDescriptor expectedTwoWildcard =
        FieldAccessDescriptor.create()
            .withNestedField(
                FieldDescriptor.builder().setFieldName("a").build(),
                FieldAccessDescriptor.create()
                    .withNestedField(
                        FieldDescriptor.builder().setFieldName("b").build(),
                        FieldAccessDescriptor.withAllFields()));
    assertEquals(expectedTwoWildcard, FieldAccessDescriptorParser.parse("a.b.*"));
    assertEquals(expectedTwoWildcard, FieldAccessDescriptorParser.parse(" a.b.* "));

    // Qualified expressions fall back to ANTLR4 cleanly.
    assertEquals(
        FieldAccessDescriptorParser.parse(" a.b[] "), FieldAccessDescriptorParser.parse("a.b[]"));
    assertEquals(
        FieldAccessDescriptorParser.parse(" a.b{} "), FieldAccessDescriptorParser.parse("a.b{}"));

    // Non-terminal wildcard must still throw via ANTLR fallback.
    assertThrows(IllegalArgumentException.class, () -> FieldAccessDescriptorParser.parse("*.b"));
    assertThrows(IllegalArgumentException.class, () -> FieldAccessDescriptorParser.parse("a.*.b"));
  }
}

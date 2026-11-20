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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.junit.Test;

/** Tests for {@link FieldAccessDescriptorParser}. */
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
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(null));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier(""));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("*"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a.b"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a[]"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a b"));
    assertFalse(FieldAccessDescriptorParser.isSimpleIdentifier("a-b"));
  }

  @Test
  public void testSimpleIdentifierAndWildcardParse() {
    assertEquals(
        FieldAccessDescriptor.withFieldNames("simple_field"),
        FieldAccessDescriptorParser.parse("simple_field"));
    assertEquals(FieldAccessDescriptor.withAllFields(), FieldAccessDescriptorParser.parse("*"));
  }
}

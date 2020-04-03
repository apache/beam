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
import static org.junit.Assert.assertTrue;

import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor.FieldDescriptor;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link FieldAccessDescriptor}. */
public class FieldAccessDescriptorTest {
  private static final Schema SIMPLE_SCHEMA =
      Schema.builder()
          .addStringField("field0")
          .addStringField("field1")
          .addInt32Field("field2")
          .addInt32Field("field3")
          .build();

  private static final Schema NESTED_SCHEMA1 =
      Schema.builder().addStringField("field0").addRowField("field1", SIMPLE_SCHEMA).build();

  private static final Schema NESTED_SCHEMA2 =
      Schema.builder().addStringField("field0").addRowField("field1", NESTED_SCHEMA1).build();

  private static final Schema NESTED_ARRAY_SCHEMA =
      Schema.builder()
          .addStringField("field0")
          .addArrayField("field1", FieldType.row(SIMPLE_SCHEMA))
          .build();

  private static final Schema NESTED_MAP_SCHEMA =
      Schema.builder()
          .addStringField("field0")
          .addMapField("field1", FieldType.STRING, FieldType.row(SIMPLE_SCHEMA))
          .build();

  // test all fields
  @Test
  public void testAllFields() {
    FieldAccessDescriptor fieldAccessDescriptor = FieldAccessDescriptor.withAllFields();
    assertTrue(fieldAccessDescriptor.resolve(SIMPLE_SCHEMA).getAllFields());
  }

  // test field names
  @Test
  public void testFieldNames() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0", "field2").resolve(SIMPLE_SCHEMA);
    assertEquals(ImmutableList.of(0, 2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testFieldIds() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldIds(1, 3).resolve(SIMPLE_SCHEMA);
    assertEquals(ImmutableList.of(1, 3), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testNestedFieldByName() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1")
            .withNestedField("field1", FieldAccessDescriptor.withAllFields());
    fieldAccessDescriptor = fieldAccessDescriptor.resolve(NESTED_SCHEMA2);
    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertTrue(nestedAccess.getAllFields());
  }

  @Test
  public void testNestedFieldById() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1")
            .withNestedField(1, FieldAccessDescriptor.withAllFields());
    fieldAccessDescriptor = fieldAccessDescriptor.resolve(NESTED_SCHEMA2);
    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertTrue(nestedAccess.getAllFields());
  }

  @Test
  public void testPartialAccessNestedField() {
    FieldAccessDescriptor level1 = FieldAccessDescriptor.withFieldNames("field2");
    FieldAccessDescriptor level2 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level1);
    FieldAccessDescriptor level3 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level2);

    FieldAccessDescriptor resolved = level3.resolve(NESTED_SCHEMA2);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFieldsById().size());
    resolved = resolved.nestedFieldsById().get(1);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFieldsById().size());
    resolved = resolved.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), resolved.fieldIdsAccessed());
  }

  @Test
  public void testArrayNestedField() {
    FieldAccessDescriptor level1 = FieldAccessDescriptor.withFieldNames("field2");
    FieldAccessDescriptor level2 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level1);

    FieldAccessDescriptor resolved = level2.resolve(NESTED_ARRAY_SCHEMA);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFieldsById().size());
    resolved = resolved.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), resolved.fieldIdsAccessed());
  }

  @Test
  public void testMapNestedField() {
    FieldAccessDescriptor level1 = FieldAccessDescriptor.withFieldNames("field2");
    FieldAccessDescriptor level2 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level1);

    FieldAccessDescriptor resolved = level2.resolve(NESTED_MAP_SCHEMA);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFieldsById().size());
    resolved = resolved.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), resolved.fieldIdsAccessed());
  }

  @Test
  public void testParseAllFields() {
    FieldAccessDescriptor fieldAccessDescriptor = FieldAccessDescriptor.withFieldNames("*");
    assertTrue(fieldAccessDescriptor.resolve(SIMPLE_SCHEMA).getAllFields());
  }

  @Test
  public void testParseNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0", "field1.*").resolve(NESTED_SCHEMA2);
    assertEquals(1, fieldAccessDescriptor.getFieldsAccessed().size());
    assertEquals(
        "field0", fieldAccessDescriptor.getFieldsAccessed().iterator().next().getFieldName());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertTrue(nestedAccess.getAllFields());
  }

  @Test
  public void testParseSiblingFields() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1.field0", "field1.field1")
            .resolve(NESTED_SCHEMA1);
    assertEquals(0, fieldAccessDescriptor.getFieldsAccessed().size());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertEquals(
        ImmutableSet.of(0, 1),
        nestedAccess.getFieldsAccessed().stream()
            .map(FieldDescriptor::getFieldId)
            .collect(Collectors.toSet()));
  }

  @Test
  public void testParseShortCircuit() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0", "field1.field0", "field1")
            .resolve(NESTED_SCHEMA2);
    assertEquals(2, fieldAccessDescriptor.getFieldsAccessed().size());
    assertEquals(
        ImmutableSet.of(0, 1),
        fieldAccessDescriptor.getFieldsAccessed().stream()
            .map(FieldDescriptor::getFieldId)
            .collect(Collectors.toSet()));
    assertTrue(fieldAccessDescriptor.nestedFieldsById().isEmpty());
  }

  @Test
  public void testParseWildcardShortCircuit() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0", "field1.field0", "field1.*")
            .resolve(NESTED_SCHEMA2);
    assertEquals(1, fieldAccessDescriptor.getFieldsAccessed().size());
    assertEquals(
        "field0", fieldAccessDescriptor.getFieldsAccessed().iterator().next().getFieldName());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertTrue(nestedAccess.getAllFields());
  }

  @Test
  public void testParsePartialAccessNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1.field1.field2").resolve(NESTED_SCHEMA2);

    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testParseArrayNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1[].field2").resolve(NESTED_ARRAY_SCHEMA);

    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testParseMapNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1{}.field2").resolve(NESTED_MAP_SCHEMA);

    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(1);
    assertEquals(ImmutableList.of(2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  private static final Schema DOUBLE_NESTED_ARRAY_SCHEMA =
      Schema.builder()
          .addArrayField("field0", FieldType.array(FieldType.row(SIMPLE_SCHEMA)))
          .build();

  @Test
  public void testParseDoubleArrayNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0[][].field2")
            .resolve(DOUBLE_NESTED_ARRAY_SCHEMA);

    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(0);
    assertEquals(ImmutableList.of(2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testInvalidQualifier() {
    thrown.expect(IllegalArgumentException.class);
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0[]{}.field2")
            .resolve(DOUBLE_NESTED_ARRAY_SCHEMA);
  }

  private static final Schema NESTED_ARRAY_MAP_SCHEMA =
      Schema.builder()
          .addArrayField("field0", FieldType.map(FieldType.STRING, FieldType.row(SIMPLE_SCHEMA)))
          .build();

  @Test
  public void testParseArrayMapNestedField() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0[]{}.field2").resolve(NESTED_ARRAY_MAP_SCHEMA);

    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFieldsById().size());
    fieldAccessDescriptor = fieldAccessDescriptor.nestedFieldsById().get(0);
    assertEquals(ImmutableList.of(2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testFieldInsertionOrdering() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field3", "field2", "field1", "field0")
            .resolve(SIMPLE_SCHEMA);

    assertEquals(ImmutableList.of(3, 2, 1, 0), fieldAccessDescriptor.fieldIdsAccessed());
  }
}

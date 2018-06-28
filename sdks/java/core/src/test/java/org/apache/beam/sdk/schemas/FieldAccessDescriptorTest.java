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

import com.google.common.collect.Sets;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.Test;

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
    assertTrue(fieldAccessDescriptor.resolve(SIMPLE_SCHEMA).allFields());
  }

  // test field names
  @Test
  public void testFieldNames() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field0", "field2").resolve(SIMPLE_SCHEMA);
    assertEquals(Sets.newHashSet(0, 2), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testFieldIds() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldIds(1, 3).resolve(SIMPLE_SCHEMA);
    assertEquals(Sets.newHashSet(1, 3), fieldAccessDescriptor.fieldIdsAccessed());
  }

  @Test
  public void testNestedFieldByName() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1")
            .withNestedField("field1", FieldAccessDescriptor.withAllFields());
    fieldAccessDescriptor = fieldAccessDescriptor.resolve(NESTED_SCHEMA2);
    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFields().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFields().get(1);
    assertTrue(nestedAccess.allFields());
  }

  @Test
  public void testNestedFieldById() {
    FieldAccessDescriptor fieldAccessDescriptor =
        FieldAccessDescriptor.withFieldNames("field1")
            .withNestedField(1, FieldAccessDescriptor.withAllFields());
    fieldAccessDescriptor = fieldAccessDescriptor.resolve(NESTED_SCHEMA2);
    assertTrue(fieldAccessDescriptor.fieldIdsAccessed().isEmpty());
    assertEquals(1, fieldAccessDescriptor.nestedFields().size());
    FieldAccessDescriptor nestedAccess = fieldAccessDescriptor.nestedFields().get(1);
    assertTrue(nestedAccess.allFields());
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
    assertEquals(1, resolved.nestedFields().size());
    resolved = resolved.nestedFields().get(1);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFields().size());
    resolved = resolved.nestedFields().get(1);
    assertEquals(Sets.newHashSet(2), resolved.fieldIdsAccessed());
  }

  @Test
  public void testArrayNestedField() {
    FieldAccessDescriptor level1 = FieldAccessDescriptor.withFieldNames("field2");
    FieldAccessDescriptor level2 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level1);

    FieldAccessDescriptor resolved = level2.resolve(NESTED_ARRAY_SCHEMA);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFields().size());
    resolved = resolved.nestedFields().get(1);
    assertEquals(Sets.newHashSet(2), resolved.fieldIdsAccessed());
  }

  @Test
  public void testMapNestedField() {
    FieldAccessDescriptor level1 = FieldAccessDescriptor.withFieldNames("field2");
    FieldAccessDescriptor level2 =
        FieldAccessDescriptor.withFieldNames("field1").withNestedField("field1", level1);

    FieldAccessDescriptor resolved = level2.resolve(NESTED_MAP_SCHEMA);
    assertTrue(resolved.fieldIdsAccessed().isEmpty());
    assertEquals(1, resolved.nestedFields().size());
    resolved = resolved.nestedFields().get(1);
    assertEquals(Sets.newHashSet(2), resolved.fieldIdsAccessed());
  }
}

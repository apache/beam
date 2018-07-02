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
package org.apache.beam.sdk.schemas.utils;

import static org.junit.Assert.assertEquals;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;

/** Utilities for testing schemas. */
public class SchemaTestUtils {
  // Assert that two schemas are equivalent, ignoring field order. This tests that both schemas
  // (recursively) contain the same fields with the same names, but possibly different orders.
  public static void assertSchemaEquivalent(Schema expected, Schema actual) {
    List<Field> expectedFields =
        expected
            .getFields()
            .stream()
            .sorted(Comparator.comparing(Field::getName))
            .collect(Collectors.toList());
    List<Field> actualFields =
        actual
            .getFields()
            .stream()
            .sorted(Comparator.comparing(Field::getName))
            .collect(Collectors.toList());
    assertEquals(expectedFields.size(), actualFields.size());

    for (int i = 0; i < expectedFields.size(); ++i) {
      Field expectedField = expectedFields.get(i);
      Field actualField = actualFields.get(i);
      assertFieldEquivalent(expectedField, actualField);
    }
  }

  public static void assertFieldEquivalent(Field expectedField, Field actualField) {
    assertEquals(expectedField.getName(), actualField.getName());
    assertEquals(expectedField.getNullable(), actualField.getNullable());
    assertFieldTypeEquivalent(expectedField.getType(), actualField.getType());
  }

  public static void assertFieldTypeEquivalent(
      FieldType expectedFieldType, FieldType actualFieldType) {
    assertEquals(expectedFieldType.getTypeName(), actualFieldType.getTypeName());
    if (TypeName.ROW.equals(expectedFieldType.getTypeName())) {
      assertSchemaEquivalent(expectedFieldType.getRowSchema(), actualFieldType.getRowSchema());
    } else if (TypeName.ARRAY.equals(expectedFieldType.getTypeName())) {
      assertFieldTypeEquivalent(
          expectedFieldType.getCollectionElementType(), actualFieldType.getCollectionElementType());
    } else if (TypeName.MAP.equals(expectedFieldType.getTypeName())) {
      assertFieldTypeEquivalent(expectedFieldType.getMapKeyType(), actualFieldType.getMapKeyType());
      assertFieldTypeEquivalent(
          expectedFieldType.getMapValueType(), actualFieldType.getMapValueType());
    } else {
      assertEquals(expectedFieldType, actualFieldType);
    }
  }
}

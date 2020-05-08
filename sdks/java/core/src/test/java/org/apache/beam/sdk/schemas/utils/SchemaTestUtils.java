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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

/** Utilities for testing schemas. */
public class SchemaTestUtils {
  // Assert that two schemas are equivalent, ignoring field order. This tests that both schemas
  // (recursively) contain the same fields with the same names, but possibly different orders.
  public static void assertSchemaEquivalent(Schema expected, Schema actual) {
    assertTrue("Expected: " + expected + "  Got: " + actual, actual.equivalent(expected));
  }

  public static class RowEquivalent extends BaseMatcher<Row> {
    private final Row expected;

    public RowEquivalent(Row expected) {
      this.expected = expected;
    }

    @Override
    public boolean matches(Object actual) {
      if (actual == null) {
        return expected == null;
      }
      if (!(actual instanceof Row)) {
        return false;
      }
      Row actualRow = (Row) actual;
      return rowsEquivalent(expected, actualRow);
    }

    private static boolean rowsEquivalent(Row expected, Row actual) {
      if (!actual.getSchema().equivalent(expected.getSchema())) {
        return false;
      }
      if (expected.getFieldCount() != actual.getFieldCount()) {
        return false;
      }
      for (int i = 0; i < expected.getFieldCount(); ++i) {
        Field field = expected.getSchema().getField(i);
        int actualIndex = actual.getSchema().indexOf(field.getName());
        if (!fieldsEquivalent(
            expected.getValue(i), actual.getValue(actualIndex), field.getType())) {
          return false;
        }
      }
      return true;
    }

    private static boolean fieldsEquivalent(Object expected, Object actual, FieldType fieldType) {
      if (expected == null || actual == null) {
        return expected == actual;
      } else if (fieldType.getTypeName() == TypeName.LOGICAL_TYPE) {
        return fieldsEquivalent(expected, actual, fieldType.getLogicalType().getBaseType());
      } else if (fieldType.getTypeName() == Schema.TypeName.BYTES) {
        return Arrays.equals((byte[]) expected, (byte[]) actual);
      } else if (fieldType.getTypeName() == TypeName.ARRAY) {
        return collectionsEquivalent(
            (Collection<Object>) expected,
            (Collection<Object>) actual,
            fieldType.getCollectionElementType());
      } else if (fieldType.getTypeName() == TypeName.ITERABLE) {
        return iterablesEquivalent(
            (Iterable<Object>) expected,
            (Iterable<Object>) actual,
            fieldType.getCollectionElementType());
      } else if (fieldType.getTypeName() == Schema.TypeName.MAP) {
        return mapsEquivalent(
            (Map<Object, Object>) expected,
            (Map<Object, Object>) actual,
            fieldType.getMapValueType());
      } else {
        return Objects.equals(expected, actual);
      }
    }

    static boolean collectionsEquivalent(
        Collection<Object> expected, Collection<Object> actual, Schema.FieldType elementType) {
      if (expected == actual) {
        return true;
      }

      if (expected.size() != actual.size()) {
        return false;
      }

      return iterablesEquivalent(expected, actual, elementType);
    }

    static boolean iterablesEquivalent(
        Iterable<Object> expected, Iterable<Object> actual, Schema.FieldType elementType) {
      if (expected == actual) {
        return true;
      }
      Iterator<Object> actualIter = actual.iterator();
      for (Object currentExpected : expected) {
        if (!actualIter.hasNext()) {
          return false;
        }
        if (!fieldsEquivalent(currentExpected, actualIter.next(), elementType)) {
          return false;
        }
      }
      return !actualIter.hasNext();
    }

    static <K, V> boolean mapsEquivalent(
        Map<K, V> expected, Map<K, V> actual, Schema.FieldType valueType) {
      if (expected == actual) {
        return true;
      }

      if (expected.size() != actual.size()) {
        return false;
      }

      for (Map.Entry<K, V> expectedElement : expected.entrySet()) {
        K key = expectedElement.getKey();
        V value = expectedElement.getValue();
        V otherValue = actual.get(key);

        if (value == null) {
          if (otherValue != null || !actual.containsKey(key)) {
            return false;
          }
        } else {
          if (!fieldsEquivalent(value, otherValue, valueType)) {
            return false;
          }
        }
      }

      return true;
    }

    @Override
    public void describeTo(Description description) {
      description.appendValue(expected);
    }
  }

  public static class RowFieldMatcherIterableFieldAnyOrder extends BaseMatcher<Row> {
    private final int fieldIndex;
    private final Object expected;
    private final FieldType fieldType;

    public RowFieldMatcherIterableFieldAnyOrder(Schema schema, int fieldIndex, Object expected) {
      this.fieldIndex = fieldIndex;
      this.expected = expected;
      this.fieldType = schema.getField(fieldIndex).getType();
    }

    public RowFieldMatcherIterableFieldAnyOrder(Schema schema, String fieldName, Object expected) {
      this.fieldIndex = schema.indexOf(fieldName);
      this.expected = expected;
      this.fieldType = schema.getField(fieldIndex).getType();
    }

    @Override
    public boolean matches(Object item) {
      if (!(item instanceof Row)) {
        return false;
      }
      Row row = (Row) item;
      switch (fieldType.getTypeName()) {
        case ROW:
          if (!row.getSchema().getField(fieldIndex).getType().getTypeName().isCompositeType()) {
            return false;
          }
          Row actualRow = row.getRow(fieldIndex);
          return new RowEquivalent((Row) expected).matches(actualRow);
        case ARRAY:
          Row[] expectedArray = ((List<Row>) expected).toArray(new Row[0]);

          return containsInAnyOrder(expectedArray).matches(row.getArray(fieldIndex));
        case ITERABLE:
          Row[] expectedIterable = Iterables.toArray((Iterable<Row>) expected, Row.class);
          List<Row> actualIterable = Lists.newArrayList(row.getIterable(fieldIndex));
          return containsInAnyOrder(expectedIterable).matches(actualIterable);
        case MAP:
          throw new RuntimeException("Not yet implemented for maps");
        default:
          return equalTo(expected).matches(row.getValue(fieldIndex));
      }
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("rowFieldMatcher: ");
      description.appendText("FieldType: " + fieldType.getTypeName());
      description.appendText(expected.toString());
    }
  }
}

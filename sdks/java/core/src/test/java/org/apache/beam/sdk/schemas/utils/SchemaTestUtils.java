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

import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
          return equalTo((Row) expected).matches(actualRow);
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

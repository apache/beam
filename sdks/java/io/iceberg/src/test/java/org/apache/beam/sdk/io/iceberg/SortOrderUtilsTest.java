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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SortOrderUtilsTest {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));

  @Test
  public void testNullOrEmptyFieldsYieldsUnsorted() {
    assertTrue(SortOrderUtils.toSortOrder(null, SCHEMA).isUnsorted());
    assertTrue(SortOrderUtils.toSortOrder(Collections.emptyList(), SCHEMA).isUnsorted());
  }

  @Test
  public void testIdentityDefaultsToAscNullsFirst() {
    SortOrder order = SortOrderUtils.toSortOrder(Collections.singletonList("id"), SCHEMA);
    assertEquals(1, order.fields().size());
    SortField field = order.fields().get(0);
    assertEquals(SortDirection.ASC, field.direction());
    assertEquals(NullOrder.NULLS_FIRST, field.nullOrder());
  }

  @Test
  public void testDescDefaultsToNullsLast() {
    SortOrder order = SortOrderUtils.toSortOrder(Collections.singletonList("id desc"), SCHEMA);
    SortField field = order.fields().get(0);
    assertEquals(SortDirection.DESC, field.direction());
    assertEquals(NullOrder.NULLS_LAST, field.nullOrder());
  }

  @Test
  public void testExplicitNullOrder() {
    SortOrder order =
        SortOrderUtils.toSortOrder(Collections.singletonList("name asc nulls last"), SCHEMA);
    SortField field = order.fields().get(0);
    assertEquals(SortDirection.ASC, field.direction());
    assertEquals(NullOrder.NULLS_LAST, field.nullOrder());
  }

  @Test
  public void testTransformTerms() {
    SortOrder order =
        SortOrderUtils.toSortOrder(
            Arrays.asList("bucket(id, 4) desc", "day(ts)", "truncate(name, 3) asc nulls first"),
            SCHEMA);
    assertEquals(3, order.fields().size());
    assertEquals(SortDirection.DESC, order.fields().get(0).direction());
    assertEquals("bucket[4]", order.fields().get(0).transform().toString());
    assertEquals(SortDirection.ASC, order.fields().get(1).direction());
    assertEquals("day", order.fields().get(1).transform().toString());
    assertEquals(SortDirection.ASC, order.fields().get(2).direction());
    assertEquals(NullOrder.NULLS_FIRST, order.fields().get(2).nullOrder());
    assertEquals("truncate[3]", order.fields().get(2).transform().toString());
  }

  @Test
  public void testHandlesExtraWhitespace() {
    SortOrder order =
        SortOrderUtils.toSortOrder(Collections.singletonList("  id   desc   "), SCHEMA);
    assertEquals(1, order.fields().size());
    SortField field = order.fields().get(0);
    assertEquals(SortDirection.DESC, field.direction());
    assertEquals(NullOrder.NULLS_LAST, field.nullOrder());
    assertEquals("identity", field.transform().toString());
  }

  @Test
  public void testCaseInsensitive() {
    SortOrder order =
        SortOrderUtils.toSortOrder(Collections.singletonList("id DESC NULLS FIRST"), SCHEMA);
    SortField field = order.fields().get(0);
    assertEquals(SortDirection.DESC, field.direction());
    assertEquals(NullOrder.NULLS_FIRST, field.nullOrder());
  }

  @Test
  public void testBadModifierThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SortOrderUtils.toSortOrder(Collections.singletonList("id upward"), SCHEMA));
  }

  @Test
  public void testUnknownTermThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SortOrderUtils.toSortOrder(Collections.singletonList("sqrt(id)"), SCHEMA));
  }
}

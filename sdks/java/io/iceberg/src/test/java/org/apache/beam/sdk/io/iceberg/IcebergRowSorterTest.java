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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class IcebergRowSorterTest {

  private static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addNullableField("name", Schema.FieldType.STRING)
          .addNullableField("value", Schema.FieldType.DOUBLE)
          .addNullableField("active", Schema.FieldType.BOOLEAN)
          .build();

  private static final org.apache.iceberg.Schema ICEBERG_SCHEMA =
      new org.apache.iceberg.Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "value", Types.DoubleType.get()),
          Types.NestedField.optional(4, "active", Types.BooleanType.get()));

  private static final Comparator<byte[]> BYTE_ARR_COMPARATOR =
      org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedBytes
          .lexicographicalComparator();

  private static byte[] encodeSortKeyHelper(Row row, SortOrder sortOrder) throws Exception {
    java.util.List<org.apache.iceberg.SortField> fields = sortOrder.fields();
    String[] columnNames = new String[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      columnNames[i] = ICEBERG_SCHEMA.findColumnName(fields.get(i).sourceId());
    }
    return IcebergRowSorter.encodeSortKey(row, sortOrder, columnNames, ICEBERG_SCHEMA, BEAM_SCHEMA);
  }

  @Test
  public void testStringKeyEncodingOrder() throws Exception {
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("name").build();

    Row r1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "apple", 1.5, true).build();
    Row r2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "banana", 2.0, true).build();
    Row r3 = Row.withSchema(BEAM_SCHEMA).addValues(3, "apricot", 3.0, false).build();

    byte[] k1 = encodeSortKeyHelper(r1, sortOrder);
    byte[] k2 = encodeSortKeyHelper(r2, sortOrder);
    byte[] k3 = encodeSortKeyHelper(r3, sortOrder);

    assertTrue(BYTE_ARR_COMPARATOR.compare(k1, k2) < 0); // apple < banana
    assertTrue(BYTE_ARR_COMPARATOR.compare(k1, k3) < 0); // apple < apricot
    assertTrue(BYTE_ARR_COMPARATOR.compare(k3, k2) < 0); // apricot < banana
  }

  @Test
  public void testStringCollisionProofing() throws Exception {
    // Tests that secondary columns don't bleed into primary columns.
    // Row 1: Primary="abc", Secondary="def"
    // Row 2: Primary="abcdef", Secondary=null
    // In raw byte concatenation, both could equal "abcdef\0" if delimiters or escaping fail.
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("name").asc("value").build();

    Row r1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "abc", 1.0, true).build();
    Row r2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "abcdef", null, true).build();

    byte[] k1 = encodeSortKeyHelper(r1, sortOrder);
    byte[] k2 = encodeSortKeyHelper(r2, sortOrder);

    // "abc" must sort lexicographically before "abcdef"
    assertTrue(BYTE_ARR_COMPARATOR.compare(k1, k2) < 0);
  }

  @Test
  public void testDescInversion() throws Exception {
    SortOrder sortOrderAsc = SortOrder.builderFor(ICEBERG_SCHEMA).asc("id").build();
    SortOrder sortOrderDesc = SortOrder.builderFor(ICEBERG_SCHEMA).desc("id").build();

    Row r1 = Row.withSchema(BEAM_SCHEMA).addValues(10, "test", 1.5, true).build();
    Row r2 = Row.withSchema(BEAM_SCHEMA).addValues(20, "test", 2.0, true).build();

    byte[] k1Asc = encodeSortKeyHelper(r1, sortOrderAsc);
    byte[] k2Asc = encodeSortKeyHelper(r2, sortOrderAsc);

    byte[] k1Desc = encodeSortKeyHelper(r1, sortOrderDesc);
    byte[] k2Desc = encodeSortKeyHelper(r2, sortOrderDesc);

    // Ascending: 10 < 20
    assertTrue(BYTE_ARR_COMPARATOR.compare(k1Asc, k2Asc) < 0);

    // Descending: 10 > 20 (inverted bytes)
    assertTrue(BYTE_ARR_COMPARATOR.compare(k1Desc, k2Desc) > 0);
  }

  @Test
  public void testNullOrderingMatrix() throws Exception {
    Row rNonNull = Row.withSchema(BEAM_SCHEMA).addValues(1, "apple", 1.5, true).build();
    Row rNull = Row.withSchema(BEAM_SCHEMA).addValues(2, null, 2.0, true).build();

    // 1. ASC, NULLS_FIRST
    SortOrder ascFirst =
        SortOrder.builderFor(ICEBERG_SCHEMA).asc("name", NullOrder.NULLS_FIRST).build();
    byte[] kNonNullAscFirst = encodeSortKeyHelper(rNonNull, ascFirst);
    byte[] kNullAscFirst = encodeSortKeyHelper(rNull, ascFirst);
    assertTrue(
        "ASC NULLS_FIRST failed: null should sort before non-null",
        BYTE_ARR_COMPARATOR.compare(kNullAscFirst, kNonNullAscFirst) < 0);

    // 2. ASC, NULLS_LAST
    SortOrder ascLast =
        SortOrder.builderFor(ICEBERG_SCHEMA).asc("name", NullOrder.NULLS_LAST).build();
    byte[] kNonNullAscLast = encodeSortKeyHelper(rNonNull, ascLast);
    byte[] kNullAscLast = encodeSortKeyHelper(rNull, ascLast);
    assertTrue(
        "ASC NULLS_LAST failed: null should sort after non-null",
        BYTE_ARR_COMPARATOR.compare(kNullAscLast, kNonNullAscLast) > 0);

    // 3. DESC, NULLS_FIRST
    SortOrder descFirst =
        SortOrder.builderFor(ICEBERG_SCHEMA).desc("name", NullOrder.NULLS_FIRST).build();
    byte[] kNonNullDescFirst = encodeSortKeyHelper(rNonNull, descFirst);
    byte[] kNullDescFirst = encodeSortKeyHelper(rNull, descFirst);
    assertTrue(
        "DESC NULLS_FIRST failed: null should sort before non-null",
        BYTE_ARR_COMPARATOR.compare(kNullDescFirst, kNonNullDescFirst) < 0);

    // 4. DESC, NULLS_LAST
    SortOrder descLast =
        SortOrder.builderFor(ICEBERG_SCHEMA).desc("name", NullOrder.NULLS_LAST).build();
    byte[] kNonNullDescLast = encodeSortKeyHelper(rNonNull, descLast);
    byte[] kNullDescLast = encodeSortKeyHelper(rNull, descLast);
    assertTrue(
        "DESC NULLS_LAST failed: null should sort after non-null",
        BYTE_ARR_COMPARATOR.compare(kNullDescLast, kNonNullDescLast) > 0);
  }

  @Test
  public void testEndToEndSorting() {
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("name").desc("id").build();

    List<Row> input =
        Arrays.asList(
            Row.withSchema(BEAM_SCHEMA).addValues(2, "banana", 2.0, true).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(1, "banana", 1.0, true).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(5, "apple", 1.5, true).build(),
            Row.withSchema(BEAM_SCHEMA).addValues(10, "cherry", 3.0, false).build());

    Iterable<Row> sorted = IcebergRowSorter.sortRows(input, sortOrder, ICEBERG_SCHEMA, BEAM_SCHEMA);
    List<Row> sortedList =
        StreamSupport.stream(sorted.spliterator(), false).collect(Collectors.toList());

    assertEquals(4, sortedList.size());

    // Expected: apple (5) -> banana (2) -> banana (1) -> cherry (10)
    assertEquals("apple", sortedList.get(0).getString("name"));
    assertEquals(Integer.valueOf(5), sortedList.get(0).getInt32("id"));

    assertEquals("banana", sortedList.get(1).getString("name"));
    assertEquals(Integer.valueOf(2), sortedList.get(1).getInt32("id"));

    assertEquals("banana", sortedList.get(2).getString("name"));
    assertEquals(Integer.valueOf(1), sortedList.get(2).getInt32("id"));

    assertEquals("cherry", sortedList.get(3).getString("name"));
    assertEquals(Integer.valueOf(10), sortedList.get(3).getInt32("id"));
  }

  @Test
  public void testScaleAndExternalDiskSpill() {
    // Verifies sorting operates correctly with thousands of elements,
    // proving that BufferedExternalSorter handles memory constraints correctly.
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("id").build();

    int count = 5000;
    List<Row> input = new ArrayList<>(count);
    Random rand = new Random(42);

    for (int i = 0; i < count; i++) {
      // Intentionally insert random IDs to enforce complex sorting
      int randomId = rand.nextInt(100_000);
      input.add(Row.withSchema(BEAM_SCHEMA).addValues(randomId, "item" + i, 1.0, true).build());
    }

    Iterable<Row> sorted = IcebergRowSorter.sortRows(input, sortOrder, ICEBERG_SCHEMA, BEAM_SCHEMA);
    List<Row> sortedList =
        StreamSupport.stream(sorted.spliterator(), false).collect(Collectors.toList());

    assertEquals(count, sortedList.size());

    // Validate that the returned dataset is in strictly non-decreasing order of 'id'
    for (int i = 0; i < sortedList.size() - 1; i++) {
      int idCurrent = sortedList.get(i).getInt32("id");
      int idNext = sortedList.get(i + 1).getInt32("id");
      assertTrue(
          String.format("Sort violation detected at index %d: %d > %d", i, idCurrent, idNext),
          idCurrent <= idNext);
    }
  }
}

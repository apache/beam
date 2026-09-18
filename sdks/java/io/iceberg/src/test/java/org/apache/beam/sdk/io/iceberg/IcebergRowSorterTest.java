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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderParser;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
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
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IcebergRowSorter.encodeSortKey(row, sortOrder, columnNames, baos, ICEBERG_SCHEMA, BEAM_SCHEMA);
    return baos.toByteArray();
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
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("name").asc("value").build();

    Row r1 = Row.withSchema(BEAM_SCHEMA).addValues(1, "abc", 1.0, true).build();
    Row r2 = Row.withSchema(BEAM_SCHEMA).addValues(2, "abcdef", null, true).build();

    byte[] k1 = encodeSortKeyHelper(r1, sortOrder);
    byte[] k2 = encodeSortKeyHelper(r2, sortOrder);

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

    assertTrue(BYTE_ARR_COMPARATOR.compare(k1Asc, k2Asc) < 0);
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
    SortOrder sortOrder = SortOrder.builderFor(ICEBERG_SCHEMA).asc("id").build();

    int count = 5000;
    List<Row> input = new ArrayList<>(count);
    Random rand = new Random(42);

    for (int i = 0; i < count; i++) {
      int randomId = rand.nextInt(100_000);
      input.add(Row.withSchema(BEAM_SCHEMA).addValues(randomId, "item" + i, 1.0, true).build());
    }

    Iterable<Row> sorted = IcebergRowSorter.sortRows(input, sortOrder, ICEBERG_SCHEMA, BEAM_SCHEMA);
    List<Row> sortedList =
        StreamSupport.stream(sorted.spliterator(), false).collect(Collectors.toList());

    assertEquals(count, sortedList.size());

    for (int i = 0; i < sortedList.size() - 1; i++) {
      int idCurrent = sortedList.get(i).getInt32("id");
      int idNext = sortedList.get(i + 1).getInt32("id");
      assertTrue(
          String.format("Sort violation at index %d: %d > %d", i, idCurrent, idNext),
          idCurrent <= idNext);
    }
  }

  @Test
  public void testUnsupportedComplexTypeSorting() {
    org.apache.iceberg.Schema mapSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(
                2,
                "attributes",
                Types.MapType.ofOptional(3, 4, Types.StringType.get(), Types.StringType.get())));

    assertThrows(
        org.apache.iceberg.exceptions.ValidationException.class,
        () -> SortOrder.builderFor(mapSchema).asc("attributes").build());
  }

  /** Encodes a single-column row so each sortable primitive can be exercised in isolation. */
  private static byte[] encodeSingle(
      Schema.FieldType beamType, Type icebergType, Object value, boolean desc) throws Exception {
    Schema beamSchema = Schema.of(Schema.Field.nullable("v", beamType));
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(Types.NestedField.optional(1, "v", icebergType));
    SortOrder sortOrder =
        desc
            ? SortOrder.builderFor(icebergSchema).desc("v").build()
            : SortOrder.builderFor(icebergSchema).asc("v").build();

    Row row = Row.withSchema(beamSchema).addValue(value).build();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IcebergRowSorter.encodeSortKey(
        row, sortOrder, new String[] {"v"}, baos, icebergSchema, beamSchema);
    return baos.toByteArray();
  }

  /** Asserts {@code smaller} sorts before {@code larger} ASC, and after it DESC. */
  private static void assertOrdered(
      Schema.FieldType beamType, Type icebergType, Object smaller, Object larger) throws Exception {
    assertTrue(
        "ASC ordering failed for " + icebergType + ": " + smaller + " should precede " + larger,
        BYTE_ARR_COMPARATOR.compare(
                encodeSingle(beamType, icebergType, smaller, false),
                encodeSingle(beamType, icebergType, larger, false))
            < 0);
    assertTrue(
        "DESC ordering failed for " + icebergType + ": " + smaller + " should follow " + larger,
        BYTE_ARR_COMPARATOR.compare(
                encodeSingle(beamType, icebergType, smaller, true),
                encodeSingle(beamType, icebergType, larger, true))
            > 0);
  }

  @Test
  public void testDecimalKeyOrdering() throws Exception {
    Type decimal = Types.DecimalType.of(12, 2);
    assertOrdered(
        Schema.FieldType.DECIMAL, decimal, new BigDecimal("-9999999.99"), new BigDecimal("-10.50"));
    assertOrdered(
        Schema.FieldType.DECIMAL, decimal, new BigDecimal("-10.50"), new BigDecimal("-0.01"));
    assertOrdered(
        Schema.FieldType.DECIMAL, decimal, new BigDecimal("-0.01"), new BigDecimal("0.00"));
    assertOrdered(
        Schema.FieldType.DECIMAL, decimal, new BigDecimal("0.00"), new BigDecimal("3.14"));
    assertOrdered(
        Schema.FieldType.DECIMAL, decimal, new BigDecimal("3.14"), new BigDecimal("9999999.99"));
  }

  @Test
  public void testDateAndTimeKeyOrdering() throws Exception {
    assertOrdered(
        Schema.FieldType.logicalType(SqlTypes.DATE),
        Types.DateType.get(),
        LocalDate.parse("1969-12-31"),
        LocalDate.parse("2024-02-29"));
    assertOrdered(
        Schema.FieldType.logicalType(SqlTypes.TIME),
        Types.TimeType.get(),
        LocalTime.parse("00:00:01"),
        LocalTime.parse("23:59:59.999999"));
  }

  @Test
  public void testTimestampKeyOrdering() throws Exception {
    // timestamp without zone resolves to a LocalDateTime.
    assertOrdered(
        Schema.FieldType.logicalType(SqlTypes.DATETIME),
        Types.TimestampType.withoutZone(),
        LocalDateTime.parse("1969-07-20T20:17:40"),
        LocalDateTime.parse("2031-06-05T12:00:00"));
    // timestamptz resolves to an OffsetDateTime.
    assertOrdered(
        Schema.FieldType.DATETIME,
        Types.TimestampType.withZone(),
        org.joda.time.Instant.parse("1969-07-20T20:17:40Z"),
        org.joda.time.Instant.parse("2031-06-05T12:00:00Z"));
  }

  @Test
  public void testNarrowIntegerKeyOrdering() throws Exception {
    assertOrdered(Schema.FieldType.INT16, Types.IntegerType.get(), (short) -300, (short) 300);
    assertOrdered(Schema.FieldType.BYTE, Types.IntegerType.get(), (byte) -5, (byte) 5);
  }

  @Test
  public void testUuidKeyEncodingIsDeterministic() throws Exception {
    byte[] first =
        encodeSingle(Schema.FieldType.BYTES, Types.UUIDType.get(), new byte[] {1, 2}, false);
    byte[] repeat =
        encodeSingle(Schema.FieldType.BYTES, Types.UUIDType.get(), new byte[] {1, 2}, false);
    byte[] other =
        encodeSingle(Schema.FieldType.BYTES, Types.UUIDType.get(), new byte[] {3, 4}, false);

    assertEquals(0, BYTE_ARR_COMPARATOR.compare(first, repeat));
    assertTrue(BYTE_ARR_COMPARATOR.compare(first, other) != 0);
  }

  @Test
  public void testDecimalRejectsValueExceedingDeclaredScale() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            encodeSingle(
                Schema.FieldType.DECIMAL,
                Types.DecimalType.of(12, 2),
                new BigDecimal("1.234"),
                false));
  }

  @Test
  public void testNonIdentityTransformSortOrder() throws Exception {
    Schema beamSchema =
        Schema.builder()
            .addInt32Field("id")
            .addLogicalTypeField("d", SqlTypes.DATE)
            .addLogicalTypeField("ts", SqlTypes.DATETIME)
            .addDecimalField("dec")
            .addStringField("s")
            .build();
    org.apache.iceberg.Schema icebergSchema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "d", Types.DateType.get()),
            Types.NestedField.required(3, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(4, "dec", Types.DecimalType.of(12, 2)),
            Types.NestedField.required(5, "s", Types.StringType.get()));

    // Sort by month(d) ASC, hour(ts) ASC, truncate(dec, 100) ASC, truncate(s, 3) ASC, id ASC.
    // Exercise both a directly built SortOrder and a JSON-deserialized SortOrder (which produces
    // unbound transforms until .bind(sourceType) is invoked).
    SortOrder builtOrder =
        SortOrder.builderFor(icebergSchema)
            .asc(Expressions.month("d"))
            .asc(Expressions.hour("ts"))
            .asc(Expressions.truncate("dec", 100))
            .asc(Expressions.truncate("s", 3))
            .asc("id")
            .build();
    SortOrder jsonOrder =
        SortOrderParser.fromJson(icebergSchema, SortOrderParser.toJson(builtOrder));

    // r1 and r2 share the same month (2024-03), same hour (10:xx), same truncated decimal
    // (1.99 and 1.05 both truncate to 1.00 with width 100 unscaled units), and same 3-char prefix
    // ("abc..."), so they must tie on all 4 transform keys and order by id ASC (r2 before r1).
    // r3 is in month 2024-01, so it must sort before both r1 and r2.
    Row r1 =
        Row.withSchema(beamSchema)
            .addValues(
                20,
                LocalDate.parse("2024-03-25"),
                LocalDateTime.parse("2024-03-25T10:55:00"),
                new BigDecimal("1.99"),
                "abc_zzz")
            .build();
    Row r2 =
        Row.withSchema(beamSchema)
            .addValues(
                10,
                LocalDate.parse("2024-03-02"),
                LocalDateTime.parse("2024-03-02T10:05:00"),
                new BigDecimal("1.05"),
                "abc_aaa")
            .build();
    Row r3 =
        Row.withSchema(beamSchema)
            .addValues(
                30,
                LocalDate.parse("2024-01-31"),
                LocalDateTime.parse("2024-01-31T23:00:00"),
                new BigDecimal("99.99"),
                "zzz")
            .build();

    for (SortOrder order : Arrays.asList(builtOrder, jsonOrder)) {
      try (CloseableIterable<Row> sorted =
          IcebergRowSorter.sortRows(Arrays.asList(r1, r2, r3), order, icebergSchema, beamSchema)) {
        List<Row> sortedList =
            StreamSupport.stream(sorted.spliterator(), false).collect(Collectors.toList());
        assertEquals(
            Arrays.asList(30, 10, 20),
            sortedList.stream().map(r -> r.getInt32("id")).collect(Collectors.toList()));
      }
    }
  }
}

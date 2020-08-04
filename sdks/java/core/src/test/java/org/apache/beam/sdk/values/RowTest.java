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
package org.apache.beam.sdk.values;

import static org.apache.beam.sdk.schemas.Schema.toSchema;
import static org.apache.beam.sdk.values.Row.toRow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link Row}. */
public class RowTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreatesNullRecord() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_byte", FieldType.BYTE).withNullable(true),
                Schema.Field.of("f_int16", FieldType.INT16).withNullable(true),
                Schema.Field.of("f_int32", FieldType.INT32).withNullable(true),
                Schema.Field.of("f_int64", FieldType.INT64).withNullable(true),
                Schema.Field.of("f_decimal", FieldType.DECIMAL).withNullable(true),
                Schema.Field.of("f_float", FieldType.FLOAT).withNullable(true),
                Schema.Field.of("f_double", FieldType.DOUBLE).withNullable(true),
                Schema.Field.of("f_string", FieldType.STRING).withNullable(true),
                Schema.Field.of("f_datetime", FieldType.DATETIME).withNullable(true),
                Schema.Field.of("f_boolean", FieldType.BOOLEAN).withNullable(true),
                Schema.Field.of("f_array", FieldType.array(FieldType.DATETIME)).withNullable(true),
                Schema.Field.of("f_iter", FieldType.iterable(FieldType.DATETIME))
                    .withNullable(true),
                Schema.Field.of("f_map", FieldType.map(FieldType.INT32, FieldType.DOUBLE))
                    .withNullable(true))
            .collect(toSchema());

    Row row = Row.nullRow(type);

    assertNull(row.getByte("f_byte"));
    assertNull(row.getByte(0));
    assertNull(row.getInt16("f_int16"));
    assertNull(row.getInt16(1));
    assertNull(row.getInt32("f_int32"));
    assertNull(row.getInt32(2));
    assertNull(row.getInt64("f_int64"));
    assertNull(row.getInt64(3));
    assertNull(row.getDecimal("f_decimal"));
    assertNull(row.getDecimal(4));
    assertNull(row.getFloat("f_float"));
    assertNull(row.getFloat(5));
    assertNull(row.getDouble("f_double"));
    assertNull(row.getDouble(6));
    assertNull(row.getString("f_string"));
    assertNull(row.getString(7));
    assertNull(row.getDateTime("f_datetime"));
    assertNull(row.getDateTime(8));
    assertNull(row.getBoolean("f_boolean"));
    assertNull(row.getBoolean(9));
    assertNull(row.getBoolean("f_array"));
    assertNull(row.getBoolean(10));
    assertNull(row.getBoolean("f_iter"));
    assertNull(row.getBoolean(11));
    assertNull(row.getBoolean("f_map"));
    assertNull(row.getBoolean(12));
  }

  @Test
  public void testRejectsNullRecord() {
    Schema type = Stream.of(Schema.Field.of("f_int", Schema.FieldType.INT32)).collect(toSchema());
    thrown.expect(IllegalArgumentException.class);
    Row.nullRow(type);
  }

  @Test
  public void testCreatesRecord() {
    Schema schema =
        Schema.builder()
            .addByteField("f_byte")
            .addInt16Field("f_int16")
            .addInt32Field("f_int32")
            .addInt64Field("f_int64")
            .addDecimalField("f_decimal")
            .addFloatField("f_float")
            .addDoubleField("f_double")
            .addStringField("f_string")
            .addDateTimeField("f_datetime")
            .addBooleanField("f_boolean")
            .build();

    DateTime dateTime =
        new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
    Row row =
        Row.withSchema(schema)
            .addValues(
                (byte) 0, (short) 1, 2, 3L, new BigDecimal(2.3), 1.2f, 3.0d, "str", dateTime, false)
            .build();

    assertEquals((byte) 0, (Object) row.getByte("f_byte"));
    assertEquals((byte) 0, (Object) row.getByte(0));
    assertEquals((short) 1, (Object) row.getInt16("f_int16"));
    assertEquals((short) 1, (Object) row.getInt16(1));
    assertEquals((int) 2, (Object) row.getInt32("f_int32"));
    assertEquals((int) 2, (Object) row.getInt32(2));
    assertEquals((long) 3, (Object) row.getInt64("f_int64"));
    assertEquals((long) 3, (Object) row.getInt64(3));
    assertEquals(new BigDecimal(2.3), row.getDecimal("f_decimal"));
    assertEquals(new BigDecimal(2.3), row.getDecimal(4));
    assertEquals(1.2f, row.getFloat("f_float"), 0);
    assertEquals(1.2f, row.getFloat(5), 0);
    assertEquals(3.0d, row.getDouble("f_double"), 0);
    assertEquals(3.0d, row.getDouble(6), 0);
    assertEquals("str", row.getString("f_string"));
    assertEquals("str", row.getString(7));
    assertEquals(dateTime, row.getDateTime("f_datetime"));
    assertEquals(dateTime, row.getDateTime(8));
    assertFalse(row.getBoolean("f_boolean"));
    assertFalse(row.getBoolean(9));
  }

  @Test
  public void testCreatesNestedRow() {
    Schema nestedType =
        Stream.of(Schema.Field.of("f1_str", Schema.FieldType.STRING)).collect(toSchema());

    Schema type =
        Stream.of(
                Schema.Field.of("f_int", Schema.FieldType.INT32),
                Schema.Field.of("nested", Schema.FieldType.row(nestedType)))
            .collect(toSchema());
    Row nestedRow = Row.withSchema(nestedType).addValues("foobar").build();
    Row row = Row.withSchema(type).addValues(42, nestedRow).build();
    assertEquals((int) 42, (Object) row.getInt32("f_int"));
    assertEquals("foobar", row.getRow("nested").getString("f1_str"));
  }

  @Test
  public void testCreatesArray() {
    List<Integer> data = Lists.newArrayList(2, 3, 5, 7);
    Schema type =
        Stream.of(Schema.Field.of("array", Schema.FieldType.array(Schema.FieldType.INT32)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesIterable() {
    List<Integer> data = Lists.newArrayList(2, 3, 5, 7);
    Schema type =
        Stream.of(Schema.Field.of("iter", Schema.FieldType.iterable(Schema.FieldType.INT32)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreatesAndComparesNullArray() {
    List<Integer> data = null;
    Schema type =
        Stream.of(Schema.Field.nullable("array", Schema.FieldType.array(Schema.FieldType.INT32)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));

    Row otherNonNull = Row.withSchema(type).addValue(ImmutableList.of(1, 2, 3)).build();
    Row otherNull = Row.withSchema(type).addValue(null).build();
    assertNotEquals(otherNonNull, row);
    assertEquals(otherNull, row);
  }

  @Test
  public void testCreatesAndComparesNullIterable() {
    List<Integer> data = null;
    Schema type =
        Stream.of(Schema.Field.nullable("iter", Schema.FieldType.iterable(Schema.FieldType.INT32)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getArray("iter"));

    Row otherNonNull = Row.withSchema(type).addValue(ImmutableList.of(1, 2, 3)).build();
    Row otherNull = Row.withSchema(type).addValue(null).build();
    assertNotEquals(otherNonNull, row);
    assertEquals(otherNull, row);
  }

  @Test
  public void testCreatesArrayWithNullElement() {
    List<Integer> data = Lists.newArrayList(2, null, 5, null);
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "array", Schema.FieldType.array(Schema.FieldType.INT32.withNullable(true))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesIterableWithNullElement() {
    List<Integer> data = Lists.newArrayList(2, null, 5, null);
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "iter", Schema.FieldType.iterable(Schema.FieldType.INT32.withNullable(true))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreatesRowArray() {
    Schema nestedType = Stream.of(Schema.Field.of("f1_str", FieldType.STRING)).collect(toSchema());
    List<Row> data =
        Lists.newArrayList(
            Row.withSchema(nestedType).addValues("one").build(),
            Row.withSchema(nestedType).addValues("two").build(),
            Row.withSchema(nestedType).addValues("three").build());

    Schema type =
        Stream.of(Schema.Field.of("array", FieldType.array(FieldType.row(nestedType))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesRowIterable() {
    Schema nestedType = Stream.of(Schema.Field.of("f1_str", FieldType.STRING)).collect(toSchema());
    List<Row> data =
        Lists.newArrayList(
            Row.withSchema(nestedType).addValues("one").build(),
            Row.withSchema(nestedType).addValues("two").build(),
            Row.withSchema(nestedType).addValues("three").build());

    Schema type =
        Stream.of(Schema.Field.of("iter", FieldType.iterable(FieldType.row(nestedType))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreatesArrayArray() {
    List<List<Integer>> data = Lists.<List<Integer>>newArrayList(Lists.newArrayList(1, 2, 3, 4));
    Schema type =
        Stream.of(Schema.Field.of("array", FieldType.array(FieldType.array(FieldType.INT32))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesIterableArray() {
    List<List<Integer>> data = Lists.<List<Integer>>newArrayList(Lists.newArrayList(1, 2, 3, 4));
    Schema type =
        Stream.of(Schema.Field.of("iter", FieldType.iterable(FieldType.array(FieldType.INT32))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreatesArrayArrayWithNullElement() {
    List<List<Integer>> data =
        Lists.<List<Integer>>newArrayList(Lists.newArrayList(1, null, 3, null), null);
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "array",
                    FieldType.array(
                            FieldType.array(FieldType.INT32.withNullable(true)).withNullable(true))
                        .withNullable(true)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesIterableIterableWithNullElement() {
    List<List<Integer>> data =
        Lists.<List<Integer>>newArrayList(Lists.newArrayList(1, null, 3, null), null);
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "iter",
                    FieldType.iterable(
                            FieldType.iterable(FieldType.INT32.withNullable(true))
                                .withNullable(true))
                        .withNullable(true)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreatesArrayOfMap() {
    List<Map<Integer, String>> data =
        ImmutableList.<Map<Integer, String>>builder()
            .add(ImmutableMap.of(1, "value1"))
            .add(ImmutableMap.of(2, "value2"))
            .build();
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "array", FieldType.array(FieldType.map(FieldType.INT32, FieldType.STRING))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesIterableOfMap() {
    List<Map<Integer, String>> data =
        ImmutableList.<Map<Integer, String>>builder()
            .add(ImmutableMap.of(1, "value1"))
            .add(ImmutableMap.of(2, "value2"))
            .build();
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "iter", FieldType.iterable(FieldType.map(FieldType.INT32, FieldType.STRING))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addIterable(data).build();
    assertEquals(data, row.getIterable("iter"));
  }

  @Test
  public void testCreateMapWithPrimitiveValue() {
    Map<Integer, String> data =
        ImmutableMap.<Integer, String>builder()
            .put(1, "value1")
            .put(2, "value2")
            .put(3, "value3")
            .put(4, "value4")
            .build();
    Schema type =
        Stream.of(Schema.Field.of("map", FieldType.map(FieldType.INT32, FieldType.STRING)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testCreateAndCompareNullMap() {
    List<Integer> data = null;
    Schema type =
        Stream.of(Schema.Field.nullable("map", FieldType.map(FieldType.INT32, FieldType.STRING)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getArray("map"));

    Row otherNonNull = Row.withSchema(type).addValue(ImmutableMap.of(1, "value1")).build();
    Row otherNull = Row.withSchema(type).addValue(null).build();
    assertNotEquals(otherNonNull, row);
    assertEquals(otherNull, row);
  }

  @Test
  public void testCreateMapWithNullValue() {
    Map<Integer, String> data = new HashMap();
    data.put(1, "value1");
    data.put(2, "value2");
    data.put(3, null);
    data.put(4, null);
    Schema type =
        Stream.of(Schema.Field.of("map", FieldType.map(FieldType.INT32, FieldType.STRING, true)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testCreateMapWithArrayValue() {
    Map<Integer, List<String>> data =
        ImmutableMap.<Integer, List<String>>builder()
            .put(1, Arrays.asList("value1"))
            .put(2, Arrays.asList("value2"))
            .build();
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "map", FieldType.map(FieldType.INT32, FieldType.array(FieldType.STRING))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testCreateMapWithMapValue() {
    Map<Integer, Map<Integer, String>> data =
        ImmutableMap.<Integer, Map<Integer, String>>builder()
            .put(1, ImmutableMap.of(1, "value1"))
            .put(2, ImmutableMap.of(2, "value2"))
            .build();
    Schema type =
        Stream.of(
                Schema.Field.of(
                    "map",
                    FieldType.map(
                        FieldType.INT32, FieldType.map(FieldType.INT32, FieldType.STRING))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testCreateMapWithMapValueWithNull() {
    Map<Integer, Map<Integer, String>> data = new HashMap();
    Map<Integer, String> innerData = new HashMap();
    innerData.put(11, null);
    innerData.put(12, "value3");
    data.put(1, ImmutableMap.of(1, "value1"));
    data.put(2, ImmutableMap.of(2, "value2"));
    data.put(3, null);
    data.put(4, innerData);

    Schema type =
        Stream.of(
                Schema.Field.of(
                    "map",
                    FieldType.map(
                        FieldType.INT32,
                        FieldType.map(FieldType.INT32, FieldType.STRING, true),
                        true)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testCreateMapWithRowValue() {
    Schema nestedType = Stream.of(Schema.Field.of("f1_str", FieldType.STRING)).collect(toSchema());
    Map<Integer, Row> data =
        ImmutableMap.<Integer, Row>builder()
            .put(1, Row.withSchema(nestedType).addValues("one").build())
            .put(2, Row.withSchema(nestedType).addValues("two").build())
            .build();
    Schema type =
        Stream.of(Schema.Field.of("map", FieldType.map(FieldType.INT32, FieldType.row(nestedType))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(data).build();
    assertEquals(data, row.getMap("map"));
  }

  @Test
  public void testLogicalTypeWithRowValue() {
    EnumerationType enumerationType = EnumerationType.create("zero", "one", "two");
    Schema type =
        Stream.of(Schema.Field.of("f1_enum", FieldType.logicalType(enumerationType)))
            .collect(toSchema());
    Row row = Row.withSchema(type).addValue(enumerationType.valueOf("zero")).build();
    assertEquals(enumerationType.valueOf(0), row.getValue(0));
    assertEquals(
        enumerationType.valueOf("zero"), row.getLogicalTypeValue(0, EnumerationType.Value.class));
  }

  @Test
  public void testLogicalTypeWithRowValueName() {
    EnumerationType enumerationType = EnumerationType.create("zero", "one", "two");
    Schema type =
        Stream.of(Schema.Field.of("f1_enum", FieldType.logicalType(enumerationType)))
            .collect(toSchema());
    Row row =
        Row.withSchema(type).withFieldValue("f1_enum", enumerationType.valueOf("zero")).build();
    assertEquals(enumerationType.valueOf(0), row.getValue(0));
    assertEquals(
        enumerationType.valueOf("zero"), row.getLogicalTypeValue(0, EnumerationType.Value.class));
  }

  @Test
  public void testLogicalTypeWithRowValueOverride() {
    EnumerationType enumerationType = EnumerationType.create("zero", "one", "two");
    Schema type =
        Stream.of(Schema.Field.of("f1_enum", FieldType.logicalType(enumerationType)))
            .collect(toSchema());
    Row row =
        Row.withSchema(type).withFieldValue("f1_enum", enumerationType.valueOf("zero")).build();
    Row overriddenRow =
        Row.fromRow(row).withFieldValue("f1_enum", enumerationType.valueOf("one")).build();
    assertEquals(enumerationType.valueOf(1), overriddenRow.getValue(0));
    assertEquals(
        enumerationType.valueOf("one"),
        overriddenRow.getLogicalTypeValue(0, EnumerationType.Value.class));
  }

  @Test
  public void testCreateWithNames() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_byte", FieldType.BYTE),
                Schema.Field.of("f_short", FieldType.INT16),
                Schema.Field.of("f_int", FieldType.INT32),
                Schema.Field.of("f_long", FieldType.INT64),
                Schema.Field.of("f_float", FieldType.FLOAT),
                Schema.Field.of("f_double", FieldType.DOUBLE),
                Schema.Field.of("f_decimal", FieldType.DECIMAL),
                Schema.Field.of("f_boolean", FieldType.BOOLEAN),
                Schema.Field.of("f_datetime", FieldType.DATETIME),
                Schema.Field.of("f_bytes", FieldType.BYTES),
                Schema.Field.of("f_array", FieldType.array(FieldType.STRING)),
                Schema.Field.of("f_iterable", FieldType.iterable(FieldType.STRING)),
                Schema.Field.of("f_map", FieldType.map(FieldType.STRING, FieldType.STRING)))
            .collect(toSchema());

    DateTime dateTime =
        new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
    byte[] bytes = new byte[] {1, 2, 3, 4};

    Row row =
        Row.withSchema(type)
            .withFieldValue("f_str", "str1")
            .withFieldValue("f_byte", (byte) 42)
            .withFieldValue("f_short", (short) 43)
            .withFieldValue("f_int", (int) 44)
            .withFieldValue("f_long", (long) 45)
            .withFieldValue("f_float", (float) 3.14)
            .withFieldValue("f_double", (double) 3.141)
            .withFieldValue("f_decimal", new BigDecimal(3.1415))
            .withFieldValue("f_boolean", true)
            .withFieldValue("f_datetime", dateTime)
            .withFieldValue("f_bytes", bytes)
            .withFieldValue("f_array", Lists.newArrayList("one", "two"))
            .withFieldValue("f_iterable", Lists.newArrayList("one", "two", "three"))
            .withFieldValue("f_map", ImmutableMap.of("hello", "goodbye", "here", "there"))
            .build();

    Row expectedRow =
        Row.withSchema(type)
            .addValues(
                "str1",
                (byte) 42,
                (short) 43,
                (int) 44,
                (long) 45,
                (float) 3.14,
                (double) 3.141,
                new BigDecimal(3.1415),
                true,
                dateTime,
                bytes,
                Lists.newArrayList("one", "two"),
                Lists.newArrayList("one", "two", "three"),
                ImmutableMap.of("hello", "goodbye", "here", "there"))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testCreateWithNestedNames() {
    Schema nestedType =
        Stream.of(
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_int", FieldType.INT32))
            .collect(toSchema());
    Schema topType =
        Stream.of(
                Schema.Field.of("top_int", FieldType.INT32),
                Schema.Field.of("f_nested", FieldType.row(nestedType)))
            .collect(toSchema());
    Row row =
        Row.withSchema(topType)
            .withFieldValue("top_int", 42)
            .withFieldValue("f_nested.f_str", "string")
            .withFieldValue("f_nested.f_int", 43)
            .build();

    Row expectedRow =
        Row.withSchema(topType)
            .addValues(42, Row.withSchema(nestedType).addValues("string", 43).build())
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testCreateWithCollectionNames() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_array", FieldType.array(FieldType.INT32)),
                Schema.Field.of("f_iterable", FieldType.iterable(FieldType.INT32)),
                Schema.Field.of("f_map", FieldType.map(FieldType.STRING, FieldType.STRING)))
            .collect(toSchema());

    Row row =
        Row.withSchema(type)
            .withFieldValue("f_array", ImmutableList.of(1, 2, 3))
            .withFieldValue("f_iterable", ImmutableList.of(1, 2, 3))
            .withFieldValue("f_map", ImmutableMap.of("one", "two"))
            .build();

    Row expectedRow =
        Row.withSchema(type)
            .addValues(
                ImmutableList.of(1, 2, 3), ImmutableList.of(1, 2, 3), ImmutableMap.of("one", "two"))
            .build();
    assertEquals(expectedRow, row);
  }

  @Test
  public void testOverrideRow() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_int", FieldType.INT32))
            .collect(toSchema());
    Row sourceRow =
        Row.withSchema(type).withFieldValue("f_str", "string").withFieldValue("f_int", 42).build();

    Row modifiedRow = Row.fromRow(sourceRow).withFieldValue("f_str", "modifiedString").build();

    Row expectedRow =
        Row.withSchema(type)
            .withFieldValue("f_str", "modifiedString")
            .withFieldValue("f_int", 42)
            .build();
    assertEquals(expectedRow, modifiedRow);
  }

  @Test
  public void testOverrideNestedRow() {
    Schema nestedType =
        Stream.of(
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_int", FieldType.INT32))
            .collect(toSchema());
    Schema topType =
        Stream.of(
                Schema.Field.of("top_int", FieldType.INT32),
                Schema.Field.of("f_nested", FieldType.row(nestedType)))
            .collect(toSchema());
    Row sourceRow =
        Row.withSchema(topType)
            .withFieldValue("top_int", 42)
            .withFieldValue("f_nested.f_str", "string")
            .withFieldValue("f_nested.f_int", 43)
            .build();
    Row modifiedRow =
        Row.fromRow(sourceRow)
            .withFieldValue("f_nested.f_str", "modifiedString")
            .withFieldValue("f_nested.f_int", 143)
            .build();

    Row expectedRow =
        Row.withSchema(topType)
            .withFieldValue("top_int", 42)
            .withFieldValue("f_nested.f_str", "modifiedString")
            .withFieldValue("f_nested.f_int", 143)
            .build();
    assertEquals(expectedRow, modifiedRow);
  }

  @Test
  public void testCollector() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_int", FieldType.INT32),
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_double", FieldType.DOUBLE))
            .collect(toSchema());

    Row row = Stream.of(1, "2", 3.0d).collect(toRow(type));

    assertEquals(1, row.<Object>getValue("f_int"));
    assertEquals("2", row.getValue("f_str"));
    assertEquals(3.0d, row.<Object>getValue("f_double"));
  }

  @Test
  public void testThrowsForIncorrectNumberOfFields() {
    Schema type =
        Stream.of(
                Schema.Field.of("f_int", FieldType.INT32),
                Schema.Field.of("f_str", FieldType.STRING),
                Schema.Field.of("f_double", FieldType.DOUBLE))
            .collect(toSchema());

    thrown.expect(IllegalArgumentException.class);
    Row.withSchema(type).addValues(1, "2").build();
  }

  @Test
  public void testByteArrayEquality() {
    byte[] a0 = new byte[] {1, 2, 3, 4};
    byte[] b0 = new byte[] {1, 2, 3, 4};

    Schema schema = Schema.of(Schema.Field.of("bytes", Schema.FieldType.BYTES));

    Row a = Row.withSchema(schema).addValue(a0).build();
    Row b = Row.withSchema(schema).addValue(b0).build();

    assertEquals(a, b);
  }

  @Test
  public void testByteBufferEquality() {
    byte[] a0 = new byte[] {1, 2, 3, 4};
    byte[] b0 = new byte[] {1, 2, 3, 4};

    Schema schema = Schema.of(Schema.Field.of("bytes", Schema.FieldType.BYTES));

    Row a = Row.withSchema(schema).addValue(ByteBuffer.wrap(a0)).build();
    Row b = Row.withSchema(schema).addValue(ByteBuffer.wrap(b0)).build();

    assertEquals(a, b);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLogicalTypeWithInvalidInputValueByFieldName() {
    Schema schema = Schema.builder().addLogicalTypeField("char", FixedBytes.of(10)).build();
    byte[] byteArrayWithLengthFive = {1, 2, 3, 4, 5};
    Row row = Row.withSchema(schema).withFieldValue("char", byteArrayWithLengthFive).build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLogicalTypeWithInvalidInputValueByFieldIndex() {
    Schema schema = Schema.builder().addLogicalTypeField("char", FixedBytes.of(10)).build();
    byte[] byteArrayWithLengthFive = {1, 2, 3, 4, 5};
    Row row = Row.withSchema(schema).addValues(byteArrayWithLengthFive).build();
  }

  @Test
  public void testFixedBytes() {
    Schema schema = Schema.builder().addLogicalTypeField("char", FixedBytes.of(10)).build();
    byte[] byteArray = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    Row row = Row.withSchema(schema).withFieldValue("char", byteArray).build();
    assertTrue(Arrays.equals(byteArray, row.getLogicalTypeValue("char", byte[].class)));
  }

  @Test
  public void testWithFieldValues() {
    EnumerationType enumerationType = EnumerationType.create("zero", "one", "two");
    Schema schema = Schema.builder().addLogicalTypeField("f1_enum", enumerationType).build();
    Row row =
        Row.withSchema(schema)
            .withFieldValues(ImmutableMap.of("f1_enum", enumerationType.valueOf("zero")))
            .build();
    assertEquals(enumerationType.valueOf(0), row.getValue(0));
    assertEquals(
        enumerationType.valueOf("zero"), row.getLogicalTypeValue(0, EnumerationType.Value.class));
  }
}

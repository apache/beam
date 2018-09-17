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
import static org.junit.Assert.assertNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
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
                Schema.Field.of("f_boolean", FieldType.BOOLEAN).withNullable(true))
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
    assertEquals(false, row.getBoolean("f_boolean"));
    assertEquals(false, row.getBoolean(9));
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
  public void testCreatesArrayArray() {
    List<List<Integer>> data = Lists.<List<Integer>>newArrayList(Lists.newArrayList(1, 2, 3, 4));
    Schema type =
        Stream.of(Schema.Field.of("array", FieldType.array(FieldType.array(FieldType.INT32))))
            .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
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
}

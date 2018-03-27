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

import com.google.common.collect.Lists;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.FieldTypeDescriptor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Unit tests for {@link Row}.
 */
public class RowTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testCreatesNullRecord() {
    Schema type =
        Stream
            .of(
                Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32))
                    .withNullable(true),
                Schema.Field.of("f_str", FieldTypeDescriptor.of(FieldType.STRING))
                    .withNullable(true),
                Schema.Field.of("f_double", FieldTypeDescriptor.of(FieldType.DOUBLE))
                    .withNullable(true))
            .collect(toSchema());

    Row row = Row.nullRow(type);

    assertNull(row.getValue("f_int"));
    assertNull(row.getValue("f_str"));
    assertNull(row.getValue("f_double"));
  }

  @Test
  public void testRejectsNullRecord() {
    Schema type = Stream.of(Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32)))
        .collect(toSchema());
    thrown.expect(IllegalArgumentException.class);
    Row.nullRow(type);
  }

  @Test
  public void testCreatesRecord() {
    Schema type = Schema.of(
        Field.of("f_byte", FieldTypeDescriptor.of(FieldType.BYTE)),
        Field.of("f_int16", FieldTypeDescriptor.of(FieldType.INT16)),
        Field.of("f_int32", FieldTypeDescriptor.of(FieldType.INT32)),
        Field.of("f_int64", FieldTypeDescriptor.of(FieldType.INT64)),
        Field.of("f_decimal", FieldTypeDescriptor.of(FieldType.DECIMAL)),
        Field.of("f_float", FieldTypeDescriptor.of(FieldType.FLOAT)),
        Field.of("f_double", FieldTypeDescriptor.of(FieldType.DOUBLE)),
        Field.of("f_string", FieldTypeDescriptor.of(FieldType.STRING)),
        Field.of("f_datetime", FieldTypeDescriptor.of(FieldType.DATETIME)),
        Field.of("f_boolean", FieldTypeDescriptor.of(FieldType.BOOLEAN)));

    DateTime dateTime = new DateTime().withDate(1979, 03, 14)
        .withTime(1, 2, 3, 4)
        .withZone(DateTimeZone.UTC);
    Row row =
        Row
            .withSchema(type)
            .addValues((byte) 0, (short) 1, 2, 3L, new BigDecimal(2.3), 1.2f, 3.0d, "str",
                dateTime, false)
            .build();

    assertEquals(0, row.getByte("f_byte"));
    assertEquals(1, row.getInt16("f_int16"));
    assertEquals(2, row.getInt32("f_int32"));
    assertEquals(3, row.getInt64("f_int64"));
    assertEquals(new BigDecimal(2.3), row.getDecimal("f_decimal"));
    assertEquals(1.2f, row.getFloat("f_float"), 0);
    assertEquals(3.0d, row.getDouble("f_double"), 0);
    assertEquals("str", row.getString("f_string"));
    assertEquals(dateTime, row.getDateTime("f_datetime"));
    assertEquals(false, row.getBoolean("f_boolean"));
    assertEquals("str", row.getString("f_string"));
    assertEquals(false, row.getBoolean("f_boolean"));
  }

  @Test
  public void testCreatesNestedRow() {
    Schema nestedType = Stream.of(
        Schema.Field.of("f1_str", FieldTypeDescriptor.of(FieldType.STRING)))
        .collect(toSchema());

    Schema type =
        Stream
            .of(Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32)),
                Schema.Field.of("nested",
                    FieldTypeDescriptor.of(FieldType.ROW)
                    .withRowSchema(nestedType)))
        .collect(toSchema());
    Row nestedRow = Row.withSchema(nestedType).addValues("foobar").build();
    Row row = Row.withSchema(type).addValues(42, nestedRow).build();
    assertEquals(42, row.getInt32("f_int"));
    assertEquals("foobar", row.getRow("nested").getString("f1_str"));
  }

  @Test
  public void testCreatesArray() {
    List<Integer> data = Lists.newArrayList(2, 3, 5, 7);
    Schema type = Stream
        .of(Schema.Field.of("array",
            FieldTypeDescriptor.of(FieldType.ARRAY)
                .withComponentType(FieldTypeDescriptor.of(FieldType.INT32))))
        .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesRowArray() {
    Schema nestedType = Stream.of(
        Schema.Field.of("f1_str", FieldTypeDescriptor.of(FieldType.STRING)))
        .collect(toSchema());
    List<Row> data = Lists.newArrayList(
        Row.withSchema(nestedType).addValues("one").build(),
        Row.withSchema(nestedType).addValues("two").build(),
        Row.withSchema(nestedType).addValues("three").build());

    Schema type = Stream
        .of(Schema.Field.of("array",
            FieldTypeDescriptor.of(FieldType.ARRAY)
                .withComponentType(FieldTypeDescriptor.of(FieldType.ROW)
                    .withRowSchema(nestedType))))
        .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCreatesArrayArray() {
    List<List<Integer>> data = Lists.<List<Integer>>newArrayList(
        Lists.newArrayList(1, 2, 3, 4));
    Schema type = Stream
        .of(Schema.Field.of("array",
            FieldTypeDescriptor.of(FieldType.ARRAY)
                .withComponentType(FieldTypeDescriptor.of(FieldType.ARRAY)
                    .withComponentType(FieldTypeDescriptor.of(FieldType.INT32)))))
        .collect(toSchema());
    Row row = Row.withSchema(type).addArray(data).build();
    assertEquals(data, row.getArray("array"));
  }

  @Test
  public void testCollector() {
    Schema type =
        Stream
            .of(
                Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32)),
                Schema.Field.of("f_str", FieldTypeDescriptor.of(FieldType.STRING)),
                Schema.Field.of("f_double", FieldTypeDescriptor.of(FieldType.DOUBLE)))
            .collect(toSchema());

    Row row =
        Stream
            .of(1, "2", 3.0d)
            .collect(toRow(type));

    assertEquals(1, row.<Object>getValue("f_int"));
    assertEquals("2", row.getValue("f_str"));
    assertEquals(3.0d, row.<Object>getValue("f_double"));
  }

  @Test
  public void testThrowsForIncorrectNumberOfFields() {
    Schema type =
        Stream
            .of(
                Schema.Field.of("f_int", FieldTypeDescriptor.of(FieldType.INT32)),
                Schema.Field.of("f_str", FieldTypeDescriptor.of(FieldType.STRING)),
                Schema.Field.of("f_double", FieldTypeDescriptor.of(FieldType.DOUBLE)))
            .collect(toSchema());

    thrown.expect(IllegalArgumentException.class);
    Row.withSchema(type).addValues(1, "2").build();
  }
}

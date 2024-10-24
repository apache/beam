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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Tests for {@link TableSchema}. */
public class TableSchemaTest {

  @Test
  public void testParseDate() {
    assertEquals(ColumnType.DATE, ColumnType.parse("Date"));
  }

  @Test
  public void testParseDateTime() {
    assertEquals(ColumnType.DATETIME, ColumnType.parse("DateTime"));
  }

  @Test
  public void testParseFloat32() {
    assertEquals(ColumnType.FLOAT32, ColumnType.parse("Float32"));
  }

  @Test
  public void testParseFloat64() {
    assertEquals(ColumnType.FLOAT64, ColumnType.parse("Float64"));
  }

  @Test
  public void testParseInt8() {
    assertEquals(ColumnType.INT8, ColumnType.parse("Int8"));
  }

  @Test
  public void testParseInt16() {
    assertEquals(ColumnType.INT16, ColumnType.parse("Int16"));
  }

  @Test
  public void testParseInt32() {
    assertEquals(ColumnType.INT32, ColumnType.parse("Int32"));
  }

  @Test
  public void testParseInt64() {
    assertEquals(ColumnType.INT64, ColumnType.parse("Int64"));
  }

  @Test
  public void testParseUInt8() {
    assertEquals(ColumnType.UINT8, ColumnType.parse("UInt8"));
  }

  @Test
  public void testParseUInt16() {
    assertEquals(ColumnType.UINT16, ColumnType.parse("UInt16"));
  }

  @Test
  public void testParseUInt32() {
    assertEquals(ColumnType.UINT32, ColumnType.parse("UInt32"));
  }

  @Test
  public void testParseUInt64() {
    assertEquals(ColumnType.UINT64, ColumnType.parse("UInt64"));
  }

  @Test
  public void testParseBool() {
    assertEquals(ColumnType.BOOL, ColumnType.parse("Bool"));
  }

  @Test
  public void testParseString() {
    assertEquals(ColumnType.STRING, ColumnType.parse("String"));
  }

  @Test
  public void testParseArray() {
    assertEquals(ColumnType.array(ColumnType.STRING), ColumnType.parse("Array(String)"));
  }

  @Test
  public void testParseEnum8() {
    Map<String, Integer> enumValues =
        ImmutableMap.of(
            "a", -1,
            "b", 0,
            "c", 42);

    assertEquals(
        ColumnType.enum8(enumValues), ColumnType.parse("Enum8('a' = -1, 'b' = 0, 'c' = 42)"));
  }

  @Test
  public void testParseEnum16() {
    Map<String, Integer> enumValues =
        ImmutableMap.of(
            "a", -1,
            "b", 0,
            "c", 42);

    assertEquals(
        ColumnType.enum16(enumValues), ColumnType.parse("Enum16('a' = -1, 'b' = 0, 'c' = 42)"));
  }

  @Test
  public void testParseNullableEnum16() {
    Map<String, Integer> enumValues =
        ImmutableMap.of(
            "a", -1,
            "b", 0,
            "c", 42);

    assertEquals(
        ColumnType.enum16(enumValues).withNullable(true),
        ColumnType.parse("Nullable(Enum16('a' = -1, 'b' = 0, 'c' = 42))"));
  }

  @Test
  public void testParseFixedString() {
    assertEquals(ColumnType.fixedString(16), ColumnType.parse("FixedString(16)"));
  }

  @Test
  public void testParseNullableFixedString() {
    assertEquals(
        ColumnType.fixedString(16).withNullable(true),
        ColumnType.parse("Nullable(FixedString(16))"));
  }

  @Test
  public void testParseNullableInt32() {
    assertEquals(
        ColumnType.nullable(TableSchema.TypeName.INT32), ColumnType.parse("Nullable(Int32)"));
  }

  @Test
  public void testParseArrayOfNullable() {
    assertEquals(
        ColumnType.array(ColumnType.nullable(TableSchema.TypeName.INT32)),
        ColumnType.parse("Array(Nullable(Int32))"));
  }

  @Test
  public void testParseArrayOfArrays() {
    assertEquals(
        ColumnType.array(ColumnType.array(ColumnType.STRING)),
        ColumnType.parse("Array(Array(String))"));
  }

  @Test
  public void testParseDefaultExpressionString() {
    assertEquals("abc", ColumnType.parseDefaultExpression(ColumnType.STRING, "abc"));
  }

  @Test
  public void testParseDefaultExpressionInt64() {
    assertEquals(-1L, ColumnType.parseDefaultExpression(ColumnType.INT64, "-1"));
  }

  @Test
  public void testEquivalentSchema() {
    TableSchema tableSchema =
        TableSchema.of(
            TableSchema.Column.of("f0", ColumnType.INT64),
            TableSchema.Column.of("f1", ColumnType.nullable(TableSchema.TypeName.INT64)));

    Schema expected =
        Schema.of(
            Schema.Field.of("f0", Schema.FieldType.INT64),
            Schema.Field.nullable("f1", Schema.FieldType.INT64));

    assertEquals(expected, TableSchema.getEquivalentSchema(tableSchema));
  }

  @Test
  public void testParseTupleSingle() {
    Map<String, ColumnType> m1 = new HashMap<>();
    m1.put("s", ColumnType.STRING);
    ColumnType columnType01 = ColumnType.parse("Tuple('s' String)");
    assertEquals(ColumnType.tuple(m1), columnType01);
  }

  @Test
  public void testParseTupleDouble() {
    Map<String, ColumnType> m2 = new HashMap<>();
    m2.put("a1", ColumnType.STRING);
    m2.put("b", ColumnType.BOOL);
    ColumnType columnType02 = ColumnType.parse("Tuple('a1' String,'b' Bool)");
    assertEquals(ColumnType.tuple(m2), columnType02);
  }

  @Test
  public void testTupleNested() {
    Map<String, ColumnType> m1 = new HashMap<>();
    m1.put("a", ColumnType.STRING);
    Map<String, ColumnType> m3 = new HashMap<>();
    m3.put("a", ColumnType.STRING);
    m3.put("b", ColumnType.BOOL);
    m3.put("c", ColumnType.tuple(m1));
    ColumnType columnType03 = ColumnType.parse("Tuple('a' String,'b' Bool,'c' Tuple('a' String))");
    assertEquals(ColumnType.tuple(m3), columnType03);
  }

  @Test
  public void testTupleComplex() {
    Map<String, ColumnType> m1 = new HashMap<>();
    m1.put("width", ColumnType.INT64.withNullable(true));
    m1.put("height", ColumnType.INT64.withNullable(true));

    Map<String, ColumnType> m2 = new HashMap<>();
    m2.put("name", ColumnType.STRING.withNullable(true));
    m2.put("size", ColumnType.tuple(m1));
    m2.put("version", ColumnType.STRING.withNullable(true));

    Map<String, ColumnType> m3 = new HashMap<>();
    m3.put("browser", ColumnType.tuple(m2));
    m3.put("deviceCategory", ColumnType.STRING.withNullable(true));

    ColumnType columnType03 =
        ColumnType.parse(
            "Tuple('browser' Tuple('name' Nullable(String),'size' Tuple('width' Nullable(Int64),'height' Nullable(Int64)),'version' Nullable(String)),'deviceCategory' Nullable(String))");
    assertEquals(ColumnType.tuple(m3), columnType03);
  }
}

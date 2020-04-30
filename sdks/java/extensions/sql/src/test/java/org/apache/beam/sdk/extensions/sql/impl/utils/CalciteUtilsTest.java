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
package org.apache.beam.sdk.extensions.sql.impl.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.logicaltypes.FixedLengthString;
import org.apache.beam.sdk.schemas.logicaltypes.LogicalDecimal;
import org.apache.beam.sdk.schemas.logicaltypes.VariableLengthBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableLengthString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

/** Tests for conversion from Beam schema to Calcite data type. */
public class CalciteUtilsTest {

  RelDataTypeFactory dataTypeFactory;

  @Before
  public void setUp() {
    dataTypeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
  }

  Map<String, RelDataType> calciteRowTypeFields(Schema schema) {
    final RelDataType dataType = CalciteUtils.toCalciteRowType(schema, dataTypeFactory);

    return dataType.getFieldNames().stream()
        .collect(
            Collectors.toMap(
                x -> x,
                x ->
                    dataType
                        .getField(x, /*caseSensitive=*/ true, /*elideRecord=*/ false)
                        .getType()));
  }

  @Test
  public void testToCalciteRowType() {
    final Schema schema =
        Schema.builder()
            .addField("byte", Schema.FieldType.BYTE)
            .addField("short", Schema.FieldType.INT16)
            .addField("int", Schema.FieldType.INT32)
            .addField("long", Schema.FieldType.INT64)
            .addField("float", Schema.FieldType.FLOAT)
            .addField("double", Schema.FieldType.DOUBLE)
            .addField("decimal", Schema.FieldType.DECIMAL)
            .addField("boolean", Schema.FieldType.BOOLEAN)
            .addField("byteArray", Schema.FieldType.BYTES)
            .addField("string", Schema.FieldType.STRING)
            .addField("char", CalciteUtils.CHAR)
            .addField("date", CalciteUtils.DATE)
            .addField("time", CalciteUtils.TIME)
            .addField("timestamp", CalciteUtils.TIMESTAMP)
            .addField("timeWithLocalTZ", CalciteUtils.TIME_WITH_LOCAL_TZ)
            .addField("timestampWithLocalTZ", CalciteUtils.TIMESTAMP_WITH_LOCAL_TZ)
            .addField("fixedBytes", Schema.FieldType.logicalType(FixedBytes.of(10)))
            .addField("variableBytes", Schema.FieldType.logicalType(VariableLengthBytes.of(100)))
            .addField("fixedString", Schema.FieldType.logicalType(FixedLengthString.of(10)))
            .addField("variableString", Schema.FieldType.logicalType(VariableLengthString.of(100)))
            .addField("customDecimal", Schema.FieldType.logicalType(LogicalDecimal.of(10, 5)))
            .build();

    final Map<String, RelDataType> fields = calciteRowTypeFields(schema);

    assertEquals(21, fields.size());

    fields.values().forEach(x -> assertFalse(x.isNullable()));

    assertEquals(SqlTypeName.TINYINT, fields.get("byte").getSqlTypeName());
    assertEquals(SqlTypeName.SMALLINT, fields.get("short").getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, fields.get("int").getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, fields.get("long").getSqlTypeName());
    assertEquals(SqlTypeName.FLOAT, fields.get("float").getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, fields.get("double").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("decimal").getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, fields.get("boolean").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("byteArray").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("string").getSqlTypeName());
    assertEquals(SqlTypeName.CHAR, fields.get("char").getSqlTypeName());
    assertEquals(SqlTypeName.DATE, fields.get("date").getSqlTypeName());
    assertEquals(SqlTypeName.TIME, fields.get("time").getSqlTypeName());
    assertEquals(SqlTypeName.TIMESTAMP, fields.get("timestamp").getSqlTypeName());
    assertEquals(
        SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, fields.get("timeWithLocalTZ").getSqlTypeName());
    assertEquals(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        fields.get("timestampWithLocalTZ").getSqlTypeName());
    assertEquals(SqlTypeName.BINARY, fields.get("fixedBytes").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("variableBytes").getSqlTypeName());
    assertEquals(SqlTypeName.CHAR, fields.get("fixedString").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("variableString").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("customDecimal").getSqlTypeName());
  }

  @Test
  public void testToCalciteRowTypeNullable() {
    final Schema schema =
        Schema.builder()
            .addNullableField("byte", Schema.FieldType.BYTE)
            .addNullableField("short", Schema.FieldType.INT16)
            .addNullableField("int", Schema.FieldType.INT32)
            .addNullableField("long", Schema.FieldType.INT64)
            .addNullableField("float", Schema.FieldType.FLOAT)
            .addNullableField("double", Schema.FieldType.DOUBLE)
            .addNullableField("decimal", Schema.FieldType.DECIMAL)
            .addNullableField("boolean", Schema.FieldType.BOOLEAN)
            .addNullableField("byteArray", Schema.FieldType.BYTES)
            .addNullableField("string", Schema.FieldType.STRING)
            .addNullableField("char", CalciteUtils.CHAR)
            .addNullableField("date", CalciteUtils.DATE)
            .addNullableField("time", CalciteUtils.TIME)
            .addNullableField("timestamp", CalciteUtils.TIMESTAMP)
            .addNullableField("timeWithLocalTZ", CalciteUtils.TIME_WITH_LOCAL_TZ)
            .addNullableField("timestampWithLocalTZ", CalciteUtils.TIMESTAMP_WITH_LOCAL_TZ)
            .addNullableField("fixedBytes", Schema.FieldType.logicalType(FixedBytes.of(10)))
            .addNullableField(
                "variableBytes", Schema.FieldType.logicalType(VariableLengthBytes.of(100)))
            .addNullableField("fixedString", Schema.FieldType.logicalType(FixedLengthString.of(10)))
            .addNullableField(
                "variableString", Schema.FieldType.logicalType(VariableLengthString.of(100)))
            .addNullableField(
                "customDecimal", Schema.FieldType.logicalType(LogicalDecimal.of(10, 5)))
            .build();

    final Map<String, RelDataType> fields = calciteRowTypeFields(schema);

    assertEquals(21, fields.size());

    fields.values().forEach(x -> assertTrue(x.isNullable()));

    assertEquals(SqlTypeName.TINYINT, fields.get("byte").getSqlTypeName());
    assertEquals(SqlTypeName.SMALLINT, fields.get("short").getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, fields.get("int").getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, fields.get("long").getSqlTypeName());
    assertEquals(SqlTypeName.FLOAT, fields.get("float").getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, fields.get("double").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("decimal").getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, fields.get("boolean").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("byteArray").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("string").getSqlTypeName());
    assertEquals(SqlTypeName.CHAR, fields.get("char").getSqlTypeName());
    assertEquals(SqlTypeName.DATE, fields.get("date").getSqlTypeName());
    assertEquals(SqlTypeName.TIME, fields.get("time").getSqlTypeName());
    assertEquals(SqlTypeName.TIMESTAMP, fields.get("timestamp").getSqlTypeName());
    assertEquals(
        SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE, fields.get("timeWithLocalTZ").getSqlTypeName());
    assertEquals(
        SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
        fields.get("timestampWithLocalTZ").getSqlTypeName());
    assertEquals(SqlTypeName.BINARY, fields.get("fixedBytes").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("variableBytes").getSqlTypeName());
    assertEquals(SqlTypeName.CHAR, fields.get("fixedString").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("variableString").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("customDecimal").getSqlTypeName());
  }

  @Test
  public void testRoundTripBeamSchema() {
    final Schema schema =
        Schema.builder()
            .addField("f1", Schema.FieldType.BYTE)
            .addField("f2", Schema.FieldType.INT16)
            .addField("f3", Schema.FieldType.INT32)
            .addField("f4", Schema.FieldType.INT64)
            .addField("f5", Schema.FieldType.FLOAT)
            .addField("f6", Schema.FieldType.DOUBLE)
            .addField("f7", Schema.FieldType.DECIMAL)
            .addField("f8", Schema.FieldType.BOOLEAN)
            .addField("f9", Schema.FieldType.BYTES)
            .addField("f10", Schema.FieldType.STRING)
            .build();

    final Schema out =
        CalciteUtils.toSchema(CalciteUtils.toCalciteRowType(schema, dataTypeFactory));

    assertEquals(schema, out);
  }

  @Test
  public void testRoundTripBeamNullableSchema() {
    final Schema schema =
        Schema.builder()
            .addNullableField("f1", Schema.FieldType.BYTE)
            .addNullableField("f2", Schema.FieldType.INT16)
            .addNullableField("f3", Schema.FieldType.INT32)
            .addNullableField("f4", Schema.FieldType.INT64)
            .addNullableField("f5", Schema.FieldType.FLOAT)
            .addNullableField("f6", Schema.FieldType.DOUBLE)
            .addNullableField("f7", Schema.FieldType.DECIMAL)
            .addNullableField("f8", Schema.FieldType.BOOLEAN)
            .addNullableField("f9", Schema.FieldType.BYTES)
            .addNullableField("f10", Schema.FieldType.STRING)
            .build();

    final Schema out =
        CalciteUtils.toSchema(CalciteUtils.toCalciteRowType(schema, dataTypeFactory));

    assertEquals(schema, out);
  }
}

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
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataType;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.beam.vendor.calcite.v1_26_0.org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;

/** Tests for conversion from Beam schema to Calcite data type. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
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

    final Map<String, RelDataType> fields = calciteRowTypeFields(schema);

    assertEquals(10, fields.size());

    fields.values().forEach(x -> assertFalse(x.isNullable()));

    assertEquals(SqlTypeName.TINYINT, fields.get("f1").getSqlTypeName());
    assertEquals(SqlTypeName.SMALLINT, fields.get("f2").getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, fields.get("f3").getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, fields.get("f4").getSqlTypeName());
    assertEquals(SqlTypeName.FLOAT, fields.get("f5").getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, fields.get("f6").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("f7").getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, fields.get("f8").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("f9").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("f10").getSqlTypeName());
  }

  @Test
  public void testToCalciteRowTypeNullable() {
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

    final Map<String, RelDataType> fields = calciteRowTypeFields(schema);

    assertEquals(10, fields.size());

    fields.values().forEach(x -> assertTrue(x.isNullable()));

    assertEquals(SqlTypeName.TINYINT, fields.get("f1").getSqlTypeName());
    assertEquals(SqlTypeName.SMALLINT, fields.get("f2").getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, fields.get("f3").getSqlTypeName());
    assertEquals(SqlTypeName.BIGINT, fields.get("f4").getSqlTypeName());
    assertEquals(SqlTypeName.FLOAT, fields.get("f5").getSqlTypeName());
    assertEquals(SqlTypeName.DOUBLE, fields.get("f6").getSqlTypeName());
    assertEquals(SqlTypeName.DECIMAL, fields.get("f7").getSqlTypeName());
    assertEquals(SqlTypeName.BOOLEAN, fields.get("f8").getSqlTypeName());
    assertEquals(SqlTypeName.VARBINARY, fields.get("f9").getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, fields.get("f10").getSqlTypeName());
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

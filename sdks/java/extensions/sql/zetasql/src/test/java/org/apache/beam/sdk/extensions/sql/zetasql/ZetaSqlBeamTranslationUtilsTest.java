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
package org.apache.beam.sdk.extensions.sql.zetasql;

import static org.junit.Assert.assertEquals;

import com.google.protobuf.ByteString;
import com.google.zetasql.ArrayType;
import com.google.zetasql.StructType;
import com.google.zetasql.StructType.StructField;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.Value;
import com.google.zetasql.ZetaSQLType.TypeKind;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for utility methods for ZetaSQL related operations. */
@RunWith(JUnit4.class)
public class ZetaSqlBeamTranslationUtilsTest {

  private static final Schema TEST_INNER_SCHEMA =
      Schema.builder().addField("i1", FieldType.INT64).addField("i2", FieldType.STRING).build();

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addField("f_int64", FieldType.INT64)
          .addField("f_float64", FieldType.DOUBLE)
          .addField("f_boolean", FieldType.BOOLEAN)
          .addField("f_string", FieldType.STRING)
          .addField("f_bytes", FieldType.BYTES)
          .addLogicalTypeField("f_date", SqlTypes.DATE)
          .addLogicalTypeField("f_datetime", SqlTypes.DATETIME)
          .addLogicalTypeField("f_time", SqlTypes.TIME)
          .addField("f_timestamp", FieldType.DATETIME)
          .addArrayField("f_array", FieldType.DOUBLE)
          .addRowField("f_struct", TEST_INNER_SCHEMA)
          .addField("f_numeric", FieldType.DECIMAL)
          .addNullableField("f_null", FieldType.INT64)
          .build();

  private static final FieldType TEST_FIELD_TYPE = FieldType.row(TEST_SCHEMA);

  private static final ArrayType TEST_INNER_ARRAY_TYPE =
      TypeFactory.createArrayType(TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE));

  private static final StructType TEST_INNER_STRUCT_TYPE =
      TypeFactory.createStructType(
          Arrays.asList(
              new StructField("i1", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
              new StructField("i2", TypeFactory.createSimpleType(TypeKind.TYPE_STRING))));

  private static final StructType TEST_TYPE =
      TypeFactory.createStructType(
          Arrays.asList(
              new StructField("f_int64", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
              new StructField("f_float64", TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE)),
              new StructField("f_boolean", TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
              new StructField("f_string", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
              new StructField("f_bytes", TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)),
              new StructField("f_date", TypeFactory.createSimpleType(TypeKind.TYPE_DATE)),
              new StructField("f_datetime", TypeFactory.createSimpleType(TypeKind.TYPE_DATETIME)),
              new StructField("f_time", TypeFactory.createSimpleType(TypeKind.TYPE_TIME)),
              new StructField("f_timestamp", TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
              new StructField("f_array", TEST_INNER_ARRAY_TYPE),
              new StructField("f_struct", TEST_INNER_STRUCT_TYPE),
              new StructField("f_numeric", TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)),
              new StructField("f_null", TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

  private static final Row TEST_ROW =
      Row.withSchema(TEST_SCHEMA)
          .addValue(64L)
          .addValue(5.15)
          .addValue(false)
          .addValue("Hello")
          .addValue(new byte[] {0x11, 0x22})
          .addValue(LocalDate.of(2020, 6, 4))
          .addValue(LocalDateTime.of(2008, 12, 25, 15, 30, 0).withNano(123456000))
          .addValue(LocalTime.of(15, 30, 45).withNano(123456000))
          .addValue(Instant.ofEpochMilli(12345678L))
          .addArray(3.0, 6.5)
          .addValue(Row.withSchema(TEST_INNER_SCHEMA).addValues(0L, "world").build())
          .addValue(ZetaSqlTypesUtils.bigDecimalAsNumeric("12346"))
          .addValue(null)
          .build();

  private static final Value TEST_VALUE =
      Value.createStructValue(
          TEST_TYPE,
          Arrays.asList(
              Value.createInt64Value(64L),
              Value.createDoubleValue(5.15),
              Value.createBoolValue(false),
              Value.createStringValue("Hello"),
              Value.createBytesValue(ByteString.copyFrom(new byte[] {0x11, 0x22})),
              Value.createDateValue(LocalDate.of(2020, 6, 4)),
              Value.createDatetimeValue(
                  LocalDateTime.of(2008, 12, 25, 15, 30, 0).withNano(123456000)),
              Value.createTimeValue(LocalTime.of(15, 30, 45).withNano(123456000)),
              Value.createTimestampValueFromUnixMicros(12345678000L),
              Value.createArrayValue(
                  TEST_INNER_ARRAY_TYPE,
                  Arrays.asList(Value.createDoubleValue(3.0), Value.createDoubleValue(6.5))),
              Value.createStructValue(
                  TEST_INNER_STRUCT_TYPE,
                  Arrays.asList(Value.createInt64Value(0L), Value.createStringValue("world"))),
              Value.createNumericValue(ZetaSqlTypesUtils.bigDecimalAsNumeric("12346")),
              Value.createNullValue(TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

  @Test
  public void testBeamFieldTypeToZetaSqlType() {
    assertEquals(ZetaSqlBeamTranslationUtils.toZetaSqlType(TEST_FIELD_TYPE), TEST_TYPE);
  }

  @Test
  public void testJavaObjectToZetaSqlValue() {
    assertEquals(ZetaSqlBeamTranslationUtils.toZetaSqlValue(TEST_ROW, TEST_FIELD_TYPE), TEST_VALUE);
  }

  @Test
  public void testZetaSqlValueToJavaObject() {
    assertEquals(
        ZetaSqlBeamTranslationUtils.toBeamObject(TEST_VALUE, TEST_FIELD_TYPE, true), TEST_ROW);
  }
}

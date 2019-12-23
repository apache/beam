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
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for utility methods for ZetaSQL related operations. */
@RunWith(JUnit4.class)
public class ZetaSqlUtilsTest {

  private static final Schema TEST_INNER_SCHEMA =
      Schema.builder().addField("i1", FieldType.INT64).addField("i2", FieldType.STRING).build();

  private static final Schema TEST_SCHEMA =
      Schema.builder()
          .addField("f1", FieldType.INT64)
          // .addField("f2", FieldType.DECIMAL)
          .addField("f3", FieldType.DOUBLE)
          .addField("f4", FieldType.STRING)
          .addField("f5", FieldType.DATETIME)
          .addField("f6", FieldType.BOOLEAN)
          .addField("f7", FieldType.BYTES)
          .addArrayField("f8", FieldType.DOUBLE)
          .addRowField("f9", TEST_INNER_SCHEMA)
          .addNullableField("f10", FieldType.INT64)
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
              new StructField("f1", TypeFactory.createSimpleType(TypeKind.TYPE_INT64)),
              // new StructField("f2", TypeFactory.createSimpleType(TypeKind.TYPE_NUMERIC)),
              new StructField("f3", TypeFactory.createSimpleType(TypeKind.TYPE_DOUBLE)),
              new StructField("f4", TypeFactory.createSimpleType(TypeKind.TYPE_STRING)),
              new StructField("f5", TypeFactory.createSimpleType(TypeKind.TYPE_TIMESTAMP)),
              new StructField("f6", TypeFactory.createSimpleType(TypeKind.TYPE_BOOL)),
              new StructField("f7", TypeFactory.createSimpleType(TypeKind.TYPE_BYTES)),
              new StructField("f8", TEST_INNER_ARRAY_TYPE),
              new StructField("f9", TEST_INNER_STRUCT_TYPE),
              new StructField("f10", TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

  private static final Row TEST_ROW =
      Row.withSchema(TEST_SCHEMA)
          .addValue(64L)
          // .addValue(BigDecimal.valueOf(9999L))
          .addValue(5.0)
          .addValue("Hello")
          .addValue(Instant.ofEpochMilli(12345678L))
          .addValue(false)
          .addValue(new byte[] {0x11, 0x22})
          .addArray(3.0, 6.5)
          .addValue(Row.withSchema(TEST_INNER_SCHEMA).addValues(0L, "world").build())
          .addValue(null)
          .build();

  private static final Value TEST_VALUE =
      Value.createStructValue(
          TEST_TYPE,
          Arrays.asList(
              Value.createInt64Value(64L),
              // TODO[BEAM-8630]: Value.createNumericValue() is broken due to a dependency issue
              // Value.createNumericValue(BigDecimal.valueOf(9999L)),
              Value.createDoubleValue(5.0),
              Value.createStringValue("Hello"),
              Value.createTimestampValueFromUnixMicros(12345678000L),
              Value.createBoolValue(false),
              Value.createBytesValue(ByteString.copyFrom(new byte[] {0x11, 0x22})),
              Value.createArrayValue(
                  TEST_INNER_ARRAY_TYPE,
                  Arrays.asList(Value.createDoubleValue(3.0), Value.createDoubleValue(6.5))),
              Value.createStructValue(
                  TEST_INNER_STRUCT_TYPE,
                  Arrays.asList(Value.createInt64Value(0L), Value.createStringValue("world"))),
              Value.createNullValue(TypeFactory.createSimpleType(TypeKind.TYPE_INT64))));

  @Test
  public void testBeamFieldTypeToZetaSqlType() {
    assertEquals(ZetaSqlUtils.beamFieldTypeToZetaSqlType(TEST_FIELD_TYPE), TEST_TYPE);
  }

  @Test
  public void testJavaObjectToZetaSqlValue() {
    assertEquals(ZetaSqlUtils.javaObjectToZetaSqlValue(TEST_ROW, TEST_FIELD_TYPE), TEST_VALUE);
  }

  @Test
  public void testZetaSqlValueToJavaObject() {
    assertEquals(ZetaSqlUtils.zetaSqlValueToJavaObject(TEST_VALUE, TEST_FIELD_TYPE), TEST_ROW);
  }
}

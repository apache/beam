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

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.ObjectAndMaxId;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamFieldTypeToIcebergFieldType;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(Enclosed.class)
public class IcebergUtilsTest {

  @RunWith(JUnit4.class)
  public static class RowToRecordTests {
    /**
     * Checks a value that when converted to Iceberg type is the same value when interpreted in
     * Java.
     */
    private void checkRowValueToRecordValue(
        Schema.FieldType sourceType, Type destType, Object value) {
      checkRowValueToRecordValue(sourceType, value, destType, value);
    }

    private void checkRowValueToRecordValue(
        Schema.FieldType sourceType, Object sourceValue, Type destType, Object destValue) {
      Schema beamSchema = Schema.of(Schema.Field.of("v", sourceType));
      Row row = Row.withSchema(beamSchema).addValues(sourceValue).build();

      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(required(0, "v", destType));
      Record record = IcebergUtils.beamRowToIcebergRecord(icebergSchema, row);

      assertThat(record.getField("v"), equalTo(destValue));
    }

    @Test
    public void testBoolean() {
      checkRowValueToRecordValue(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), true);
      checkRowValueToRecordValue(Schema.FieldType.BOOLEAN, Types.BooleanType.get(), false);
    }

    @Test
    public void testInteger() {
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), -13);
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), 42);
      checkRowValueToRecordValue(Schema.FieldType.INT32, Types.IntegerType.get(), 0);
    }

    @Test
    public void testLong() {
      checkRowValueToRecordValue(Schema.FieldType.INT64, Types.LongType.get(), 13L);
      checkRowValueToRecordValue(Schema.FieldType.INT64, Types.LongType.get(), 42L);
    }

    @Test
    public void testFloat() {
      checkRowValueToRecordValue(Schema.FieldType.FLOAT, Types.FloatType.get(), 3.14159f);
      checkRowValueToRecordValue(Schema.FieldType.FLOAT, Types.FloatType.get(), 42.0f);
    }

    @Test
    public void testDouble() {
      checkRowValueToRecordValue(Schema.FieldType.DOUBLE, Types.DoubleType.get(), 3.14159);
    }

    @Test
    public void testDate() {}

    @Test
    public void testTime() {}

    @Test
    public void testTimestamp() {
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

      checkRowValueToRecordValue(
          Schema.FieldType.DATETIME,
          dateTime.toInstant(),
          Types.TimestampType.withoutZone(),
          dateTime.getMillis());
    }

    @Test
    public void testFixed() {}

    @Test
    public void testBinary() {
      byte[] bytes = new byte[] {1, 2, 3, 4};
      checkRowValueToRecordValue(
          Schema.FieldType.BYTES, bytes, Types.BinaryType.get(), ByteBuffer.wrap(bytes));
    }

    @Test
    public void testDecimal() {
      BigDecimal num = BigDecimal.valueOf(123.456);

      checkRowValueToRecordValue(Schema.FieldType.DECIMAL, Types.DecimalType.of(6, 3), num);
    }

    @Test
    public void testStruct() {
      Schema schema = Schema.builder().addStringField("nested_str").build();
      Row beamRow = Row.withSchema(schema).addValue("str_value").build();

      Types.NestedField nestedFieldType = required(1, "nested_str", Types.StringType.get());
      GenericRecord icebergRow =
          GenericRecord.create(new org.apache.iceberg.Schema(nestedFieldType));
      icebergRow.setField("nested_str", "str_value");

      checkRowValueToRecordValue(
          Schema.FieldType.row(schema), beamRow, Types.StructType.of(nestedFieldType), icebergRow);
    }

    @Test
    public void testMap() {
      Map<String, Integer> map =
          ImmutableMap.<String, Integer>builder().put("a", 123).put("b", 456).put("c", 789).build();

      checkRowValueToRecordValue(
          Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32),
          Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get()),
          map);
    }

    @Test
    public void testList() {
      List<String> list = Arrays.asList("abc", "xyz", "123", "foo", "bar");

      checkRowValueToRecordValue(
          Schema.FieldType.array(Schema.FieldType.STRING),
          Types.ListType.ofRequired(1, Types.StringType.get()),
          list);
    }
  }

  @RunWith(JUnit4.class)
  public static class RecordToRowTests {
    private void checkRecordValueToRowValue(
        Type sourceType, Schema.FieldType destType, Object value) {
      checkRecordValueToRowValue(sourceType, value, destType, value);
    }

    private void checkRecordValueToRowValue(
        Type sourceType, Object sourceValue, Schema.FieldType destType, Object destValue) {
      Schema beamSchema = Schema.of(Schema.Field.of("v", destType));

      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(required(0, "v", sourceType));
      Record record = GenericRecord.create(icebergSchema);
      record.setField("v", sourceValue);

      Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);

      assertThat(row.getBaseValue("v"), equalTo(destValue));
    }

    @Test
    public void testBoolean() {
      checkRecordValueToRowValue(Types.BooleanType.get(), Schema.FieldType.BOOLEAN, true);
      checkRecordValueToRowValue(Types.BooleanType.get(), Schema.FieldType.BOOLEAN, false);
    }

    @Test
    public void testInteger() {
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, -13);
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, 42);
      checkRecordValueToRowValue(Types.IntegerType.get(), Schema.FieldType.INT32, 0);
    }

    @Test
    public void testLong() {
      checkRecordValueToRowValue(Types.LongType.get(), Schema.FieldType.INT64, 13L);
      checkRecordValueToRowValue(Types.LongType.get(), Schema.FieldType.INT64, 42L);
    }

    @Test
    public void testFloat() {
      checkRecordValueToRowValue(Types.FloatType.get(), Schema.FieldType.FLOAT, 3.14159f);
      checkRecordValueToRowValue(Types.FloatType.get(), Schema.FieldType.FLOAT, 42.0f);
    }

    @Test
    public void testDouble() {
      checkRecordValueToRowValue(Types.DoubleType.get(), Schema.FieldType.DOUBLE, 3.14159);
    }

    @Test
    public void testDate() {}

    @Test
    public void testTime() {}

    @Test
    public void testTimestamp() {
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);

      checkRecordValueToRowValue(
          Types.TimestampType.withoutZone(),
          dateTime.getMillis(),
          Schema.FieldType.DATETIME,
          dateTime.toInstant());
    }

    @Test
    public void testFixed() {}

    @Test
    public void testBinary() {
      byte[] bytes = new byte[] {1, 2, 3, 4};
      checkRecordValueToRowValue(
          Types.BinaryType.get(), ByteBuffer.wrap(bytes), Schema.FieldType.BYTES, bytes);
    }

    @Test
    public void testDecimal() {
      BigDecimal num = BigDecimal.valueOf(123.456);

      checkRecordValueToRowValue(Types.DecimalType.of(6, 3), Schema.FieldType.DECIMAL, num);
    }

    @Test
    public void testStruct() {
      Schema schema = Schema.builder().addStringField("nested_str").build();
      Row beamRow = Row.withSchema(schema).addValue("str_value").build();

      Types.NestedField nestedFieldType = required(1, "nested_str", Types.StringType.get());
      GenericRecord icebergRow =
          GenericRecord.create(new org.apache.iceberg.Schema(nestedFieldType));
      icebergRow.setField("nested_str", "str_value");

      checkRecordValueToRowValue(
          Types.StructType.of(nestedFieldType), icebergRow, Schema.FieldType.row(schema), beamRow);
    }

    @Test
    public void testMap() {
      Map<String, Integer> map =
          ImmutableMap.<String, Integer>builder().put("a", 123).put("b", 456).put("c", 789).build();

      checkRecordValueToRowValue(
          Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get()),
          Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32),
          map);
    }

    @Test
    public void testList() {
      List<String> list = Arrays.asList("abc", "xyz", "123", "foo", "bar");

      checkRecordValueToRowValue(
          Types.ListType.ofRequired(1, Types.StringType.get()),
          Schema.FieldType.iterable(Schema.FieldType.STRING),
          list);
    }
  }

  @RunWith(JUnit4.class)
  public static class SchemaTests {
    private static class BeamFieldTypeTestCase {
      final int icebergFieldId;
      final Schema.FieldType beamType;
      final int expectedMaxId;
      final Type expectedIcebergType;

      BeamFieldTypeTestCase(
          int icebergFieldId,
          Schema.FieldType beamType,
          int expectedMaxId,
          Type expectedIcebergType) {
        this.icebergFieldId = icebergFieldId;
        this.beamType = beamType;
        this.expectedMaxId = expectedMaxId;
        this.expectedIcebergType = expectedIcebergType;
      }
    }

    private void checkTypes(List<BeamFieldTypeTestCase> testCases) {
      for (BeamFieldTypeTestCase testCase : testCases) {
        ObjectAndMaxId<Type> ret =
            beamFieldTypeToIcebergFieldType(testCase.icebergFieldId, testCase.beamType);

        assertEquals(testCase.expectedMaxId, ret.maxId);
        checkEquals(testCase.expectedIcebergType, ret.object);
      }
    }

    private void checkEquals(Type expected, Type actual) {
      if (expected.isListType() && actual.isListType()) {
        Type nestedExpected = expected.asListType().elementType();
        Type nestedActual = actual.asListType().elementType();

        assertEquals(nestedExpected.typeId(), nestedActual.typeId());
        checkEquals(nestedExpected, nestedActual);
      } else {
        assertEquals(expected, actual);
      }
    }

    @Test
    public void testPrimitiveBeamFieldTypeToIcebergFieldType() {
      List<BeamFieldTypeTestCase> primitives =
          Arrays.asList(
              new BeamFieldTypeTestCase(1, Schema.FieldType.BOOLEAN, 1, Types.BooleanType.get()),
              new BeamFieldTypeTestCase(3, Schema.FieldType.INT32, 3, Types.IntegerType.get()),
              new BeamFieldTypeTestCase(6, Schema.FieldType.INT64, 6, Types.LongType.get()),
              new BeamFieldTypeTestCase(10, Schema.FieldType.FLOAT, 10, Types.FloatType.get()),
              new BeamFieldTypeTestCase(7, Schema.FieldType.DOUBLE, 7, Types.DoubleType.get()),
              new BeamFieldTypeTestCase(11, Schema.FieldType.STRING, 11, Types.StringType.get()),
              new BeamFieldTypeTestCase(15, Schema.FieldType.BYTES, 15, Types.BinaryType.get()));

      checkTypes(primitives);
    }

    @Test
    public void testArrayBeamFieldTypeToIcebergFieldType() {
      // Iceberg sets one field ID for the List type itself and another field ID for the collection
      // type.
      List<BeamFieldTypeTestCase> listTypes =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.array(Schema.FieldType.BOOLEAN),
                  2,
                  Types.ListType.ofRequired(1, Types.BooleanType.get())),
              new BeamFieldTypeTestCase(
                  3,
                  Schema.FieldType.iterable(Schema.FieldType.INT32),
                  4,
                  Types.ListType.ofRequired(3, Types.IntegerType.get())),
              new BeamFieldTypeTestCase(
                  6,
                  Schema.FieldType.array(Schema.FieldType.INT64),
                  7,
                  Types.ListType.ofRequired(6, Types.LongType.get())),
              new BeamFieldTypeTestCase(
                  10,
                  Schema.FieldType.array(Schema.FieldType.FLOAT),
                  11,
                  Types.ListType.ofRequired(10, Types.FloatType.get())),
              new BeamFieldTypeTestCase(
                  7,
                  Schema.FieldType.iterable(Schema.FieldType.DOUBLE),
                  8,
                  Types.ListType.ofRequired(7, Types.DoubleType.get())),
              new BeamFieldTypeTestCase(
                  11,
                  Schema.FieldType.array(Schema.FieldType.STRING),
                  12,
                  Types.ListType.ofRequired(11, Types.StringType.get())),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.iterable(Schema.FieldType.BYTES),
                  16,
                  Types.ListType.ofRequired(15, Types.BinaryType.get())),
              new BeamFieldTypeTestCase(
                  23,
                  Schema.FieldType.array(
                      Schema.FieldType.array(Schema.FieldType.iterable(Schema.FieldType.STRING))),
                  26,
                  Types.ListType.ofRequired(
                      23,
                      Types.ListType.ofRequired(
                          24, Types.ListType.ofRequired(25, Types.StringType.get())))));

      checkTypes(listTypes);
    }

    @Test
    public void testStructBeamFieldTypeToIcebergFieldType() {
      // Iceberg sets one field ID for each nested type.
      List<BeamFieldTypeTestCase> listTypes =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.row(Schema.builder().addStringField("str").build()),
                  2,
                  Types.StructType.of(
                      Types.NestedField.required(2, "str", Types.StringType.get()))),
              new BeamFieldTypeTestCase(
                  3,
                  Schema.FieldType.row(Schema.builder().addInt32Field("int").build()),
                  4,
                  Types.StructType.of(
                      Types.NestedField.required(4, "int", Types.IntegerType.get()))),
              new BeamFieldTypeTestCase(
                  0,
                  Schema.FieldType.row(BEAM_SCHEMA_PRIMITIVE),
                  7,
                  Types.StructType.of(ICEBERG_SCHEMA_PRIMITIVE.columns())),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.row(
                      Schema.builder()
                          .addArrayField("arr", Schema.FieldType.STRING)
                          .addNullableStringField("str")
                          .build()),
                  18,
                  Types.StructType.of(
                      Types.NestedField.required(
                          16, "arr", Types.ListType.ofRequired(17, Types.StringType.get())),
                      Types.NestedField.optional(18, "str", Types.StringType.get()))),
              new BeamFieldTypeTestCase(
                  20,
                  Schema.FieldType.row(
                      Schema.builder()
                          .addRowField(
                              "row",
                              Schema.builder()
                                  .addRowField(
                                      "nested_row", Schema.builder().addStringField("str").build())
                                  .build())
                          .addNullableRowField(
                              "nullable_row", Schema.builder().addInt64Field("long").build())
                          .build()),
                  25,
                  Types.StructType.of(
                      Types.NestedField.required(
                          21,
                          "row",
                          Types.StructType.of(
                              Types.NestedField.required(
                                  22,
                                  "nested_row",
                                  Types.StructType.of(
                                      Types.NestedField.required(
                                          23, "str", Types.StringType.get()))))),
                      Types.NestedField.optional(
                          24,
                          "nullable_row",
                          Types.StructType.of(
                              Types.NestedField.required(25, "long", Types.LongType.get()))))));

      checkTypes(listTypes);
    }

    @Test
    public void testMapBeamFieldTypeToIcebergFieldType() {
      List<BeamFieldTypeTestCase> primitives =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32),
                  3,
                  Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.IntegerType.get())),
              new BeamFieldTypeTestCase(
                  6,
                  Schema.FieldType.map(
                      Schema.FieldType.FLOAT, Schema.FieldType.array(Schema.FieldType.STRING)),
                  9,
                  Types.MapType.ofRequired(
                      7,
                      8,
                      Types.FloatType.get(),
                      Types.ListType.ofRequired(9, Types.StringType.get()))),
              new BeamFieldTypeTestCase(
                  10,
                  Schema.FieldType.map(
                      Schema.FieldType.STRING,
                      Schema.FieldType.map(
                          Schema.FieldType.BOOLEAN,
                          Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32))),
                  16,
                  Types.MapType.ofRequired(
                      11,
                      12,
                      Types.StringType.get(),
                      Types.MapType.ofRequired(
                          13,
                          14,
                          Types.BooleanType.get(),
                          Types.MapType.ofRequired(
                              15, 16, Types.StringType.get(), Types.IntegerType.get())))),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.map(
                      Schema.FieldType.row(Schema.builder().addStringField("str").build()),
                      Schema.FieldType.row(Schema.builder().addInt32Field("int").build())),
                  19,
                  Types.MapType.ofRequired(
                      16,
                      17,
                      Types.StructType.of(
                          Types.NestedField.required(18, "str", Types.StringType.get())),
                      Types.StructType.of(
                          Types.NestedField.required(19, "int", Types.IntegerType.get())))));

      checkTypes(primitives);
    }

    static final Schema BEAM_SCHEMA_PRIMITIVE =
        Schema.builder()
            .addInt32Field("int")
            .addFloatField("float")
            .addNullableDoubleField("double")
            .addInt64Field("long")
            .addNullableStringField("str")
            .addNullableBooleanField("bool")
            .addByteArrayField("bytes")
            .build();

    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_PRIMITIVE =
        new org.apache.iceberg.Schema(
            required(1, "int", Types.IntegerType.get()),
            required(2, "float", Types.FloatType.get()),
            optional(3, "double", Types.DoubleType.get()),
            required(4, "long", Types.LongType.get()),
            optional(5, "str", Types.StringType.get()),
            optional(6, "bool", Types.BooleanType.get()),
            required(7, "bytes", Types.BinaryType.get()));

    @Test
    public void testPrimitiveBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA_PRIMITIVE);

      System.out.println(convertedIcebergSchema);
      System.out.println(ICEBERG_SCHEMA_PRIMITIVE);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA_PRIMITIVE));
    }

    @Test
    public void testPrimitiveIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA_PRIMITIVE);

      assertEquals(BEAM_SCHEMA_PRIMITIVE, convertedBeamSchema);
    }

    static final Schema BEAM_SCHEMA_LIST =
        Schema.builder()
            .addIterableField("arr_str", Schema.FieldType.STRING)
            .addIterableField("arr_int", Schema.FieldType.INT32)
            .addIterableField("arr_bool", Schema.FieldType.BOOLEAN)
            .build();
    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_LIST =
        new org.apache.iceberg.Schema(
            required(1, "arr_str", Types.ListType.ofRequired(2, Types.StringType.get())),
            required(3, "arr_int", Types.ListType.ofRequired(4, Types.IntegerType.get())),
            required(5, "arr_bool", Types.ListType.ofRequired(6, Types.BooleanType.get())));

    @Test
    public void testArrayBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA_LIST);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA_LIST));
    }

    @Test
    public void testArrayIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA_LIST);

      System.out.println(convertedBeamSchema);
      System.out.println(BEAM_SCHEMA_LIST);

      assertEquals(BEAM_SCHEMA_LIST, convertedBeamSchema);
    }

    static final Schema BEAM_SCHEMA_MAP =
        Schema.builder()
            .addMapField("str_int", Schema.FieldType.STRING, Schema.FieldType.INT32)
            .addNullableMapField("long_bool", Schema.FieldType.INT64, Schema.FieldType.BOOLEAN)
            .build();

    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_MAP =
        new org.apache.iceberg.Schema(
            required(
                1,
                "str_int",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.IntegerType.get())),
            optional(
                4,
                "long_bool",
                Types.MapType.ofRequired(5, 6, Types.LongType.get(), Types.BooleanType.get())));

    @Test
    public void testMapBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA_MAP);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA_MAP));
    }

    @Test
    public void testMapIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA_MAP);

      assertEquals(BEAM_SCHEMA_MAP, convertedBeamSchema);
    }

    static final Schema BEAM_SCHEMA_STRUCT =
        Schema.builder()
            .addRowField(
                "row",
                Schema.builder()
                    .addStringField("str")
                    .addNullableInt32Field("int")
                    .addInt64Field("long")
                    .build())
            .addNullableRowField(
                "nullable_row",
                Schema.builder().addNullableStringField("str").addBooleanField("bool").build())
            .build();

    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_STRUCT =
        new org.apache.iceberg.Schema(
            required(
                1,
                "row",
                Types.StructType.of(
                    required(2, "str", Types.StringType.get()),
                    optional(3, "int", Types.IntegerType.get()),
                    required(4, "long", Types.LongType.get()))),
            optional(
                5,
                "nullable_row",
                Types.StructType.of(
                    optional(6, "str", Types.StringType.get()),
                    required(7, "bool", Types.BooleanType.get()))));

    @Test
    public void testStructBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA_STRUCT);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA_STRUCT));
    }

    @Test
    public void testStructIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA_STRUCT);

      assertEquals(BEAM_SCHEMA_STRUCT, convertedBeamSchema);
    }
  }
}

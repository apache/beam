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

import static org.apache.beam.sdk.io.iceberg.IcebergUtils.TypeAndMaxId;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamFieldTypeToIcebergFieldType;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link IcebergUtils}. */
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
    public void testDate() {
      checkRowValueToRecordValue(
          Schema.FieldType.logicalType(SqlTypes.DATE),
          Types.DateType.get(),
          DateTimeUtil.dateFromDays(12345));
    }

    @Test
    public void testTime() {
      checkRowValueToRecordValue(
          Schema.FieldType.logicalType(SqlTypes.TIME),
          Types.TimeType.get(),
          DateTimeUtil.timeFromMicros(12345678L));
    }

    @Test
    public void testTimestamp() {
      // SqlTypes.DATETIME
      checkRowValueToRecordValue(
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          Types.TimestampType.withoutZone(),
          DateTimeUtil.timestampFromMicros(123456789L));

      // Schema.FieldType.DATETIME
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
      checkRowValueToRecordValue(
          Schema.FieldType.DATETIME,
          dateTime,
          Types.TimestampType.withoutZone(),
          DateTimeUtil.timestampFromMicros(dateTime.getMillis() * 1000L));

      // Schema.FieldType.INT64
      long micros = 1234567890L;
      checkRowValueToRecordValue(
          Schema.FieldType.INT64,
          micros,
          Types.TimestampType.withoutZone(),
          DateTimeUtil.timestampFromMicros(micros));

      // Schema.FieldType.STRING
      String val = "2024-10-08T13:18:20.053";
      LocalDateTime localDateTime = LocalDateTime.of(2024, 10, 8, 13, 18, 20, 53_000_000);
      checkRowValueToRecordValue(
          Schema.FieldType.STRING, val, Types.TimestampType.withoutZone(), localDateTime);
    }

    @Test
    public void testTimestampWithZone() {
      String val = "2024-10-08T13:18:20.053+03:27";
      DateTime dateTime = DateTime.parse(val);
      OffsetDateTime offsetDateTime = OffsetDateTime.parse(val);
      LocalDateTime localDateTime =
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
      // SqlTypes.DATETIME
      checkRowValueToRecordValue(
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          localDateTime,
          Types.TimestampType.withZone(),
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));

      // Schema.FieldType.DATETIME
      checkRowValueToRecordValue(
          Schema.FieldType.DATETIME,
          dateTime,
          Types.TimestampType.withZone(),
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));

      // Schema.FieldType.INT64
      checkRowValueToRecordValue(
          Schema.FieldType.INT64,
          DateTimeUtil.microsFromTimestamptz(offsetDateTime),
          Types.TimestampType.withZone(),
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));

      // Schema.FieldType.STRING
      checkRowValueToRecordValue(
          Schema.FieldType.STRING,
          val,
          Types.TimestampType.withZone(),
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC));
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

      assertThat(row.getValue("v"), equalTo(destValue));
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
    public void testDate() {
      checkRecordValueToRowValue(
          Types.DateType.get(),
          Schema.FieldType.logicalType(SqlTypes.DATE),
          DateTimeUtil.dateFromDays(12345));
    }

    @Test
    public void testTime() {
      checkRecordValueToRowValue(
          Types.TimeType.get(),
          Schema.FieldType.logicalType(SqlTypes.TIME),
          DateTimeUtil.timeFromMicros(1234567L));
    }

    @Test
    public void testTimestamp() {
      // SqlTypes.DATETIME
      checkRecordValueToRowValue(
          Types.TimestampType.withoutZone(),
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          DateTimeUtil.timestampFromMicros(123456789L));

      // Schema.FieldType.DATETIME
      DateTime dateTime =
          new DateTime().withDate(1979, 03, 14).withTime(1, 2, 3, 4).withZone(DateTimeZone.UTC);
      checkRecordValueToRowValue(
          Types.TimestampType.withoutZone(),
          dateTime.getMillis() * 1000L,
          Schema.FieldType.DATETIME,
          dateTime);
    }

    @Test
    public void testTimestampWithZone() {
      String timestamp = "2024-10-08T13:18:20.053+03:27";
      OffsetDateTime offsetDateTime = OffsetDateTime.parse(timestamp);
      LocalDateTime localDateTime =
          offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime();
      // SqlTypes.DATETIME
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(),
          offsetDateTime,
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          localDateTime);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(),
          localDateTime,
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          localDateTime);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(),
          DateTimeUtil.microsFromTimestamptz(offsetDateTime),
          Schema.FieldType.logicalType(SqlTypes.DATETIME),
          localDateTime);

      // Schema.FieldType.DATETIME
      DateTime dateTime = DateTime.parse(timestamp).withZone(DateTimeZone.UTC);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(), offsetDateTime, Schema.FieldType.DATETIME, dateTime);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(), localDateTime, Schema.FieldType.DATETIME, dateTime);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(),
          DateTimeUtil.microsFromTimestamptz(offsetDateTime),
          Schema.FieldType.DATETIME,
          dateTime);
      checkRecordValueToRowValue(
          Types.TimestampType.withZone(), timestamp, Schema.FieldType.DATETIME, dateTime);
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
        TypeAndMaxId ret =
            beamFieldTypeToIcebergFieldType(testCase.beamType, testCase.icebergFieldId);

        assertEquals(testCase.expectedMaxId, ret.maxId);
        checkEquals(testCase.expectedIcebergType, ret.type);
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
      // primitive types don't use the nested field ID
      List<BeamFieldTypeTestCase> primitives =
          Arrays.asList(
              new BeamFieldTypeTestCase(1, Schema.FieldType.BOOLEAN, 0, Types.BooleanType.get()),
              new BeamFieldTypeTestCase(3, Schema.FieldType.INT32, 2, Types.IntegerType.get()),
              new BeamFieldTypeTestCase(6, Schema.FieldType.INT64, 5, Types.LongType.get()),
              new BeamFieldTypeTestCase(10, Schema.FieldType.FLOAT, 9, Types.FloatType.get()),
              new BeamFieldTypeTestCase(7, Schema.FieldType.DOUBLE, 6, Types.DoubleType.get()),
              new BeamFieldTypeTestCase(11, Schema.FieldType.STRING, 10, Types.StringType.get()),
              new BeamFieldTypeTestCase(15, Schema.FieldType.BYTES, 14, Types.BinaryType.get()));

      checkTypes(primitives);
    }

    @Test
    public void testArrayBeamFieldTypeToIcebergFieldType() {
      // Iceberg's ListType reserves one nested ID for its element type
      List<BeamFieldTypeTestCase> listTypes =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.array(Schema.FieldType.BOOLEAN),
                  1,
                  Types.ListType.ofRequired(1, Types.BooleanType.get())),
              new BeamFieldTypeTestCase(
                  3,
                  Schema.FieldType.iterable(Schema.FieldType.INT32),
                  3,
                  Types.ListType.ofRequired(3, Types.IntegerType.get())),
              new BeamFieldTypeTestCase(
                  6,
                  Schema.FieldType.array(Schema.FieldType.INT64),
                  6,
                  Types.ListType.ofRequired(6, Types.LongType.get())),
              new BeamFieldTypeTestCase(
                  10,
                  Schema.FieldType.array(Schema.FieldType.FLOAT),
                  10,
                  Types.ListType.ofRequired(10, Types.FloatType.get())),
              new BeamFieldTypeTestCase(
                  7,
                  Schema.FieldType.iterable(Schema.FieldType.DOUBLE),
                  7,
                  Types.ListType.ofRequired(7, Types.DoubleType.get())),
              new BeamFieldTypeTestCase(
                  11,
                  Schema.FieldType.array(Schema.FieldType.STRING),
                  11,
                  Types.ListType.ofRequired(11, Types.StringType.get())),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.iterable(Schema.FieldType.BYTES),
                  15,
                  Types.ListType.ofRequired(15, Types.BinaryType.get())),
              new BeamFieldTypeTestCase(
                  23,
                  Schema.FieldType.array(
                      Schema.FieldType.array(Schema.FieldType.iterable(Schema.FieldType.STRING))),
                  25,
                  Types.ListType.ofRequired(
                      23,
                      Types.ListType.ofRequired(
                          24, Types.ListType.ofRequired(25, Types.StringType.get())))));

      checkTypes(listTypes);
    }

    @Test
    public void testStructBeamFieldTypeToIcebergFieldType() {
      // Iceberg sets one unique field ID for each nested type.
      List<BeamFieldTypeTestCase> listTypes =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.row(Schema.builder().addStringField("str").build()),
                  1,
                  Types.StructType.of(
                      Types.NestedField.required(1, "str", Types.StringType.get()))),
              new BeamFieldTypeTestCase(
                  3,
                  Schema.FieldType.row(Schema.builder().addInt32Field("int").build()),
                  3,
                  Types.StructType.of(
                      Types.NestedField.required(3, "int", Types.IntegerType.get()))),
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.row(BEAM_SCHEMA_PRIMITIVE),
                  10,
                  Types.StructType.of(ICEBERG_SCHEMA_PRIMITIVE.columns())),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.row(
                      Schema.builder()
                          .addArrayField("arr", Schema.FieldType.STRING)
                          .addNullableStringField("str")
                          .build()),
                  17,
                  Types.StructType.of(
                      Types.NestedField.required(
                          15, "arr", Types.ListType.ofRequired(17, Types.StringType.get())),
                      Types.NestedField.optional(16, "str", Types.StringType.get()))),
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
                  24,
                  Types.StructType.of(
                      Types.NestedField.required(
                          20,
                          "row",
                          Types.StructType.of(
                              Types.NestedField.required(
                                  22,
                                  "nested_row",
                                  Types.StructType.of(
                                      Types.NestedField.required(
                                          23, "str", Types.StringType.get()))))),
                      Types.NestedField.optional(
                          21,
                          "nullable_row",
                          Types.StructType.of(
                              Types.NestedField.required(24, "long", Types.LongType.get()))))));

      checkTypes(listTypes);
    }

    @Test
    public void testMapBeamFieldTypeToIcebergFieldType() {
      // Iceberg's MapType reserves two nested IDs. one for its key type and one for its value type.
      List<BeamFieldTypeTestCase> primitives =
          Arrays.asList(
              new BeamFieldTypeTestCase(
                  1,
                  Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32),
                  2,
                  Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.IntegerType.get())),
              new BeamFieldTypeTestCase(
                  6,
                  Schema.FieldType.map(
                      Schema.FieldType.FLOAT, Schema.FieldType.array(Schema.FieldType.STRING)),
                  8,
                  Types.MapType.ofRequired(
                      6,
                      7,
                      Types.FloatType.get(),
                      Types.ListType.ofRequired(8, Types.StringType.get()))),
              new BeamFieldTypeTestCase(
                  10,
                  Schema.FieldType.map(
                      Schema.FieldType.STRING,
                      Schema.FieldType.map(
                          Schema.FieldType.BOOLEAN,
                          Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.INT32))),
                  15,
                  Types.MapType.ofRequired(
                      10,
                      11,
                      Types.StringType.get(),
                      Types.MapType.ofRequired(
                          12,
                          13,
                          Types.BooleanType.get(),
                          Types.MapType.ofRequired(
                              14, 15, Types.StringType.get(), Types.IntegerType.get())))),
              new BeamFieldTypeTestCase(
                  15,
                  Schema.FieldType.map(
                      Schema.FieldType.row(Schema.builder().addStringField("str").build()),
                      Schema.FieldType.row(Schema.builder().addInt32Field("int").build())),
                  18,
                  Types.MapType.ofRequired(
                      15,
                      16,
                      Types.StructType.of(
                          Types.NestedField.required(17, "str", Types.StringType.get())),
                      Types.StructType.of(
                          Types.NestedField.required(18, "int", Types.IntegerType.get())))));

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
            .addLogicalTypeField("datetime", SqlTypes.DATETIME)
            .addLogicalTypeField("time", SqlTypes.TIME)
            .addLogicalTypeField("date", SqlTypes.DATE)
            .build();

    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_PRIMITIVE =
        new org.apache.iceberg.Schema(
            required(1, "int", Types.IntegerType.get()),
            required(2, "float", Types.FloatType.get()),
            optional(3, "double", Types.DoubleType.get()),
            required(4, "long", Types.LongType.get()),
            optional(5, "str", Types.StringType.get()),
            optional(6, "bool", Types.BooleanType.get()),
            required(7, "bytes", Types.BinaryType.get()),
            required(8, "datetime", Types.TimestampType.withoutZone()),
            required(9, "time", Types.TimeType.get()),
            required(10, "date", Types.DateType.get()));

    @Test
    public void testPrimitiveBeamSchemaToIcebergSchema() {
      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(BEAM_SCHEMA_PRIMITIVE);

      assertTrue(convertedIcebergSchema.sameSchema(ICEBERG_SCHEMA_PRIMITIVE));
    }

    @Test
    public void testPrimitiveIcebergSchemaToBeamSchema() {
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(ICEBERG_SCHEMA_PRIMITIVE);

      assertEquals(BEAM_SCHEMA_PRIMITIVE, convertedBeamSchema);
    }

    @Test
    public void testDatetimeBeamToIceberg() {
      // Support converting both DATETIME types to Iceberg TIMESTAMP type
      Schema beamSchema =
          Schema.builder()
              .addDateTimeField("datetime")
              .addLogicalTypeField("datetime_tz", SqlTypes.DATETIME)
              .build();

      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(
              required(1, "datetime", Types.TimestampType.withZone()),
              required(2, "datetime_tz", Types.TimestampType.withoutZone()));

      org.apache.iceberg.Schema convertedIcebergSchema =
          IcebergUtils.beamSchemaToIcebergSchema(beamSchema);

      assertTrue(convertedIcebergSchema.sameSchema(icebergSchema));
    }

    @Test
    public void testTimestampIcebergToBeam() {
      // Both timestamp and timestamptz types will convert to Beam SqlTypes.DATETIME
      org.apache.iceberg.Schema icebergSchema =
          new org.apache.iceberg.Schema(
              required(1, "timestamp", Types.TimestampType.withoutZone()),
              required(2, "timestamp_tz", Types.TimestampType.withZone()));

      Schema expectedBeamSchema =
          Schema.builder()
              .addLogicalTypeField("timestamp", SqlTypes.DATETIME)
              .addDateTimeField("timestamp_tz")
              .build();
      Schema convertedBeamSchema = IcebergUtils.icebergSchemaToBeamSchema(icebergSchema);

      assertEquals(expectedBeamSchema, convertedBeamSchema);
    }

    static final Schema BEAM_SCHEMA_LIST =
        Schema.builder()
            .addIterableField("arr_str", Schema.FieldType.STRING)
            .addIterableField("arr_int", Schema.FieldType.INT32)
            .addIterableField("arr_bool", Schema.FieldType.BOOLEAN)
            .build();
    static final org.apache.iceberg.Schema ICEBERG_SCHEMA_LIST =
        new org.apache.iceberg.Schema(
            required(1, "arr_str", Types.ListType.ofRequired(4, Types.StringType.get())),
            required(2, "arr_int", Types.ListType.ofRequired(5, Types.IntegerType.get())),
            required(3, "arr_bool", Types.ListType.ofRequired(6, Types.BooleanType.get())));

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
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())),
            optional(
                2,
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
                    required(3, "str", Types.StringType.get()),
                    optional(4, "int", Types.IntegerType.get()),
                    required(5, "long", Types.LongType.get()))),
            optional(
                2,
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

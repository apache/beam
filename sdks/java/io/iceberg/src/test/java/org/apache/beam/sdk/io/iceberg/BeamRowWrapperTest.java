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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.Test;

public class BeamRowWrapperTest {
  private static final Schema NESTED_BEAM_SCHEMA =
      Schema.builder().addInt32Field("nested_int").build();
  private static final Schema BEAM_SCHEMA =
      Schema.builder()
          .addByteField("byte_field")
          .addInt16Field("int16_field")
          .addStringField("string_field")
          .addByteArrayField("bytes_field")
          .addByteArrayField("uuid_field")
          .addDecimalField("decimal_field")
          .addDateTimeField("datetime_field")
          .addLogicalTypeField("micros_instant_field", new MicrosInstant())
          .addLogicalTypeField("date_time_field", new DateTime())
          .addLogicalTypeField("date_field", new Date())
          .addLogicalTypeField("time_field", new Time())
          .addLogicalTypeField("fixed_numeric_field", FixedPrecisionNumeric.of(10, 2))
          .addRowField("row_field", NESTED_BEAM_SCHEMA)
          .addInt32Field("pass_through_field")
          .build();
  private static final Types.StructType ICEBERG_STRUCT =
      Types.StructType.of(
          Types.NestedField.required(1, "byte_field", Types.IntegerType.get()),
          Types.NestedField.required(2, "int16_field", Types.IntegerType.get()),
          Types.NestedField.required(3, "string_field", Types.StringType.get()),
          Types.NestedField.required(4, "bytes_field", Types.BinaryType.get()),
          Types.NestedField.required(5, "uuid_field", Types.UUIDType.get()),
          Types.NestedField.required(6, "decimal_field", Types.DecimalType.of(10, 2)),
          Types.NestedField.required(7, "datetime_field", Types.TimestampType.withZone()),
          Types.NestedField.required(8, "micros_instant_field", Types.TimestampType.withZone()),
          Types.NestedField.required(9, "date_time_field", Types.TimestampType.withoutZone()),
          Types.NestedField.required(10, "date_field", Types.DateType.get()),
          Types.NestedField.required(11, "time_field", Types.TimeType.get()),
          Types.NestedField.required(12, "fixed_numeric_field", Types.DecimalType.of(10, 2)),
          Types.NestedField.required(
              13,
              "row_field",
              Types.StructType.of(
                  Types.NestedField.required(1, "nested_int", Types.IntegerType.get()))),
          Types.NestedField.required(14, "pass_through_field", Types.IntegerType.get()));
  private static final UUID TEST_UUID = UUID.randomUUID();
  private static final Row NESTED_ROW = Row.withSchema(NESTED_BEAM_SCHEMA).addValue(999).build();
  private static final Row ROW =
      Row.withSchema(BEAM_SCHEMA)
          .addValues(
              (byte) 42,
              (short) 123,
              "testString",
              new byte[] {0x01, 0x02, 0x03},
              ByteBuffer.allocate(16)
                  .putLong(TEST_UUID.getMostSignificantBits())
                  .putLong(TEST_UUID.getLeastSignificantBits())
                  .array(),
              new BigDecimal("123.45"),
              org.joda.time.Instant.now(),
              Instant.now(),
              LocalDateTime.now(ZoneId.systemDefault()),
              LocalDate.now(),
              LocalTime.now(),
              new BigDecimal("567.89"),
              NESTED_ROW,
              888)
          .build();
  private static final BeamRowWrapper WRAPPER =
      new BeamRowWrapper(BEAM_SCHEMA, ICEBERG_STRUCT).wrap(ROW);

  @Test
  public void testSize() {
    assertEquals("Size should match the schema field count", 14, WRAPPER.size());
  }

  @Test
  public void testUnsupportedSetThrowsException() {
    assertThrows(UnsupportedOperationException.class, () -> WRAPPER.set(0, "test"));
  }

  @Test
  public void testNullRowHandling() {
    BeamRowWrapper emptyWrapper = new BeamRowWrapper(BEAM_SCHEMA, ICEBERG_STRUCT);
    assertNull(
        "Should return null if the underlying row is null", emptyWrapper.get(0, Object.class));
  }

  @Test
  public void testNullFieldHandling() {
    Schema nullableSchema = Schema.builder().addNullableStringField("nullable_str").build();
    Types.StructType nullableIcebergType =
        Types.StructType.of(Types.NestedField.optional(1, "nullable_str", Types.StringType.get()));

    Row nullRow = Row.withSchema(nullableSchema).addValue(null).build();
    BeamRowWrapper nullableWrapper =
        new BeamRowWrapper(nullableSchema, nullableIcebergType).wrap(nullRow);

    assertNull("Should return null for a null field value", nullableWrapper.get(0, String.class));
  }

  // --- Type Conversion Tests ---

  @Test
  public void testByteConversion() {
    assertEquals(ROW.getByte(0), WRAPPER.get(0, Byte.class));
  }

  @Test
  public void testInt16Conversion() {
    assertEquals(ROW.getInt16(1), WRAPPER.get(1, Short.class));
  }

  @Test
  public void testStringConversion() {
    assertEquals(ROW.getString(2), WRAPPER.get(2, String.class));
  }

  @Test
  public void testBytesToByteBufferConversion() {
    assertEquals(ByteBuffer.wrap(ROW.getBytes(3)), WRAPPER.get(3, ByteBuffer.class));
  }

  @Test
  public void testBytesToUUIDConversion() {
    assertEquals(UUIDUtil.convert(ROW.getBytes(4)), WRAPPER.get(4, UUID.class));
  }

  @Test
  public void testDecimalConversion() {
    assertEquals(ROW.getDecimal(5), WRAPPER.get(5, BigDecimal.class));
  }

  @Test
  public void testDateTimeConversion() {
    long expectedJodaMicros = TimeUnit.MILLISECONDS.toMicros(ROW.getDateTime(6).getMillis());
    assertEquals(expectedJodaMicros, (long) WRAPPER.get(6, Long.class));
  }

  @Test
  public void testMicrosInstantLogicalTypeConversion() {
    Instant javaInstant = ROW.getLogicalTypeValue(7, Instant.class);
    long expectedMicrosInstant =
        TimeUnit.SECONDS.toMicros(javaInstant.getEpochSecond()) + javaInstant.getNano() / 1000;
    assertEquals(expectedMicrosInstant, (long) WRAPPER.get(7, Long.class));
  }

  @Test
  public void testDateTimeLogicalTypeConversion() {
    long expectedDateTime =
        DateTimeUtil.microsFromTimestamp(ROW.getLogicalTypeValue(8, LocalDateTime.class));
    assertEquals(expectedDateTime, (long) WRAPPER.get(8, Long.class));
  }

  @Test
  public void testDateLogicalTypeConversion() {
    int expectedDate = DateTimeUtil.daysFromDate(ROW.getLogicalTypeValue(9, LocalDate.class));
    assertEquals(expectedDate, (int) WRAPPER.get(9, Integer.class));
  }

  @Test
  public void testTimeLogicalTypeConversion() {
    long expectedTime = DateTimeUtil.microsFromTime(ROW.getLogicalTypeValue(10, LocalTime.class));
    assertEquals(expectedTime, (long) WRAPPER.get(10, Long.class));
  }

  @Test
  public void testFixedPrecisionNumericLogicalTypeConversion() {
    assertEquals(
        ROW.getLogicalTypeValue(11, FixedPrecisionNumeric.class),
        WRAPPER.get(11, BigDecimal.class));
  }

  @Test
  public void testNestedRowConversion() {
    StructLike nestedWrapperResult = WRAPPER.get(12, StructLike.class);
    assertTrue(
        "Should return a nested BeamRowWrapper", nestedWrapperResult instanceof BeamRowWrapper);
    assertEquals(999, (int) nestedWrapperResult.get(0, Integer.class));
  }

  @Test
  public void testPassThroughFallbackConversion() {
    // Tests the 'default' case in the switch statement
    assertEquals(ROW.getInt32(13), WRAPPER.get(13, Integer.class));
  }
}

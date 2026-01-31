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
package org.apache.beam.sdk.schemas.logicaltypes;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType.Value;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Unit tests for logical types. */
public class LogicalTypesTest {
  @Test
  public void testEnumeration() {
    Map<String, Integer> enumMap = ImmutableMap.of("FIRST", 1, "SECOND", 2);
    EnumerationType enumeration = EnumerationType.create(enumMap);
    assertEquals(enumeration.valueOf(1), enumeration.valueOf("FIRST"));
    assertEquals(enumeration.valueOf(2), enumeration.valueOf("SECOND"));
    assertEquals("FIRST", enumeration.toString(enumeration.valueOf(1)));
    assertEquals(1, enumeration.valueOf("FIRST").getValue());
    assertEquals("SECOND", enumeration.toString(enumeration.valueOf(2)));
    assertEquals(2, enumeration.valueOf("SECOND").getValue());

    Schema schema =
        Schema.builder().addLogicalTypeField("enum", EnumerationType.create(enumMap)).build();
    Row row1 = Row.withSchema(schema).addValue(enumeration.valueOf(1)).build();
    Row row2 = Row.withSchema(schema).addValue(enumeration.valueOf("FIRST")).build();
    assertEquals(row1, row2);
    assertEquals(1, row1.getLogicalTypeValue(0, EnumerationType.Value.class).getValue());

    Row row3 = Row.withSchema(schema).addValue(enumeration.valueOf(2)).build();
    Row row4 = Row.withSchema(schema).addValue(enumeration.valueOf("SECOND")).build();
    assertEquals(row3, row4);
    assertEquals(2, row3.getLogicalTypeValue(0, EnumerationType.Value.class).getValue());
  }

  @Test
  public void testOneOf() {
    OneOfType oneOf =
        OneOfType.create(Field.of("string", FieldType.STRING), Field.of("int32", FieldType.INT32));
    Schema schema = Schema.builder().addLogicalTypeField("union", oneOf).build();

    Row stringOneOf =
        Row.withSchema(schema).addValue(oneOf.createValue("string", "stringValue")).build();
    Value union = stringOneOf.getLogicalTypeValue(0, OneOfType.Value.class);
    assertEquals("string", oneOf.getCaseEnumType().toString(union.getCaseType()));
    assertEquals("stringValue", union.getValue());

    Row intOneOf = Row.withSchema(schema).addValue(oneOf.createValue("int32", 42)).build();
    union = intOneOf.getLogicalTypeValue(0, OneOfType.Value.class);
    assertEquals("int32", oneOf.getCaseEnumType().toString(union.getCaseType()));
    assertEquals(42, (int) union.getValue());

    // Validate schema equality.
    OneOfType oneOf2 =
        OneOfType.create(Field.of("string", FieldType.STRING), Field.of("int32", FieldType.INT32));
    assertEquals(oneOf.getOneOfSchema(), oneOf2.getOneOfSchema());
    Schema schema2 = Schema.builder().addLogicalTypeField("union", oneOf2).build();
    assertEquals(schema, schema2);
    Row stringOneOf2 =
        Row.withSchema(schema2).addValue(oneOf.createValue("string", "stringValue")).build();
    assertEquals(stringOneOf, stringOneOf2);
  }

  @Test
  public void testNanosInstant() {
    Schema rowSchema = new NanosInstant().getBaseType().getRowSchema();
    Instant now = Instant.now();
    Row nowAsRow = Row.withSchema(rowSchema).addValues(now.getEpochSecond(), now.getNano()).build();

    Schema schema = Schema.builder().addLogicalTypeField("now", new NanosInstant()).build();
    Row row = Row.withSchema(schema).addValues(now).build();
    assertEquals(now, row.getLogicalTypeValue(0, NanosInstant.class));
    assertEquals(nowAsRow, row.getBaseValue(0, Row.class));
  }

  @Test
  public void testNanosDuration() {
    Schema rowSchema = new NanosInstant().getBaseType().getRowSchema();
    Duration duration = Duration.ofSeconds(123, 42);
    Row durationAsRow = Row.withSchema(rowSchema).addValues(123L, 42).build();

    Schema schema = Schema.builder().addLogicalTypeField("duration", new NanosDuration()).build();
    Row row = Row.withSchema(schema).addValues(duration).build();
    assertEquals(duration, row.getLogicalTypeValue(0, NanosDuration.class));
    assertEquals(durationAsRow, row.getBaseValue(0, Row.class));
  }

  @Test
  public void testUuid() {
    Schema rowSchema = new UuidLogicalType().getBaseType().getRowSchema();
    UUID uuid = UUID.randomUUID();
    Row uuidAsRow =
        Row.withSchema(rowSchema)
            .addValues(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits())
            .build();

    Schema schema = Schema.builder().addLogicalTypeField("uuid", new UuidLogicalType()).build();
    Row row = Row.withSchema(schema).addValues(uuid).build();
    assertEquals(uuid, row.getLogicalTypeValue(0, UUID.class));
    assertEquals(uuidAsRow, row.getBaseValue(0, Row.class));
  }

  @Test
  public void testSchema() {
    Schema schemaValue =
        Schema.of(
            Field.of("fieldOne", FieldType.BOOLEAN),
            Field.of("nested", FieldType.logicalType(new SchemaLogicalType())));

    Schema schema = Schema.builder().addLogicalTypeField("schema", new SchemaLogicalType()).build();
    Row row = Row.withSchema(schema).addValues(schemaValue).build();
    assertEquals(schemaValue, row.getLogicalTypeValue(0, Schema.class));

    // Check base type conversion.
    assertEquals(
        schemaValue,
        new SchemaLogicalType().toInputType(new SchemaLogicalType().toBaseType(schemaValue)));
  }

  @Test
  public void testFixedPrecisionNumeric() {
    final int precision = 10;
    final int scale = 2;

    Schema argumentSchema =
        Schema.builder().addInt32Field("precision").addInt32Field("scale").build();

    // invalid schema
    final Schema invalidArgumentSchema =
        Schema.builder().addInt32Field("invalid").addInt32Field("schema").build();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            FixedPrecisionNumeric.of(
                Row.withSchema(invalidArgumentSchema).addValues(precision, scale).build()));

    // FixedPrecisionNumeric specified precision and scale
    FixedPrecisionNumeric numeric =
        FixedPrecisionNumeric.of(
            Row.withSchema(argumentSchema).addValues(precision, scale).build());
    Schema schema = Schema.builder().addLogicalTypeField("decimal", numeric).build();

    // check argument valid case
    BigDecimal decimal = BigDecimal.valueOf(1_000_000_001, scale);
    Row row = Row.withSchema(schema).addValues(decimal).build();
    assertEquals(decimal, row.getLogicalTypeValue(0, BigDecimal.class));

    // check argument invalid case (value out of precision limit)
    decimal = BigDecimal.valueOf(100_000_000_001L, scale);
    assertThrows(IllegalArgumentException.class, Row.withSchema(schema).addValues(decimal)::build);

    // FixedPrecisionNumeric without specifying precision
    numeric = FixedPrecisionNumeric.of(scale);
    schema = Schema.builder().addLogicalTypeField("decimal", numeric).build();

    // check argument always valid  (precision limit unspecified)
    decimal = BigDecimal.valueOf(1_000_000_001, scale);
    row = Row.withSchema(schema).addValues(decimal).build();
    assertEquals(decimal, row.getLogicalTypeValue(0, BigDecimal.class));

    decimal = BigDecimal.valueOf(100_000_000_001L, scale);
    row = Row.withSchema(schema).addValues(decimal).build();
    assertEquals(decimal, row.getLogicalTypeValue(0, BigDecimal.class));
  }

  @Test
  public void testFixedBytes() {
    FixedBytes fixedBytes = FixedBytes.of(5);

    // check argument valid case, with padding
    byte[] resultBytes = fixedBytes.toInputType(new byte[] {0x1, 0x2, 0x3});
    assertArrayEquals(new byte[] {0x1, 0x2, 0x3, 0x0, 0x0}, resultBytes);

    // check argument invalid case
    assertThrows(
        IllegalArgumentException.class,
        () -> fixedBytes.toInputType(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6}));
  }

  @Test
  public void testVariableBytes() {
    VariableBytes variableBytes = VariableBytes.of(5);

    // check argument valid case, no padding
    byte[] resultBytes = variableBytes.toInputType(new byte[] {0x1, 0x2, 0x3});
    assertArrayEquals(new byte[] {0x1, 0x2, 0x3}, resultBytes);

    // check argument invalid case
    assertThrows(
        IllegalArgumentException.class,
        () -> variableBytes.toInputType(new byte[] {0x1, 0x2, 0x3, 0x4, 0x5, 0x6}));
  }

  @Test
  public void testFixedString() {
    FixedString fixedString = FixedString.of(5);

    // check argument valid case, with padding
    String resultString = fixedString.toInputType("123");
    assertEquals("123  ", resultString);

    // check argument invalid case
    assertThrows(IllegalArgumentException.class, () -> fixedString.toInputType("123456"));
  }

  @Test
  public void testVariableString() {
    VariableString varibaleString = VariableString.of(5);

    // check argument valid case, no padding
    String resultString = varibaleString.toInputType("123");
    assertEquals("123", resultString);

    // check argument invalid case
    assertThrows(IllegalArgumentException.class, () -> varibaleString.toInputType("123456"));
  }

  @Test
  public void testTimestampMillis() {
    Timestamp timestampType = Timestamp.MILLIS;
    assertEquals(3, timestampType.getArgument().intValue());

    // Positive timestamp with millisecond precision
    Instant instant = Instant.ofEpochSecond(1609459200, 123_000_000); // 2021-01-01 00:00:00.123 UTC
    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row row = Row.withSchema(schema).addValue(instant).build();

    assertEquals(instant, row.getLogicalTypeValue(0, Instant.class));

    // Check base type conversion
    Row baseRow = row.getBaseValue(0, Row.class);
    assertEquals(1609459200L, baseRow.getInt64("seconds").longValue());
    assertEquals((short) 123, baseRow.getInt16("subseconds").shortValue());
  }

  @Test
  public void testTimestampMicros() {
    Timestamp timestampType = Timestamp.MICROS;
    assertEquals(6, timestampType.getArgument().intValue());

    // Positive timestamp with microsecond precision
    Instant instant =
        Instant.ofEpochSecond(1609459200, 123_456_000); // 2021-01-01 00:00:00.123456 UTC
    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row row = Row.withSchema(schema).addValue(instant).build();

    assertEquals(instant, row.getLogicalTypeValue(0, Instant.class));

    // Check base type conversion uses INT32 for micros
    Row baseRow = row.getBaseValue(0, Row.class);
    assertEquals(1609459200L, baseRow.getInt64("seconds").longValue());
    assertEquals(123_456, baseRow.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampNanos() {
    Timestamp timestampType = Timestamp.NANOS;
    assertEquals(9, timestampType.getArgument().intValue());

    // Positive timestamp with nanosecond precision
    Instant instant =
        Instant.ofEpochSecond(1609459200, 123_456_789); // 2021-01-01 00:00:00.123456789 UTC
    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row row = Row.withSchema(schema).addValue(instant).build();

    assertEquals(instant, row.getLogicalTypeValue(0, Instant.class));

    // Check base type conversion uses INT32 for nanos
    Row baseRow = row.getBaseValue(0, Row.class);
    assertEquals(1609459200L, baseRow.getInt64("seconds").longValue());
    assertEquals(123_456_789, baseRow.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampNegative() {
    Timestamp timestampType = Timestamp.MICROS;

    // Negative timestamp: -1.5 seconds before epoch
    // Should be represented as {seconds: -2, subseconds: 500000}
    Instant instant = Instant.ofEpochSecond(-2, 500_000_000);
    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row row = Row.withSchema(schema).addValue(instant).build();

    assertEquals(instant, row.getLogicalTypeValue(0, Instant.class));

    // Verify the internal representation
    Row baseRow = row.getBaseValue(0, Row.class);
    assertEquals(-2L, baseRow.getInt64("seconds").longValue());
    assertEquals(500_000, baseRow.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampZero() {
    Timestamp timestampType = Timestamp.MICROS;

    // Epoch timestamp
    Instant instant = Instant.ofEpochSecond(0, 0);
    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row row = Row.withSchema(schema).addValue(instant).build();

    assertEquals(instant, row.getLogicalTypeValue(0, Instant.class));

    Row baseRow = row.getBaseValue(0, Row.class);
    assertEquals(0L, baseRow.getInt64("seconds").longValue());
    assertEquals(0, baseRow.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampPrecisionBoundary() {
    // Test the boundary between INT16 and INT32 representation
    Timestamp precision4 = Timestamp.of(4);
    Timestamp precision5 = Timestamp.of(5);

    // Precision 4 should use INT16
    Instant instant4 = Instant.ofEpochSecond(100, 999_900_000);
    Schema schema4 = Schema.builder().addLogicalTypeField("ts", precision4).build();
    Row row4 = Row.withSchema(schema4).addValue(instant4).build();
    Row baseRow4 = row4.getBaseValue(0, Row.class);
    assertEquals((short) 999_9, baseRow4.getInt16("subseconds").shortValue());

    // Precision 5 should use INT32
    Instant instant5 = Instant.ofEpochSecond(100, 999_990_000);
    Schema schema5 = Schema.builder().addLogicalTypeField("ts", precision5).build();
    Row row5 = Row.withSchema(schema5).addValue(instant5).build();
    Row baseRow5 = row5.getBaseValue(0, Row.class);
    assertEquals(999_99, baseRow5.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampDataLossDetection() {
    Timestamp millisType = Timestamp.MILLIS;

    // Try to store microsecond-precision instant in millis logical type
    Instant instant = Instant.ofEpochSecond(100, 123_456_000); // Has microseconds
    Schema schema = Schema.builder().addLogicalTypeField("ts", millisType).build();

    // Should throw because 123_456_000 nanos is not divisible by 1_000_000
    assertThrows(
        IllegalStateException.class, () -> Row.withSchema(schema).addValue(instant).build());
  }

  @Test
  public void testTimestampDataLossDetectionNanos() {
    Timestamp microsType = Timestamp.MICROS;

    // Try to store nanosecond-precision instant in micros logical type
    Instant instant = Instant.ofEpochSecond(100, 123_456_789); // Has nanoseconds
    Schema schema = Schema.builder().addLogicalTypeField("ts", microsType).build();

    // Should throw because 123_456_789 nanos is not divisible by 1_000
    assertThrows(
        IllegalStateException.class, () -> Row.withSchema(schema).addValue(instant).build());
  }

  @Test
  public void testTimestampInvalidPrecision() {
    assertThrows(IllegalArgumentException.class, () -> Timestamp.of(-1));
    assertThrows(IllegalArgumentException.class, () -> Timestamp.of(10));
  }

  @Test
  public void testTimestampRoundTrip() {
    // Test that we can round-trip through base type for all precisions
    for (int precision = 0; precision <= 9; precision++) {
      Timestamp timestampType = Timestamp.of(precision);

      long nanos = 123_456_789;
      int scalingFactor = (int) Math.pow(10, 9 - precision);
      nanos = (nanos / scalingFactor) * scalingFactor;

      Instant original = Instant.ofEpochSecond(1609459200, nanos);

      Row baseRow = timestampType.toBaseType(original);
      Instant roundTripped = timestampType.toInputType(baseRow);

      assertEquals(original, roundTripped);
    }
  }

  @Test
  public void testTimestampNegativeRoundTrip() {
    Timestamp timestampType = Timestamp.MICROS;

    Instant original = Instant.ofEpochSecond(-100, 500_000_000);
    Row baseRow = timestampType.toBaseType(original);
    Instant roundTripped = timestampType.toInputType(baseRow);

    assertEquals(original, roundTripped);

    assertEquals(-100L, baseRow.getInt64("seconds").longValue());
    assertEquals(500_000, baseRow.getInt32("subseconds").intValue());
  }

  @Test
  public void testTimestampArgumentType() {
    Timestamp timestampType = Timestamp.MICROS;

    // Check argument type is INT32
    assertEquals(FieldType.INT32, timestampType.getArgumentType());

    // Check argument value
    assertEquals(Integer.valueOf(6), timestampType.getArgument());
  }

  @Test
  public void testTimestampBaseTypeStructure() {
    Timestamp millisType = Timestamp.MILLIS;
    Timestamp microsType = Timestamp.MICROS;

    // Check base type is a row schema
    assertEquals(Schema.TypeName.ROW, millisType.getBaseType().getTypeName());
    assertEquals(Schema.TypeName.ROW, microsType.getBaseType().getTypeName());

    // Check millis uses INT16 for subseconds (precision < 5)
    Schema millisSchema = millisType.getBaseType().getRowSchema();
    assertEquals(2, millisSchema.getFieldCount());
    assertEquals("seconds", millisSchema.getField(0).getName());
    assertEquals(FieldType.INT64, millisSchema.getField(0).getType());
    assertEquals("subseconds", millisSchema.getField(1).getName());
    assertEquals(FieldType.INT16, millisSchema.getField(1).getType());

    // Check micros uses INT32 for subseconds (precision >= 5)
    Schema microsSchema = microsType.getBaseType().getRowSchema();
    assertEquals(2, microsSchema.getFieldCount());
    assertEquals("seconds", microsSchema.getField(0).getName());
    assertEquals(FieldType.INT64, microsSchema.getField(0).getType());
    assertEquals("subseconds", microsSchema.getField(1).getName());
    assertEquals(FieldType.INT32, microsSchema.getField(1).getType());
  }

  @Test
  public void testTimestampCorruptedDataNegativeSubseconds() {
    Timestamp timestampType = Timestamp.MICROS;
    Schema baseSchema = timestampType.getBaseType().getRowSchema();

    // Create a corrupted row with negative subseconds
    Row corruptedRow =
        Row.withSchema(baseSchema)
            .addValue(-1L) // seconds
            .addValue(-500_000) // subseconds
            .build();

    assertThrows(IllegalArgumentException.class, () -> timestampType.toInputType(corruptedRow));
  }

  @Test
  public void testTimestampCorruptedDataOutOfRangeSubseconds() {
    Timestamp millisType = Timestamp.MILLIS;
    Schema baseSchema = millisType.getBaseType().getRowSchema();

    // Create a corrupted row with subseconds > 999 for millis precision
    Row corruptedRow =
        Row.withSchema(baseSchema)
            .addValue(100L) // seconds
            .addValue((short) 1000) // subseconds
            .build();

    // Should throw when trying to convert back to Instant
    assertThrows(IllegalArgumentException.class, () -> millisType.toInputType(corruptedRow));
  }

  @Test
  public void testTimestampExtremeValues() {
    Timestamp timestampType = Timestamp.MICROS;
    int scalingFactor = 1000; // For micros

    // Round MAX/MIN to microsecond boundaries
    Instant nearMin = Instant.MIN.plusSeconds(1000);
    long nanos = (long) (nearMin.getNano() / scalingFactor) * scalingFactor;
    nearMin = Instant.ofEpochSecond(nearMin.getEpochSecond(), nanos);

    Schema schema = Schema.builder().addLogicalTypeField("ts", timestampType).build();
    Row minRow = Row.withSchema(schema).addValue(nearMin).build();
    assertEquals(nearMin, minRow.getLogicalTypeValue(0, Instant.class));

    // Same for MAX
    Instant nearMax = Instant.MAX.minusSeconds(1000);
    nanos = (long) (nearMax.getNano() / scalingFactor) * scalingFactor;
    nearMax = Instant.ofEpochSecond(nearMax.getEpochSecond(), nanos);

    Row maxRow = Row.withSchema(schema).addValue(nearMax).build();
    assertEquals(nearMax, maxRow.getLogicalTypeValue(0, Instant.class));
  }
}

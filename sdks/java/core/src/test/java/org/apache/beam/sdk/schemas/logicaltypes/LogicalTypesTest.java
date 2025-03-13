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
}

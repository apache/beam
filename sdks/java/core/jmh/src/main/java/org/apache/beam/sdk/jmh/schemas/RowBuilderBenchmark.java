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
package org.apache.beam.sdk.jmh.schemas;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.SchemaVerification;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

/**
 * Microbenchmarks for {@link Row.Builder}, {@link Row.FieldValueBuilder}, {@link
 * SchemaVerification}, {@link Row} utility transforms, and {@link FieldAccessDescriptorParser}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class RowBuilderBenchmark {

  /** Pre-initialized schemas, payloads, boxed values, and dynamic string paths for JMH. */
  @State(Scope.Thread)
  public static class RowState {
    Schema flatSchema;
    Schema nullableSchema;
    Schema complexSchema;
    Schema bytesSchema;

    Object[] flatValues;
    Row flatRow;
    Row childRow;
    Row complexRow;
    Row deepRow;
    Row camelRow;
    List<String> tags;
    Map<String, Long> metrics;
    java.time.Instant javaInstant;
    BigDecimal decimal;
    byte[] payload;
    ByteBuffer offsetZeroSlice;
    Instant jodaInstant;
    Map<String, Object> flatNamedValues;

    // Pre-boxed values so named/id builder benchmarks avoid spurious autoboxing noise.
    Integer intVal;
    Long longVal;
    Double doubleVal;
    Float floatVal;
    String stringVal;
    Boolean boolVal;
    Byte byteVal;
    Short shortVal;
    Long updatedChildId;

    // Non-final string fields prevent JDK 9+ C2 @Stable String.value constant folding.
    String fInt32;
    String fInt64;
    String fDouble;
    String fFloat;
    String fString;
    String fBool;
    String fByte;
    String fInt16;
    String fBytes;
    String fDatetime;
    String nestedPath;
    String deepNestedPath;
    String updatedString;
    Schema.FieldType verifyFieldType;
    String verifyFieldName;
    String simpleFieldPath;
    String dottedFieldPath;
    String dottedWildcardPath;

    /** Populates schemas and sample inputs once per trial thread. */
    @Setup
    public void setUp() {
      flatSchema =
          Schema.builder()
              .addInt32Field("f_int32")
              .addInt64Field("f_int64")
              .addDoubleField("f_double")
              .addFloatField("f_float")
              .addStringField("f_string")
              .addBooleanField("f_bool")
              .addByteField("f_byte")
              .addInt16Field("f_int16")
              .addByteArrayField("f_bytes")
              .addDateTimeField("f_datetime")
              .build();

      nullableSchema =
          Schema.builder()
              .addNullableInt32Field("f_int32")
              .addNullableInt64Field("f_int64")
              .addNullableDoubleField("f_double")
              .addNullableStringField("f_string")
              .addNullableBooleanField("f_bool")
              .build();

      Schema nestedChildSchema =
          Schema.builder().addStringField("child_name").addInt64Field("child_id").build();

      complexSchema =
          Schema.builder()
              .addStringField("id")
              .addRowField("child", nestedChildSchema)
              .addArrayField("tags", Schema.FieldType.STRING)
              .addMapField("metrics", Schema.FieldType.STRING, Schema.FieldType.INT64)
              .addLogicalTypeField("ts_micros", new MicrosInstant())
              .addDecimalField("amount")
              .addNullableStringField("optional_note")
              .build();

      Schema deepSchema =
          Schema.builder().addStringField("top_id").addRowField("outer", complexSchema).build();

      Schema camelSchema =
          Schema.builder()
              .addStringField("userId")
              .addInt64Field("eventTimestamp")
              .addStringField("sourceName")
              .addBooleanField("isActive")
              .build();

      bytesSchema = Schema.builder().addByteArrayField("bytes").build();

      payload = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
      offsetZeroSlice = ByteBuffer.wrap(payload, 0, 4).slice();
      jodaInstant = new Instant(1_700_000_000_123L);
      intVal = 42;
      longVal = 1234567890123L;
      doubleVal = 12.5;
      floatVal = 1.25f;
      stringVal = "beam-row-value";
      boolVal = Boolean.TRUE;
      byteVal = (byte) 7;
      shortVal = (short) 99;
      updatedChildId = 200L;

      fInt32 = "f_int32";
      fInt64 = "f_int64";
      fDouble = "f_double";
      fFloat = "f_float";
      fString = "f_string";
      fBool = "f_bool";
      fByte = "f_byte";
      fInt16 = "f_int16";
      fBytes = "f_bytes";
      fDatetime = "f_datetime";
      nestedPath = "child.child_id";
      deepNestedPath = "outer.child.child_id";
      updatedString = "updated";
      verifyFieldType = Schema.FieldType.STRING;
      verifyFieldName = "f_str";
      simpleFieldPath = "f_string";
      dottedFieldPath = "outer.child.child_id";
      dottedWildcardPath = "outer.child.*";

      flatValues =
          new Object[] {
            intVal,
            longVal,
            doubleVal,
            floatVal,
            stringVal,
            boolVal,
            byteVal,
            shortVal,
            payload,
            jodaInstant
          };

      flatNamedValues = new HashMap<>();
      flatNamedValues.put(fInt32, intVal);
      flatNamedValues.put(fInt64, longVal);
      flatNamedValues.put(fDouble, doubleVal);
      flatNamedValues.put(fFloat, floatVal);
      flatNamedValues.put(fString, stringVal);
      flatNamedValues.put(fBool, boolVal);
      flatNamedValues.put(fByte, byteVal);
      flatNamedValues.put(fInt16, shortVal);
      flatNamedValues.put(fBytes, payload);
      flatNamedValues.put(fDatetime, jodaInstant);

      flatRow = Row.withSchema(flatSchema).addValues(flatValues).build();
      childRow = Row.withSchema(nestedChildSchema).addValues("child-a", 100L).build();
      tags = Arrays.asList("alpha", "beta", "gamma");
      metrics = new HashMap<>();
      metrics.put("cpu", 10L);
      metrics.put("mem", 20L);
      // Use non-zero sub-second millis so MicrosInstant exercises non-cached microsecond values.
      javaInstant = java.time.Instant.ofEpochMilli(1_700_000_000_123L);
      decimal = new BigDecimal("12345.67");
      complexRow =
          Row.withSchema(complexSchema)
              .addValue("row-1")
              .addValue(childRow)
              .addValue(tags)
              .addValue(metrics)
              .addValue(javaInstant)
              .addValue(decimal)
              .addValue(null)
              .build();
      deepRow = Row.withSchema(deepSchema).addValues("top-1", complexRow).build();
      camelRow =
          Row.withSchema(camelSchema).addValues("user-1", 12345L, "kafka", Boolean.TRUE).build();
    }
  }

  @Benchmark
  public Row buildFlatPrimitiveRow(RowState s) {
    return Row.withSchema(s.flatSchema).addValues(s.flatValues).build();
  }

  @Benchmark
  public Row buildComplexRow(RowState s) {
    return Row.withSchema(s.complexSchema)
        .addValue("row-1")
        .addValue(s.childRow)
        .addValue(s.tags)
        .addValue(s.metrics)
        .addValue(s.javaInstant)
        .addValue(s.decimal)
        .addValue(null)
        .build();
  }

  @Benchmark
  public Row buildFlatNamedMapRow(RowState s) {
    return Row.withSchema(s.flatSchema).withFieldValues(s.flatNamedValues).build();
  }

  @Benchmark
  public Row buildFlatByFieldName(RowState s) {
    return Row.withSchema(s.flatSchema)
        .withFieldValue(s.fInt32, s.intVal)
        .withFieldValue(s.fInt64, s.longVal)
        .withFieldValue(s.fDouble, s.doubleVal)
        .withFieldValue(s.fFloat, s.floatVal)
        .withFieldValue(s.fString, s.stringVal)
        .withFieldValue(s.fBool, s.boolVal)
        .withFieldValue(s.fByte, s.byteVal)
        .withFieldValue(s.fInt16, s.shortVal)
        .withFieldValue(s.fBytes, s.payload)
        .withFieldValue(s.fDatetime, s.jodaInstant)
        .build();
  }

  @Benchmark
  public Row buildFlatByFieldId(RowState s) {
    return Row.withSchema(s.flatSchema)
        .withFieldValue(0, s.intVal)
        .withFieldValue(1, s.longVal)
        .withFieldValue(2, s.doubleVal)
        .withFieldValue(3, s.floatVal)
        .withFieldValue(4, s.stringVal)
        .withFieldValue(5, s.boolVal)
        .withFieldValue(6, s.byteVal)
        .withFieldValue(7, s.shortVal)
        .withFieldValue(8, s.payload)
        .withFieldValue(9, s.jodaInstant)
        .build();
  }

  @Benchmark
  public Row fromRowSingleFlatOverride(RowState s) {
    return Row.fromRow(s.flatRow).withFieldValue(s.fString, s.updatedString).build();
  }

  @Benchmark
  public Row fromRowNestedOverride(RowState s) {
    return Row.fromRow(s.complexRow).withFieldValue(s.nestedPath, s.updatedChildId).build();
  }

  @Benchmark
  public Row fromRowDeepNestedOverride(RowState s) {
    return Row.fromRow(s.deepRow).withFieldValue(s.deepNestedPath, s.updatedChildId).build();
  }

  @Benchmark
  public Row nullRowCreation(RowState s) {
    return Row.nullRow(s.nullableSchema);
  }

  @Benchmark
  public Row rowToRowCollector(RowState s) {
    return Stream.of(s.flatValues).collect(Row.toRow(s.flatSchema));
  }

  @Benchmark
  public List<Object> rowGetBaseValues(RowState s) {
    return s.flatRow.getBaseValues();
  }

  @Benchmark
  public String rowToPrettyString(RowState s) {
    return s.flatRow.toString();
  }

  @Benchmark
  public Row rowSorted(RowState s) {
    return s.flatRow.sorted();
  }

  @Benchmark
  public Row rowToSnakeCase(RowState s) {
    return s.camelRow.toSnakeCase();
  }

  @Benchmark
  public Row buildByteBufferSliceRow(RowState s) {
    return Row.withSchema(s.bytesSchema).addValue(s.offsetZeroSlice).build();
  }

  @Benchmark
  public @Nullable Object verifyFieldValueString(RowState s) {
    return SchemaVerification.verifyFieldValue(s.stringVal, s.verifyFieldType, s.verifyFieldName);
  }

  @Benchmark
  public FieldAccessDescriptor parseSimpleFieldName(RowState s) {
    return FieldAccessDescriptorParser.parse(s.simpleFieldPath);
  }

  @Benchmark
  public FieldAccessDescriptor parseDottedFieldPath(RowState s) {
    return FieldAccessDescriptorParser.parse(s.dottedFieldPath);
  }

  @Benchmark
  public FieldAccessDescriptor parseDottedWildcardPath(RowState s) {
    return FieldAccessDescriptorParser.parse(s.dottedWildcardPath);
  }
}

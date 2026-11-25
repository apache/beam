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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.parser.FieldAccessDescriptorParser;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.SchemaVerification;
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

  /** Pre-initialized schemas, payloads, and baseline rows for JMH benchmarks. */
  @State(Scope.Thread)
  public static class RowState {
    Schema flatSchema;
    Schema nullableSchema;
    Schema complexSchema;

    Object[] flatValues;
    Row flatRow;
    Row childRow;
    Row complexRow;
    Row camelRow;
    List<String> tags;
    Map<String, Long> metrics;
    java.time.Instant javaInstant;
    BigDecimal decimal;
    byte[] payload;
    Instant jodaInstant;
    Map<String, Object> flatNamedValues;

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

      Schema camelSchema =
          Schema.builder()
              .addStringField("userId")
              .addInt64Field("eventTimestamp")
              .addStringField("sourceName")
              .addBooleanField("isActive")
              .build();

      payload = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};
      jodaInstant = new Instant(1_700_000_000_000L);
      flatValues =
          new Object[] {
            42,
            1234567890123L,
            12.5,
            1.25f,
            "beam-row-value",
            Boolean.TRUE,
            (byte) 7,
            (short) 99,
            payload,
            jodaInstant
          };

      flatNamedValues = new HashMap<>();
      flatNamedValues.put("f_int32", 42);
      flatNamedValues.put("f_int64", 1234567890123L);
      flatNamedValues.put("f_double", 12.5);
      flatNamedValues.put("f_float", 1.25f);
      flatNamedValues.put("f_string", "beam-row-value");
      flatNamedValues.put("f_bool", Boolean.TRUE);
      flatNamedValues.put("f_byte", (byte) 7);
      flatNamedValues.put("f_int16", (short) 99);
      flatNamedValues.put("f_bytes", payload);
      flatNamedValues.put("f_datetime", jodaInstant);

      flatRow = Row.withSchema(flatSchema).addValues(flatValues).build();
      childRow = Row.withSchema(nestedChildSchema).attachValues("child-a", 100L);
      tags = Arrays.asList("alpha", "beta", "gamma");
      metrics = new HashMap<>();
      metrics.put("cpu", 10L);
      metrics.put("mem", 20L);
      javaInstant = java.time.Instant.ofEpochMilli(1_700_000_000_000L);
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
        .withFieldValue("f_int32", 42)
        .withFieldValue("f_int64", 1234567890123L)
        .withFieldValue("f_double", 12.5)
        .withFieldValue("f_float", 1.25f)
        .withFieldValue("f_string", "beam-row-value")
        .withFieldValue("f_bool", Boolean.TRUE)
        .withFieldValue("f_byte", (byte) 7)
        .withFieldValue("f_int16", (short) 99)
        .withFieldValue("f_bytes", s.payload)
        .withFieldValue("f_datetime", s.jodaInstant)
        .build();
  }

  @Benchmark
  public Row buildFlatByFieldId(RowState s) {
    return Row.withSchema(s.flatSchema)
        .withFieldValue(0, 42)
        .withFieldValue(1, 1234567890123L)
        .withFieldValue(2, 12.5)
        .withFieldValue(3, 1.25f)
        .withFieldValue(4, "beam-row-value")
        .withFieldValue(5, Boolean.TRUE)
        .withFieldValue(6, (byte) 7)
        .withFieldValue(7, (short) 99)
        .withFieldValue(8, s.payload)
        .withFieldValue(9, s.jodaInstant)
        .build();
  }

  @Benchmark
  public Row fromRowSingleFlatOverride(RowState s) {
    return Row.fromRow(s.flatRow).withFieldValue("f_string", "updated").build();
  }

  @Benchmark
  public Row fromRowNestedOverride(RowState s) {
    return Row.fromRow(s.complexRow).withFieldValue("child.child_id", 200L).build();
  }

  @Benchmark
  public Row nullRowCreation(RowState s) {
    return Row.nullRow(s.nullableSchema);
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
  public Object verifyFieldValueString() {
    return SchemaVerification.verifyFieldValue("beam-row-value", Schema.FieldType.STRING, "f_str");
  }

  @Benchmark
  public FieldAccessDescriptor parseSimpleFieldName() {
    return FieldAccessDescriptorParser.parse("f_string");
  }

  @Benchmark
  public FieldAccessDescriptor parseDottedFieldPath() {
    return FieldAccessDescriptorParser.parse("outer.child.child_id");
  }
}

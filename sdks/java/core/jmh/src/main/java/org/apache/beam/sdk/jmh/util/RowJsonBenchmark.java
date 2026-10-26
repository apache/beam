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
package org.apache.beam.sdk.jmh.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.RowJson.RowJsonDeserializer;
import org.apache.beam.sdk.util.RowJson.RowJsonSerializer;
import org.apache.beam.sdk.util.RowJsonUtils;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

/** Benchmarks for {@link org.apache.beam.sdk.util.RowJson} and {@link JsonUtils}. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RowJsonBenchmark {

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    Schema flatSchema;
    String flatJsonString;
    Row flatRow;
    ObjectMapper flatDeserializerMapper;
    ObjectMapper flatSerializerMapper;

    Schema complexSchema;
    String complexJsonString;
    byte[] complexJsonBytes;
    Row complexRow;
    ObjectMapper complexDeserializerMapper;
    ObjectMapper complexSerializerMapper;
    SimpleFunction<byte[], Row> jsonBytesToRowFn;
    SimpleFunction<Row, byte[]> rowToJsonBytesFn;

    @Setup
    public void setup() {
      flatSchema =
          Schema.builder()
              .addByteField("f_byte")
              .addInt16Field("f_int16")
              .addInt32Field("f_int32")
              .addInt64Field("f_int64")
              .addFloatField("f_float")
              .addDoubleField("f_double")
              .addBooleanField("f_boolean")
              .addStringField("f_string")
              .addDecimalField("f_decimal")
              .addDateTimeField("f_timestamp")
              .build();

      flatRow =
          Row.withSchema(flatSchema)
              .addValues(
                  (byte) 12,
                  (short) 22,
                  32,
                  42L,
                  1.02E5f,
                  62.2d,
                  true,
                  "hello",
                  new BigDecimal("123.12"),
                  new DateTime(2021, 11, 18, 8, 45, 38, DateTimeZone.UTC).toInstant())
              .build();

      flatDeserializerMapper =
          RowJsonUtils.newObjectMapperWith(RowJsonDeserializer.forSchema(flatSchema));
      flatSerializerMapper =
          RowJsonUtils.newObjectMapperWith(RowJsonSerializer.forSchema(flatSchema));
      flatJsonString = RowJsonUtils.rowToJson(flatSerializerMapper, flatRow);

      Schema nestedSchema =
          Schema.builder()
              .addStringField("nested_str")
              .addInt64Field("nested_long")
              .addNullableField("nested_nullable", FieldType.STRING)
              .build();

      complexSchema =
          Schema.builder()
              .addStringField("id")
              .addInt32Field("count")
              .addDoubleField("score")
              .addBooleanField("active")
              .addLogicalTypeField("date", SqlTypes.DATE)
              .addLogicalTypeField("time", SqlTypes.TIME)
              .addLogicalTypeField("datetime", SqlTypes.DATETIME)
              .addArrayField("tags", FieldType.STRING)
              .addMapField("metrics", FieldType.STRING, FieldType.INT64)
              .addRowField("nested", nestedSchema)
              .build();

      Row nestedRow = Row.withSchema(nestedSchema).addValues("inner", 999L, null).build();

      complexRow =
          Row.withSchema(complexSchema)
              .addValues(
                  "user-12345",
                  42,
                  98.6d,
                  true,
                  LocalDate.of(2025, 10, 24),
                  LocalTime.of(11, 30, 0),
                  LocalDateTime.of(2025, 10, 24, 11, 30, 0),
                  Arrays.asList("alpha", "beta", "gamma", "delta"),
                  ImmutableMap.of("k1", 10L, "k2", 20L, "k3", 30L),
                  nestedRow)
              .build();

      complexDeserializerMapper =
          RowJsonUtils.newObjectMapperWith(RowJsonDeserializer.forSchema(complexSchema));
      complexSerializerMapper =
          RowJsonUtils.newObjectMapperWith(RowJsonSerializer.forSchema(complexSchema));
      complexJsonString = RowJsonUtils.rowToJson(complexSerializerMapper, complexRow);
      complexJsonBytes = complexJsonString.getBytes(StandardCharsets.UTF_8);

      jsonBytesToRowFn = JsonUtils.getJsonBytesToRowFunction(complexSchema);
      rowToJsonBytesFn = JsonUtils.getRowToJsonBytesFunction(complexSchema);
    }
  }

  @Benchmark
  public Row deserializeFlatRowString(BenchmarkState state) {
    return RowJsonUtils.jsonToRow(state.flatDeserializerMapper, state.flatJsonString);
  }

  @Benchmark
  public Row deserializeComplexRowString(BenchmarkState state) {
    return RowJsonUtils.jsonToRow(state.complexDeserializerMapper, state.complexJsonString);
  }

  @Benchmark
  public Row deserializeComplexRowBytes(BenchmarkState state) {
    return state.jsonBytesToRowFn.apply(state.complexJsonBytes);
  }

  @Benchmark
  public String serializeComplexRowString(BenchmarkState state) {
    return RowJsonUtils.rowToJson(state.complexSerializerMapper, state.complexRow);
  }

  @Benchmark
  public byte[] serializeComplexRowBytes(BenchmarkState state) {
    return state.rowToJsonBytesFn.apply(state.complexRow);
  }
}

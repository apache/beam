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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
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

/** Benchmarks for {@link Row}, {@link RowCoder}, and {@link SchemaCoder}. */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class RowAndCoderBenchmark {

  public static class NestedPojo {
    public String nestedStr = "inner";
    public long nestedLong = 999L;
    public @Nullable String nestedNullable = null;

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof NestedPojo)) {
        return false;
      }
      NestedPojo that = (NestedPojo) o;
      return nestedLong == that.nestedLong
          && Objects.equals(nestedStr, that.nestedStr)
          && Objects.equals(nestedNullable, that.nestedNullable);
    }

    @Override
    public int hashCode() {
      return Objects.hash(nestedStr, nestedLong, nestedNullable);
    }
  }

  public static class ComplexPojo {
    public String id = "user-12345";
    public int count = 42;
    public double score = 98.6d;
    public boolean active = true;
    public @Nullable String optionalTag = null;
    public List<String> tags = Arrays.asList("alpha", "beta", "gamma", "delta");
    public Map<String, Long> metrics = ImmutableMap.of("k1", 10L, "k2", 20L, "k3", 30L);
    public NestedPojo nested = new NestedPojo();

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ComplexPojo)) {
        return false;
      }
      ComplexPojo that = (ComplexPojo) o;
      return count == that.count
          && Double.compare(that.score, score) == 0
          && active == that.active
          && Objects.equals(id, that.id)
          && Objects.equals(optionalTag, that.optionalTag)
          && Objects.equals(tags, that.tags)
          && Objects.equals(metrics, that.metrics)
          && Objects.equals(nested, that.nested);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, count, score, active, optionalTag, tags, metrics, nested);
    }
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    Schema flatSchema;
    Instant timestamp;
    BigDecimal decimal;
    Row flatRow;
    Row flatRowCopy;
    RowCoder flatRowCoder;
    byte[] encodedFlatRow;

    Schema nestedSchema;
    Schema complexSchema;
    LocalDate localDate;
    List<String> tags;
    Map<String, Long> metrics;
    Row nestedRow;
    Row complexRow;
    Row complexRowCopy;
    RowCoder complexRowCoder;
    byte[] encodedComplexRow;

    ComplexPojo complexPojo;
    SchemaCoder<ComplexPojo> pojoSchemaCoder;
    Row rowWithGetters;
    byte[] encodedPojo;

    ByteStringOutputStream outStream;

    @Setup
    public void setup() throws IOException, NoSuchSchemaException {
      timestamp = new DateTime(2021, 11, 18, 8, 45, 38, DateTimeZone.UTC).toInstant();
      decimal = new BigDecimal("123.12");

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
                  decimal,
                  timestamp)
              .build();
      flatRowCopy =
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
                  decimal,
                  timestamp)
              .build();

      flatRowCoder = RowCoder.of(flatSchema);
      outStream = new ByteStringOutputStream(256);
      flatRowCoder.encode(flatRow, outStream);
      encodedFlatRow = outStream.toByteStringAndReset().toByteArray();

      nestedSchema =
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
              .addNullableField("optional_tag", FieldType.STRING)
              .addLogicalTypeField("date", SqlTypes.DATE)
              .addArrayField("tags", FieldType.STRING)
              .addMapField("metrics", FieldType.STRING, FieldType.INT64)
              .addRowField("nested", nestedSchema)
              .build();

      localDate = LocalDate.of(2025, 10, 24);
      tags = Arrays.asList("alpha", "beta", "gamma", "delta");
      metrics = ImmutableMap.of("k1", 10L, "k2", 20L, "k3", 30L);
      nestedRow = Row.withSchema(nestedSchema).addValues("inner", 999L, null).build();

      complexRow =
          Row.withSchema(complexSchema)
              .addValues(
                  "user-12345", 42, 98.6d, true, null, localDate, tags, metrics, nestedRow)
              .build();
      complexRowCopy =
          Row.withSchema(complexSchema)
              .addValues(
                  "user-12345", 42, 98.6d, true, null, localDate, tags, metrics, nestedRow)
              .build();

      complexRowCoder = RowCoder.of(complexSchema);
      complexRowCoder.encode(complexRow, outStream);
      encodedComplexRow = outStream.toByteStringAndReset().toByteArray();

      SchemaRegistry registry = SchemaRegistry.createDefault();
      registry.registerSchemaProvider(ComplexPojo.class, new JavaFieldSchema());
      registry.registerSchemaProvider(NestedPojo.class, new JavaFieldSchema());
      complexPojo = new ComplexPojo();
      pojoSchemaCoder = registry.getSchemaCoder(ComplexPojo.class);
      rowWithGetters = pojoSchemaCoder.getToRowFunction().apply(complexPojo);
      pojoSchemaCoder.encode(complexPojo, outStream);
      encodedPojo = outStream.toByteStringAndReset().toByteArray();
    }
  }

  @Benchmark
  public Row buildFlatRow(BenchmarkState state) {
    return Row.withSchema(state.flatSchema)
        .addValues(
            (byte) 12,
            (short) 22,
            32,
            42L,
            1.02E5f,
            62.2d,
            true,
            "hello",
            state.decimal,
            state.timestamp)
        .build();
  }

  @Benchmark
  public Row buildComplexRow(BenchmarkState state) {
    Row nested = Row.withSchema(state.nestedSchema).addValues("inner", 999L, null).build();
    return Row.withSchema(state.complexSchema)
        .addValues(
            "user-12345",
            42,
            98.6d,
            true,
            null,
            state.localDate,
            state.tags,
            state.metrics,
            nested)
        .build();
  }

  @Benchmark
  public int complexRowEqualsAndHashCode(BenchmarkState state) {
    boolean eq = state.complexRow.equals(state.complexRowCopy);
    return eq ? state.complexRow.hashCode() : 0;
  }

  @Benchmark
  public int readRowWithGettersRepeatedly(BenchmarkState state) {
    Row r = state.rowWithGetters;
    // Read nested row and collection/map fields multiple times on the same RowWithGetters
    Row nested1 = r.getRow(7);
    Row nested2 = r.getRow(7);
    return r.getArray(5).size()
        + r.getMap(6).size()
        + nested1.getString(0).length()
        + nested2.getString(0).length();
  }

  @Benchmark
  public int encodeFlatRow(BenchmarkState state) throws IOException {
    state.outStream.reset();
    state.flatRowCoder.encode(state.flatRow, state.outStream);
    return state.outStream.size();
  }

  @Benchmark
  public Row decodeFlatRow(BenchmarkState state) throws IOException {
    return state.flatRowCoder.decode(new ByteArrayInputStream(state.encodedFlatRow));
  }

  @Benchmark
  public int encodeComplexRow(BenchmarkState state) throws IOException {
    state.outStream.reset();
    state.complexRowCoder.encode(state.complexRow, state.outStream);
    return state.outStream.size();
  }

  @Benchmark
  public Row decodeComplexRow(BenchmarkState state) throws IOException {
    return state.complexRowCoder.decode(new ByteArrayInputStream(state.encodedComplexRow));
  }

  @Benchmark
  public int encodePojoViaSchemaCoder(BenchmarkState state) throws IOException {
    state.outStream.reset();
    state.pojoSchemaCoder.encode(state.complexPojo, state.outStream);
    return state.outStream.size();
  }

  @Benchmark
  public ComplexPojo decodePojoViaSchemaCoder(BenchmarkState state) throws IOException {
    return state.pojoSchemaCoder.decode(new ByteArrayInputStream(state.encodedPojo));
  }
}

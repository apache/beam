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

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.time.Instant;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A timestamp represented with configurable precision.
 *
 * <p>This logical type stores timestamps as a Row with two fields:
 *
 * <ul>
 *   <li>seconds: INT64 - seconds since Unix epoch (can be negative)
 *   <li>subseconds: INT16 or INT32 - always non-negative (0 to 10^precision - 1)
 * </ul>
 *
 * <p>The subseconds field is always non-negative, even for timestamps before the epoch. For
 * example, -1.5 seconds is represented as {seconds: -2, subseconds: 500000} for microsecond
 * precision. This matches Java's {@link java.time.Instant} internal representation.
 *
 * <p><b>Note for users converting from single-integer timestamp representations:</b> If you have
 * timestamps stored as a single long value (e.g., microseconds since epoch), you must handle
 * negative modulo correctly when converting:
 *
 * <pre>{@code
 * long timestampMicros = -1_500_000;
 * long seconds = timestampMicros / 1_000_000;
 * long micros = timestampMicros % 1_000_000;
 * if (micros < 0) {
 *   micros += 1_000_000;
 *   seconds -= 1;
 * }
 * Instant instant = Instant.ofEpochSecond(seconds, micros * 1000);
 * }</pre>
 */
public class Timestamp implements Schema.LogicalType<Instant, Row> {
  public static final String IDENTIFIER = "beam:logical_type:timestamp:v1";
  static final int MIN_PRECISION = 0;
  static final int MAX_PRECISION = 9;

  private final int precision;
  private final int scalingFactor;
  private final Schema timestampSchema;

  public static Timestamp of(int precision) {
    return new Timestamp(precision);
  }

  public static final Timestamp MILLIS = Timestamp.of(3);
  public static final Timestamp MICROS = Timestamp.of(6);
  public static final Timestamp NANOS = Timestamp.of(9);

  public Timestamp(int precision) {
    checkArgument(
        precision <= MAX_PRECISION && precision >= MIN_PRECISION,
        "Timestamp precision must be between %s and %s (inclusive), but was %s.",
        MIN_PRECISION,
        MAX_PRECISION,
        precision);
    this.precision = precision;
    this.scalingFactor = (int) Math.pow(10, MAX_PRECISION - precision);
    if (precision < 5) {
      this.timestampSchema =
          Schema.builder().addInt64Field("seconds").addInt16Field("subseconds").build();
    } else {
      this.timestampSchema =
          Schema.builder().addInt64Field("seconds").addInt32Field("subseconds").build();
    }
  }

  @Override
  public String getIdentifier() {
    return IDENTIFIER;
  }

  @Override
  public Schema.FieldType getArgumentType() {
    return Schema.FieldType.INT32;
  }

  @Override
  public Integer getArgument() {
    return precision;
  }

  @Override
  public Schema.FieldType getBaseType() {
    return Schema.FieldType.row(timestampSchema);
  }

  @Override
  public Row toBaseType(Instant input) {
    // Avoid silent data loss
    checkState(
        input.getNano() % scalingFactor == 0,
        "Timestamp logical type was configured with precision %s, but encountered "
            + "a Java Instant with %s nanoseconds (not evenly divisible by scaling factor %s).",
        precision,
        input.getNano(),
        scalingFactor);

    int subseconds = input.getNano() / scalingFactor;

    Row.Builder rowBuilder = Row.withSchema(timestampSchema).addValue(input.getEpochSecond());
    if (precision < 5) {
      rowBuilder.addValue((short) subseconds); // Explicitly add as short
    } else {
      rowBuilder.addValue(subseconds); // Add as int
    }
    return rowBuilder.build();
  }

  @Override
  public Instant toInputType(@NonNull Row base) {
    long subseconds =
        (precision < 5)
            ? checkArgumentNotNull(
                base.getInt16(1),
                "While trying to convert to Instant: Row missing subseconds field")
            : checkArgumentNotNull(
                base.getInt32(1),
                "While trying to convert to Instant: Row missing subseconds field");

    checkArgument(
        subseconds >= 0,
        "While trying to convert to Instant: subseconds field must be non-negative, "
            + "but was %s. This likely indicates data corruption.",
        subseconds);

    int maxSubseconds = (int) (Math.pow(10, precision) - 1);
    checkArgument(
        subseconds <= maxSubseconds,
        "While trying to convert to Instant: subseconds field must be <= %s for precision %s, "
            + "but was %s. This likely indicates data corruption or precision mismatch.",
        maxSubseconds,
        precision,
        subseconds);
    return Instant.ofEpochSecond(
        checkArgumentNotNull(
            base.getInt64(0), "While trying to convert to Instant: Row missing seconds field"),
        subseconds * scalingFactor);
  }
}

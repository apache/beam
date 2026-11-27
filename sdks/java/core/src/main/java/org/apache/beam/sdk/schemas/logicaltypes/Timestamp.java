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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.time.Instant;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.SchemaApi;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

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
  public static final String IDENTIFIER =
      SchemaApi.LogicalTypes.Enum.TIMESTAMP
          .getValueDescriptor()
          .getOptions()
          .getExtension(RunnerApi.beamUrn);

  static final int MIN_PRECISION = 0;
  static final int MAX_PRECISION = 9;

  private static final int[] SCALING_FACTORS = {
    1_000_000_000, 100_000_000, 10_000_000, 1_000_000, 100_000, 10_000, 1_000, 100, 10, 1
  };
  private static final int[] MAX_SUBSECONDS = {
    0, 9, 99, 999, 9_999, 99_999, 999_999, 9_999_999, 99_999_999, 999_999_999
  };

  private final int precision;
  private final int scalingFactor;
  private final Schema timestampSchema;
  private transient volatile Schema.@Nullable FieldType cachedBaseType;

  private static final Timestamp[] INSTANCES = new Timestamp[MAX_PRECISION + 1];

  static {
    for (int i = MIN_PRECISION; i <= MAX_PRECISION; i++) {
      INSTANCES[i] = new Timestamp(i);
    }
  }

  public static Timestamp of(int precision) {
    if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
      throw new IllegalArgumentException(
          String.format(
              "Timestamp precision must be between %s and %s (inclusive), but was %s.",
              MIN_PRECISION, MAX_PRECISION, precision));
    }
    return INSTANCES[precision];
  }

  public static final Timestamp MILLIS = INSTANCES[3];
  public static final Timestamp MICROS = INSTANCES[6];
  public static final Timestamp NANOS = INSTANCES[9];

  public Timestamp(int precision) {
    checkArgument(
        precision <= MAX_PRECISION && precision >= MIN_PRECISION,
        "Timestamp precision must be between %s and %s (inclusive), but was %s.",
        MIN_PRECISION,
        MAX_PRECISION,
        precision);
    this.precision = precision;
    this.scalingFactor = SCALING_FACTORS[precision];
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
    Schema.FieldType baseType = cachedBaseType;
    if (baseType == null) {
      baseType = Schema.FieldType.row(timestampSchema);
      cachedBaseType = baseType;
    }
    return baseType;
  }

  @Override
  public Row toBaseType(Instant input) {
    int nano = input.getNano();
    int subseconds;
    if (scalingFactor == 1) {
      subseconds = nano;
    } else {
      if (nano % scalingFactor != 0) {
        throw new IllegalStateException(
            String.format(
                "Timestamp logical type was configured with precision %s, but encountered "
                    + "a Java Instant with %s nanoseconds (not evenly divisible by scaling factor %s).",
                precision, nano, scalingFactor));
      }
      subseconds = nano / scalingFactor;
    }

    if (precision < 5) {
      return Row.attachValues(timestampSchema, input.getEpochSecond(), (short) subseconds);
    }
    return Row.attachValues(timestampSchema, input.getEpochSecond(), subseconds);
  }

  @Override
  public Instant toInputType(@NonNull Row base) {
    long subseconds;
    if (precision < 5) {
      Short s = base.getInt16(1);
      if (s == null) {
        throw new IllegalArgumentException(
            "While trying to convert to Instant: Row missing subseconds field");
      }
      subseconds = s;
    } else {
      Integer i = base.getInt32(1);
      if (i == null) {
        throw new IllegalArgumentException(
            "While trying to convert to Instant: Row missing subseconds field");
      }
      subseconds = i;
    }

    if (subseconds < 0) {
      throw new IllegalArgumentException(
          String.format(
              "While trying to convert to Instant: subseconds field must be non-negative, "
                  + "but was %s. This likely indicates data corruption.",
              subseconds));
    }

    int maxSubseconds = MAX_SUBSECONDS[precision];
    if (subseconds > maxSubseconds) {
      throw new IllegalArgumentException(
          String.format(
              "While trying to convert to Instant: subseconds field must be <= %s for precision %s, "
                  + "but was %s. This likely indicates data corruption or precision mismatch.",
              maxSubseconds, precision, subseconds));
    }
    Long seconds = base.getInt64(0);
    if (seconds == null) {
      throw new IllegalArgumentException(
          "While trying to convert to Instant: Row missing seconds field");
    }
    return Instant.ofEpochSecond(seconds, subseconds * scalingFactor);
  }
}

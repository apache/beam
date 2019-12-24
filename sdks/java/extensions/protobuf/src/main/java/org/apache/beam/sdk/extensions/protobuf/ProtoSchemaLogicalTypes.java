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
package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.values.Row;

/** A set of {@link LogicalType} classes to represent protocol buffer types. */
public class ProtoSchemaLogicalTypes {
  /** Base class for types representing timestamps or durations as nanoseconds. */
  public abstract static class NanosType<T> implements LogicalType<T, Row> {
    private final String identifier;

    private static final Schema SCHEMA =
        Schema.builder().addInt64Field("seconds").addInt32Field("nanos").build();

    protected NanosType(String identifier) {
      this.identifier = identifier;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public FieldType getArgumentType() {
      return FieldType.STRING;
    }

    @Override
    public FieldType getBaseType() {
      return FieldType.row(SCHEMA);
    }
  }

  /** A timestamp represented as nanoseconds since the epoch. */
  public static class TimestampNanos extends NanosType<Timestamp> {
    public static final String IDENTIFIER = "ProtoTimestamp";

    public TimestampNanos() {
      super(IDENTIFIER);
    }

    @Override
    public Row toBaseType(Timestamp input) {
      return toRow(input);
    }

    @Override
    public Timestamp toInputType(Row base) {
      return toTimestamp(base);
    }

    public static Row toRow(Timestamp input) {
      return Row.withSchema(NanosType.SCHEMA)
          .addValues(input.getSeconds(), input.getNanos())
          .build();
    }

    public static Timestamp toTimestamp(Row row) {
      return Timestamp.newBuilder().setSeconds(row.getInt64(0)).setNanos(row.getInt32(1)).build();
    }
  }

  /** A duration represented in nanoseconds. */
  public static class DurationNanos extends NanosType<Duration> {
    public static final String IDENTIFIER = "ProtoTimestamp";

    public DurationNanos() {
      super(IDENTIFIER);
    }

    @Override
    public Row toBaseType(Duration input) {
      return toRow(input);
    }

    @Override
    public Duration toInputType(Row base) {
      return toDuration(base);
    }

    public static Row toRow(Duration input) {
      return Row.withSchema(NanosType.SCHEMA)
          .addValues(input.getSeconds(), input.getNanos())
          .build();
    }

    public static Duration toDuration(Row row) {
      return Duration.newBuilder().setSeconds(row.getInt64(0)).setNanos(row.getInt32(1)).build();
    }
  }

  /** A UInt32 type. */
  public static class UInt32 extends PassThroughLogicalType<Integer> {
    public static final String IDENTIFIER = "Uint32";

    UInt32() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT32);
    }
  }

  /** A SInt32 type. */
  public static class SInt32 extends PassThroughLogicalType<Integer> {
    public static final String IDENTIFIER = "Sint32";

    SInt32() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT32);
    }
  }

  /** A Fixed32 type. */
  public static class Fixed32 extends PassThroughLogicalType<Integer> {
    public static final String IDENTIFIER = "Fixed32";

    Fixed32() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT32);
    }
  }

  /** A SFixed32 type. */
  public static class SFixed32 extends PassThroughLogicalType<Integer> {
    public static final String IDENTIFIER = "SFixed32";

    SFixed32() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT32);
    }
  }

  /** A UIn64 type. */
  public static class UInt64 extends PassThroughLogicalType<Long> {
    public static final String IDENTIFIER = "Uint64";

    UInt64() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT64);
    }
  }

  /** A SIn64 type. */
  public static class SInt64 extends PassThroughLogicalType<Long> {
    public static final String IDENTIFIER = "Sint64";

    SInt64() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT64);
    }
  }

  /** A Fixed64 type. */
  public static class Fixed64 extends PassThroughLogicalType<Long> {
    public static final String IDENTIFIER = "Fixed64";

    Fixed64() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT64);
    }
  }

  /** An SFixed64 type. */
  public static class SFixed64 extends PassThroughLogicalType<Long> {
    public static final String IDENTIFIER = "SFixed64";

    SFixed64() {
      super(IDENTIFIER, FieldType.STRING, "", FieldType.INT64);
    }
  }
}

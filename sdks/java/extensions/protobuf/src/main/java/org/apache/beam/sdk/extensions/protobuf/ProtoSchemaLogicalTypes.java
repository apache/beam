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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.values.Row;

/** A set of {@link LogicalType} classes to represent protocol buffer types. */
@Experimental(Kind.SCHEMAS)
public class ProtoSchemaLogicalTypes {

  /** Compatible schema with the row schema of NanosDuration and NanosInstant. */
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("seconds").addInt32Field("nanos").build();

  public static class TimestampConvert {

    /** ByteBuddy conversion for Timestamp to NanosInstant base type. */
    public static Row toRow(Timestamp input) {
      return Row.withSchema(SCHEMA).addValues(input.getSeconds(), input.getNanos()).build();
    }

    /** ByteBuddy conversion for NanosInstant base type to Timestamp. */
    public static Timestamp toTimestamp(Row row) {
      return Timestamp.newBuilder().setSeconds(row.getInt64(0)).setNanos(row.getInt32(1)).build();
    }
  }

  public static class DurationConvert {
    /** ByteBuddy conversion for Duration to NanosDuration base type. */
    public static Row toRow(Duration input) {
      return Row.withSchema(SCHEMA).addValues(input.getSeconds(), input.getNanos()).build();
    }

    /** ByteBuddy conversion for NanosDuration base type to Duration. */
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

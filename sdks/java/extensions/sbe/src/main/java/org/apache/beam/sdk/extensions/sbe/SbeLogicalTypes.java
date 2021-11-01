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
package org.apache.beam.sdk.extensions.sbe;

import static org.apache.beam.sdk.extensions.sbe.Schemas.TZ_TIME_SCHEMA;
import static org.apache.beam.sdk.extensions.sbe.Schemas.UTC_TIME_SCHEMA;

import java.time.LocalDate;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * Classes that represent various SBE semantic types.
 *
 * <p>Not all of SBE's semantic types are represented here, as some can be reasonably represented in
 * Beam schemas with just a primitive.
 */
@Experimental(Kind.SCHEMAS)
public final class SbeLogicalTypes {
  // Default argument type values
  private static final String DEFAULT_STRING_ARG = "";

  private SbeLogicalTypes() {}

  // Unsigned types are all stored at the next highest value. This prevents unexpected behavior
  // when reading and likely has negligible space impact.

  /** Represents SBE's uint8 type. */
  public static final class Uint8 extends PassThroughLogicalType<Short> {
    public static final String IDENTIFIER = "uint8";

    public Uint8() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.INT16);
    }
  }

  /** Represents SBE's uint16 type. */
  public static final class Uint16 extends PassThroughLogicalType<Integer> {
    public static final String IDENTIFIER = "uint16";

    public Uint16() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.INT32);
    }
  }

  /** Represents SBE's uint32 type. */
  public static final class Uint32 extends PassThroughLogicalType<Long> {
    public static final String IDENTIFIER = "uint32";

    public Uint32() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.INT64);
    }
  }

  /** Represents SBE's uint64 type. */
  public static final class Uint64 extends PassThroughLogicalType<String> {
    // Unknown if anyone will ever use this as a BigInteger, so we're keeping it as a String for
    // now.

    public static final String IDENTIFIER = "uint64";

    public Uint64() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.STRING);
    }
  }

  // SBE time-based composite and logical types.

  /** Represents SBE's UTCTimestamp composite type. */
  public static final class UTCTimestamp extends PassThroughLogicalType<Row> {
    public static final String IDENTIFIER = "UTCTimestamp";

    UTCTimestamp() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.row(UTC_TIME_SCHEMA));
    }
  }

  /** Represents SBE's UTCTimeOnly composite type. */
  public static final class UTCTimeOnly extends PassThroughLogicalType<Row> {
    public static final String IDENTIFIER = "UTCTimeOnly";

    UTCTimeOnly() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.row(UTC_TIME_SCHEMA));
    }
  }

  /** Represents SBE's TZTimestamp composite type. */
  public static final class TZTimestamp extends PassThroughLogicalType<Row> {
    public static final String IDENTIFIER = "TZTimestamp";

    TZTimestamp() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.row(TZ_TIME_SCHEMA));
    }
  }

  /** Represents SBE's TimeOnly composite type. */
  public static final class TZTimeOnly extends PassThroughLogicalType<Row> {
    public static final String IDENTIFIER = "TZTimeOnly";

    TZTimeOnly() {
      super(IDENTIFIER, FieldType.STRING, DEFAULT_STRING_ARG, FieldType.row(TZ_TIME_SCHEMA));
    }
  }

  /** Helper type for SBE's date types. */
  private static class SbeDateType implements LogicalType<LocalDate, Integer> {
    private final String identifier;

    SbeDateType(String identifier) {
      this.identifier = identifier;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized String getIdentifier() {
      return identifier;
    }

    @Override
    public @Nullable @UnknownKeyFor @Initialized FieldType getArgumentType() {
      return null;
    }

    @Override
    public @UnknownKeyFor @NonNull @Initialized FieldType getBaseType() {
      return FieldType.INT32;
    }

    @Override
    public @NonNull Integer toBaseType(@NonNull LocalDate input) {
      // This is a safe cast. SBE's type is only 16 bits, but it's unsigned, so we
      // use 32-bit integers to represent it.
      return (int) input.toEpochDay();
    }

    @Override
    public @NonNull LocalDate toInputType(@NonNull Integer base) {
      return LocalDate.ofEpochDay(base);
    }
  }

  /** Representation of SBE's UTCDateOnly. */
  public static final class UTCDateOnly extends SbeDateType {
    public static final String IDENTIFIER = "UTCDateOnly";

    public UTCDateOnly() {
      super(IDENTIFIER);
    }
  }

  /** Representation of SBE's LocalMktDate. */
  public static final class LocalMktDate extends SbeDateType {
    public static final String IDENTIFIER = "LocalMktDate";

    public LocalMktDate() {
      super(IDENTIFIER);
    }
  }
}

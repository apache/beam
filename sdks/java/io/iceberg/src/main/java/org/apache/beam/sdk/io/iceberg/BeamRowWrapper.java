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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.MicrosInstant;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper that adapts a Beam {@link Row} to Iceberg's {@link StructLike} interface.
 *
 * <p>This class allows Beam rows to be processed by Iceberg internal components (like partition
 * keys or writers) without requiring a full conversion into Iceberg's internal Record format. It
 * handles the mapping between Beam's {@link Schema} and Iceberg's {@link Types.StructType},
 * including complex type conversions for timestamps, logical types, and UUIDs.
 *
 * <p><b>Note:</b> This implementation is <b>read-only</b>. Calls to {@link #set(int, Object)} will
 * throw an {@link UnsupportedOperationException}.
 */
public class BeamRowWrapper implements StructLike {

  private final FieldType[] types;
  private final @Nullable PositionalGetter<?>[] getters;
  private @Nullable Row row = null;

  /** Constructs a new wrapper and pre-computes the mapping between Beam and Iceberg fields. */
  public BeamRowWrapper(Schema schema, Types.StructType struct) {
    int size = schema.getFieldCount();

    types = (FieldType[]) Array.newInstance(FieldType.class, size);
    getters = (PositionalGetter[]) Array.newInstance(PositionalGetter.class, size);

    for (int i = 0; i < size; i++) {
      types[i] = schema.getField(i).getType();
      getters[i] = buildGetter(types[i], struct.fields().get(i).type());
    }
  }

  /**
   * Sets the current Beam {@link Row} to be wrapped. This method allows the wrapper to be reused
   * across different rows to minimize object allocation.
   */
  public BeamRowWrapper wrap(@Nullable Row row) {
    this.row = row;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  /**
   * Retrieves a field value from the wrapped row, performing any necessary type conversion to match
   * Iceberg's internal expectations (e.g., converting Timestamps to microseconds).
   */
  @Override
  public <T> @Nullable T get(int pos, Class<T> javaClass) {
    if (row == null || row.getValue(pos) == null) {
      return null;
    } else if (getters[pos] != null) {
      return javaClass.cast(getters[pos].get(checkStateNotNull(row), pos));
    }

    return javaClass.cast(checkStateNotNull(row).getValue(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
        "Could not set a field in the BeamRowWrapper because rowData is read-only");
  }

  private interface PositionalGetter<T> {
    T get(Row data, int pos);
  }

  /**
   * Factory method to create a getter that handles type-specific conversion logic.
   *
   * <p>Handles special cases:
   *
   * <ul>
   *   <li>UUID: Converts {@code byte[]} to Iceberg's UUID representation.
   *   <li>DateTime: Converts Beam {@code DateTime} or logical types to microsecond timestamps.
   *   <li>Nested Rows: Recursively wraps nested structures in a new {@code BeamRowWrapper}.
   * </ul>
   */
  private static @Nullable PositionalGetter<?> buildGetter(FieldType beamType, Type icebergType) {
    switch (beamType.getTypeName()) {
      case BYTE:
        return Row::getByte;
      case INT16:
        return Row::getInt16;
      case STRING:
        return Row::getString;
      case BYTES:
        return (row, pos) -> {
          byte[] bytes = checkStateNotNull(row.getBytes(pos));
          if (Type.TypeID.UUID == icebergType.typeId()) {
            return UUIDUtil.convert(bytes);
          } else {
            return ByteBuffer.wrap(bytes);
          }
        };
      case DECIMAL:
        return Row::getDecimal;
      case DATETIME:
        return (row, pos) ->
            TimeUnit.MILLISECONDS.toMicros(checkStateNotNull(row.getDateTime(pos)).getMillis());
      case ROW:
        Schema beamSchema = checkStateNotNull(beamType.getRowSchema());
        Types.StructType structType = (Types.StructType) icebergType;

        BeamRowWrapper nestedWrapper = new BeamRowWrapper(beamSchema, structType);
        return (row, pos) -> nestedWrapper.wrap(row.getRow(pos));
      case LOGICAL_TYPE:
        if (beamType.isLogicalType(MicrosInstant.IDENTIFIER)) {
          return (row, pos) -> {
            Instant instant = checkStateNotNull(row.getLogicalTypeValue(pos, Instant.class));
            return TimeUnit.SECONDS.toMicros(instant.getEpochSecond()) + instant.getNano() / 1000;
          };
        } else if (beamType.isLogicalType(DateTime.IDENTIFIER)) {
          return (row, pos) ->
              DateTimeUtil.microsFromTimestamp(
                  checkStateNotNull(row.getLogicalTypeValue(pos, LocalDateTime.class)));
        } else if (beamType.isLogicalType(Date.IDENTIFIER)) {
          return (row, pos) ->
              DateTimeUtil.daysFromDate(
                  checkStateNotNull(row.getLogicalTypeValue(pos, LocalDate.class)));
        } else if (beamType.isLogicalType(Time.IDENTIFIER)) {
          return (row, pos) ->
              DateTimeUtil.microsFromTime(
                  checkStateNotNull(row.getLogicalTypeValue(pos, LocalTime.class)));
        } else if (beamType.isLogicalType(FixedPrecisionNumeric.IDENTIFIER)) {
          return (row, pos) -> row.getLogicalTypeValue(pos, BigDecimal.class);
        } else {
          return null;
        }
      default:
        return null;
    }
  }
}

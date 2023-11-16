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
package org.apache.beam.sdk.io.jdbc;

import java.sql.JDBCType;
import java.time.Instant;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.FixedString;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Beam {@link org.apache.beam.sdk.schemas.Schema.LogicalType} implementations of JDBC types. */
class LogicalTypes {
  static final Schema.FieldType JDBC_BIT_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Boolean>(
              JDBCType.BIT.getName(), FieldType.STRING, "", Schema.FieldType.BOOLEAN) {});

  static final Schema.FieldType JDBC_DATE_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.DATE.getName(), FieldType.STRING, "", Schema.FieldType.DATETIME) {});

  static final Schema.FieldType JDBC_FLOAT_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Double>(
              JDBCType.FLOAT.getName(), FieldType.STRING, "", Schema.FieldType.DOUBLE) {});

  static final Schema.FieldType JDBC_TIME_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.TIME.getName(), FieldType.STRING, "", Schema.FieldType.DATETIME) {});

  static final Schema.FieldType JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<Instant>(
              JDBCType.TIMESTAMP_WITH_TIMEZONE.getName(),
              FieldType.STRING,
              "",
              Schema.FieldType.DATETIME) {});

  static final Schema.FieldType JDBC_UUID_TYPE =
      Schema.FieldType.logicalType(new UuidLogicalType());

  static final Schema.FieldType OTHER_AS_STRING_TYPE =
      Schema.FieldType.logicalType(
          new PassThroughLogicalType<String>(
              JDBCType.OTHER.getName(), FieldType.STRING, "", FieldType.STRING) {});

  @VisibleForTesting
  static Schema.FieldType fixedLengthString(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(FixedString.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType fixedLengthBytes(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(FixedBytes.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType variableLengthString(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(VariableString.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType variableLengthBytes(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(VariableBytes.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType numeric(int precision, int scale) {
    return Schema.FieldType.logicalType(FixedPrecisionNumeric.of(precision, scale));
  }

  /**
   * Returns a {@link FixedBytes}, or {@link VariableBytes} when length is Integer.MAX_VALUE.
   *
   * <p>In some database, certain variable bytes type (e.g. bytea in postgresql) also returns BINARY
   * jdbc type. This helper method make BINARY(Integer.MAX_VALUE) returns a variable bytes logical
   * type thus avoid out-of-memory due to padding in fixed-length bytes.
   */
  static Schema.LogicalType<byte[], byte[]> fixedOrVariableBytes(String name, int length) {
    if (length == Integer.MAX_VALUE) {
      return VariableBytes.of(name, length);
    } else {
      return FixedBytes.of(name, length);
    }
  }

  /** Base class for JDBC logical types. */
  abstract static class JdbcLogicalType<T extends @NonNull Object>
      implements Schema.LogicalType<T, T> {
    protected final String identifier;
    protected final Schema.FieldType argumentType;
    protected final Schema.FieldType baseType;
    protected final Object argument;

    protected JdbcLogicalType(
        String identifier,
        Schema.FieldType argumentType,
        Schema.FieldType baseType,
        Object argument) {
      this.identifier = identifier;
      this.argumentType = argumentType;
      this.baseType = baseType;
      this.argument = argument;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public FieldType getArgumentType() {
      return argumentType;
    }

    @Override
    @SuppressWarnings("TypeParameterUnusedInFormals")
    public <ArgumentT> ArgumentT getArgument() {
      return (ArgumentT) argument;
    }

    @Override
    public Schema.FieldType getBaseType() {
      return baseType;
    }

    @Override
    public T toBaseType(T input) {
      return input;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof JdbcLogicalType)) {
        return false;
      }
      JdbcLogicalType<?> that = (JdbcLogicalType<?>) o;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(baseType, that.baseType)
          && Objects.equals(argument, that.argument);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, baseType, argument);
    }
  }
}

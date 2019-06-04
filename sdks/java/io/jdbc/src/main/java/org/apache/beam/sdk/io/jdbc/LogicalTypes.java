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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;

/** Beam {@link org.apache.beam.sdk.schemas.Schema.LogicalType} implementations of JDBC types. */
public class LogicalTypes {
  public static final Schema.FieldType JDBC_BIT_TYPE =
      Schema.FieldType.logicalType(
          new org.apache.beam.sdk.schemas.LogicalTypes.PassThroughLogicalType<Boolean>(
              JDBCType.BIT.getName(), "", Schema.FieldType.BOOLEAN) {});

  public static final Schema.FieldType JDBC_DATE_TYPE =
      Schema.FieldType.logicalType(
          new org.apache.beam.sdk.schemas.LogicalTypes.PassThroughLogicalType<Instant>(
              JDBCType.DATE.getName(), "", Schema.FieldType.DATETIME) {});

  public static final Schema.FieldType JDBC_FLOAT_TYPE =
      Schema.FieldType.logicalType(
          new org.apache.beam.sdk.schemas.LogicalTypes.PassThroughLogicalType<Double>(
              JDBCType.FLOAT.getName(), "", Schema.FieldType.DOUBLE) {});

  public static final Schema.FieldType JDBC_TIME_TYPE =
      Schema.FieldType.logicalType(
          new org.apache.beam.sdk.schemas.LogicalTypes.PassThroughLogicalType<Instant>(
              JDBCType.TIME.getName(), "", Schema.FieldType.DATETIME) {});

  public static final Schema.FieldType JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE =
      Schema.FieldType.logicalType(
          new org.apache.beam.sdk.schemas.LogicalTypes.PassThroughLogicalType<Instant>(
              JDBCType.TIMESTAMP_WITH_TIMEZONE.getName(), "", Schema.FieldType.DATETIME) {});

  @VisibleForTesting
  static Schema.FieldType fixedLengthString(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(FixedLengthString.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType fixedLengthBytes(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(FixedLengthBytes.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType variableLengthString(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(VariableLengthString.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType variableLengthBytes(JDBCType jdbcType, int length) {
    return Schema.FieldType.logicalType(VariableLengthBytes.of(jdbcType.getName(), length));
  }

  @VisibleForTesting
  static Schema.FieldType numeric(int precision, int scale) {
    return Schema.FieldType.logicalType(
        FixedPrecisionNumeric.of(JDBCType.NUMERIC.getName(), precision, scale));
  }

  /** Base class for JDBC logical types. */
  public abstract static class JdbcLogicalType<T> implements Schema.LogicalType<T, T> {
    protected final String identifier;
    protected final Schema.FieldType baseType;
    protected final String argument;

    protected JdbcLogicalType(String identifier, Schema.FieldType baseType, String argument) {
      this.identifier = identifier;
      this.baseType = baseType;
      this.argument = argument;
    }

    @Override
    public String getIdentifier() {
      return identifier;
    }

    @Override
    public String getArgument() {
      return argument;
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
    public boolean equals(Object o) {
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

  /** Fixed length string types such as CHAR. */
  public static final class FixedLengthString extends JdbcLogicalType<String> {
    private final int length;

    public static FixedLengthString of(String identifier, int length) {
      return new FixedLengthString(identifier, length);
    }

    private FixedLengthString(String identifier, int length) {
      super(identifier, Schema.FieldType.STRING, String.valueOf(length));
      this.length = length;
    }

    @Override
    public String toInputType(String base) {
      checkArgument(base == null || base.length() <= length);
      return StringUtils.rightPad(base, length);
    }
  }

  /** Fixed length byte types such as BINARY. */
  public static final class FixedLengthBytes extends JdbcLogicalType<byte[]> {
    private final int length;

    public static FixedLengthBytes of(String identifier, int length) {
      return new FixedLengthBytes(identifier, length);
    }

    private FixedLengthBytes(String identifier, int length) {
      super(identifier, Schema.FieldType.BYTES, String.valueOf(length));
      this.length = length;
    }

    @Override
    public byte[] toInputType(byte[] base) {
      checkArgument(base == null || base.length <= length);
      if (base == null || base.length == length) {
        return base;
      } else {
        return Arrays.copyOf(base, length);
      }
    }
  }

  /** Variable length string types such as VARCHAR and LONGVARCHAR. */
  public static final class VariableLengthString extends JdbcLogicalType<String> {
    private final int maxLength;

    public static VariableLengthString of(String identifier, int maxLength) {
      return new VariableLengthString(identifier, maxLength);
    }

    private VariableLengthString(String identifier, int maxLength) {
      super(identifier, Schema.FieldType.STRING, String.valueOf(maxLength));
      this.maxLength = maxLength;
    }

    @Override
    public String toInputType(String base) {
      checkArgument(base == null || base.length() <= maxLength);
      return base;
    }
  }

  /** Variable length bytes types such as VARBINARY and LONGVARBINARY. */
  public static final class VariableLengthBytes extends JdbcLogicalType<byte[]> {
    private final int maxLength;

    public static VariableLengthBytes of(String identifier, int maxLength) {
      return new VariableLengthBytes(identifier, maxLength);
    }

    private VariableLengthBytes(String identifier, int maxLength) {
      super(identifier, Schema.FieldType.BYTES, String.valueOf(maxLength));
      this.maxLength = maxLength;
    }

    @Override
    public byte[] toInputType(byte[] base) {
      checkArgument(base == null || base.length <= maxLength);
      return base;
    }
  }

  /** Fixed precision numeric types such as NUMERIC. */
  public static final class FixedPrecisionNumeric extends JdbcLogicalType<BigDecimal> {
    private final int precision;
    private final int scale;

    public static FixedPrecisionNumeric of(String identifier, int precision, int scale) {
      return new FixedPrecisionNumeric(identifier, precision, scale);
    }

    private FixedPrecisionNumeric(String identifier, int precision, int scale) {
      super(identifier, Schema.FieldType.DECIMAL, precision + ":" + scale);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public BigDecimal toInputType(BigDecimal base) {
      checkArgument(base == null || (base.precision() == precision && base.scale() == scale));
      return base;
    }
  }
}

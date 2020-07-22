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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.PassThroughLogicalType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Beam {@link org.apache.beam.sdk.schemas.Schema.LogicalType} implementations of JDBC types. */
@Experimental(Kind.SCHEMAS)
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
  abstract static class JdbcLogicalType<T> implements Schema.LogicalType<T, T> {
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

  /** Fixed length string types such as CHAR. */
  static final class FixedLengthString extends JdbcLogicalType<String> {
    private final int length;

    static FixedLengthString of(String identifier, int length) {
      return new FixedLengthString(identifier, length);
    }

    private FixedLengthString(String identifier, int length) {
      super(identifier, FieldType.INT32, Schema.FieldType.STRING, length);
      this.length = length;
    }

    @Override
    public String toInputType(String base) {
      checkArgument(base == null || base.length() <= length);
      return StringUtils.rightPad(base, length);
    }
  }

  /** Fixed length byte types such as BINARY. */
  static final class FixedLengthBytes extends JdbcLogicalType<byte[]> {
    private final int length;

    static FixedLengthBytes of(String identifier, int length) {
      return new FixedLengthBytes(identifier, length);
    }

    private FixedLengthBytes(String identifier, int length) {
      super(identifier, FieldType.INT32, Schema.FieldType.BYTES, length);
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
  static final class VariableLengthString extends JdbcLogicalType<String> {
    private final int maxLength;

    static VariableLengthString of(String identifier, int maxLength) {
      return new VariableLengthString(identifier, maxLength);
    }

    private VariableLengthString(String identifier, int maxLength) {
      super(identifier, FieldType.INT32, Schema.FieldType.STRING, maxLength);
      this.maxLength = maxLength;
    }

    @Override
    public String toInputType(String base) {
      checkArgument(base == null || base.length() <= maxLength);
      return base;
    }
  }

  /** Variable length bytes types such as VARBINARY and LONGVARBINARY. */
  static final class VariableLengthBytes extends JdbcLogicalType<byte[]> {
    private final int maxLength;

    static VariableLengthBytes of(String identifier, int maxLength) {
      return new VariableLengthBytes(identifier, maxLength);
    }

    private VariableLengthBytes(String identifier, int maxLength) {
      super(identifier, FieldType.INT32, Schema.FieldType.BYTES, maxLength);
      this.maxLength = maxLength;
    }

    @Override
    public byte[] toInputType(byte[] base) {
      checkArgument(base == null || base.length <= maxLength);
      return base;
    }
  }

  /** Fixed precision numeric types such as NUMERIC. */
  static final class FixedPrecisionNumeric extends JdbcLogicalType<BigDecimal> {
    private final int precision;
    private final int scale;

    static FixedPrecisionNumeric of(String identifier, int precision, int scale) {
      Schema schema = Schema.builder().addInt32Field("precision").addInt32Field("scale").build();
      return new FixedPrecisionNumeric(schema, identifier, precision, scale);
    }

    private FixedPrecisionNumeric(
        Schema argumentSchema, String identifier, int precision, int scale) {
      super(
          identifier,
          FieldType.row(argumentSchema),
          Schema.FieldType.DECIMAL,
          Row.withSchema(argumentSchema).addValues(precision, scale).build());
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

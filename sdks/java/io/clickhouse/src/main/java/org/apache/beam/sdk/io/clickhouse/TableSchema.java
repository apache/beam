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
package org.apache.beam.sdk.io.clickhouse;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A descriptor for ClickHouse table schema. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class TableSchema implements Serializable {

  public abstract List<Column> columns();

  public static TableSchema of(Column... columns) {
    return new AutoValue_TableSchema(Arrays.asList(columns));
  }

  /**
   * Returns Beam equivalent of ClickHouse schema.
   *
   * @param tableSchema schema of ClickHouse table
   * @return Beam schema
   */
  public static Schema getEquivalentSchema(TableSchema tableSchema) {
    return tableSchema.columns().stream()
        .map(
            x -> {
              if (x.columnType().nullable()) {
                return Schema.Field.nullable(x.name(), getEquivalentFieldType(x.columnType()));
              } else {
                return Schema.Field.of(x.name(), getEquivalentFieldType(x.columnType()));
              }
            })
        .collect(Schema.toSchema());
  }

  /**
   * Returns Beam equivalent of ClickHouse column type.
   *
   * @param columnType type of ClickHouse column
   * @return Beam field type
   */
  public static Schema.FieldType getEquivalentFieldType(ColumnType columnType) {
    switch (columnType.typeName()) {
      case DATE:
      case DATETIME:
        return Schema.FieldType.DATETIME;

      case STRING:
        return Schema.FieldType.STRING;

      case FIXEDSTRING:
        int size = columnType.fixedStringSize(); // non-null for fixed strings
        return Schema.FieldType.logicalType(FixedBytes.of(size));

      case FLOAT32:
        return Schema.FieldType.FLOAT;

      case FLOAT64:
        return Schema.FieldType.DOUBLE;

      case INT8:
        return Schema.FieldType.BYTE;
      case INT16:
        return Schema.FieldType.INT16;
      case INT32:
        return Schema.FieldType.INT32;
      case INT64:
        return Schema.FieldType.INT64;

      case UINT8:
        return Schema.FieldType.INT16;
      case UINT16:
        return Schema.FieldType.INT32;
      case UINT32:
        return Schema.FieldType.INT64;
      case UINT64:
        return Schema.FieldType.INT64;

      case ARRAY:
        return Schema.FieldType.array(getEquivalentFieldType(columnType.arrayElementType()));

      case ENUM8:
      case ENUM16:
        return Schema.FieldType.STRING;
      case BOOL:
        return Schema.FieldType.BOOLEAN;
    }

    // not possible, errorprone checks for exhaustive switch
    throw new AssertionError("Unexpected type: " + columnType.typeName());
  }

  /** A column in ClickHouse table. */
  @AutoValue
  public abstract static class Column implements Serializable {
    public abstract String name();

    public abstract ColumnType columnType();

    public abstract @Nullable DefaultType defaultType();

    public abstract @Nullable Object defaultValue();

    public boolean materializedOrAlias() {
      return DefaultType.MATERIALIZED.equals(defaultType())
          || DefaultType.ALIAS.equals(defaultType());
    }

    public static Column of(String name, ColumnType columnType) {
      return of(name, columnType, null, null);
    }

    public static Column of(
        String name,
        ColumnType columnType,
        @Nullable DefaultType defaultType,
        @Nullable Object defaultValue) {
      return new AutoValue_TableSchema_Column(name, columnType, defaultType, defaultValue);
    }
  }

  /** An enumeration of possible types in ClickHouse. */
  public enum TypeName {
    // Primitive types
    DATE,
    DATETIME,
    ENUM8,
    ENUM16,
    FIXEDSTRING,
    FLOAT32,
    FLOAT64,
    INT8,
    INT16,
    INT32,
    INT64,
    STRING,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    // Composite type
    ARRAY,
    // Primitive type
    BOOL
  }

  /**
   * An enumeration of possible kinds of default values in ClickHouse.
   *
   * @see <a href="https://clickhouse.yandex/docs/en/single/#default-values">ClickHouse
   *     documentation</a>
   */
  public enum DefaultType {
    DEFAULT,
    MATERIALIZED,
    ALIAS;

    public static Optional<DefaultType> parse(String str) {
      if ("".equals(str)) {
        return Optional.empty();
      } else {
        return Optional.of(valueOf(str));
      }
    }
  }

  /** A descriptor for a column type. */
  @AutoValue
  public abstract static class ColumnType implements Serializable {
    public static final ColumnType DATE = ColumnType.of(TypeName.DATE);
    public static final ColumnType DATETIME = ColumnType.of(TypeName.DATETIME);
    public static final ColumnType FLOAT32 = ColumnType.of(TypeName.FLOAT32);
    public static final ColumnType FLOAT64 = ColumnType.of(TypeName.FLOAT64);
    public static final ColumnType INT8 = ColumnType.of(TypeName.INT8);
    public static final ColumnType INT16 = ColumnType.of(TypeName.INT16);
    public static final ColumnType INT32 = ColumnType.of(TypeName.INT32);
    public static final ColumnType INT64 = ColumnType.of(TypeName.INT64);
    public static final ColumnType STRING = ColumnType.of(TypeName.STRING);
    public static final ColumnType UINT8 = ColumnType.of(TypeName.UINT8);
    public static final ColumnType UINT16 = ColumnType.of(TypeName.UINT16);
    public static final ColumnType UINT32 = ColumnType.of(TypeName.UINT32);
    public static final ColumnType UINT64 = ColumnType.of(TypeName.UINT64);
    public static final ColumnType BOOL = ColumnType.of(TypeName.BOOL);

    // ClickHouse doesn't allow nested nullables, so boolean flag is enough
    public abstract boolean nullable();

    public abstract TypeName typeName();

    public abstract @Nullable Map<String, Integer> enumValues();

    public abstract @Nullable Integer fixedStringSize();

    public abstract @Nullable ColumnType arrayElementType();

    public ColumnType withNullable(boolean nullable) {
      return toBuilder().nullable(nullable).build();
    }

    public static ColumnType of(TypeName typeName) {
      return ColumnType.builder().typeName(typeName).nullable(false).build();
    }

    public static ColumnType nullable(TypeName typeName) {
      return ColumnType.builder().typeName(typeName).nullable(true).build();
    }

    public static ColumnType fixedString(int size) {
      return ColumnType.builder()
          .typeName(TypeName.FIXEDSTRING)
          .nullable(false)
          .fixedStringSize(size)
          .build();
    }

    public static ColumnType enum8(Map<String, Integer> enumValues) {
      return ColumnType.builder()
          .typeName(TypeName.ENUM8)
          .nullable(false)
          .enumValues(enumValues)
          .build();
    }

    public static ColumnType enum16(Map<String, Integer> enumValues) {
      return ColumnType.builder()
          .typeName(TypeName.ENUM16)
          .nullable(false)
          .enumValues(enumValues)
          .build();
    }

    public static ColumnType array(ColumnType arrayElementType) {
      return ColumnType.builder()
          .typeName(TypeName.ARRAY)
          // ClickHouse doesn't allow nullable arrays
          .nullable(false)
          .arrayElementType(arrayElementType)
          .build();
    }

    /**
     * Parse string with ClickHouse type to {@link ColumnType}.
     *
     * @param str string representation of ClickHouse type
     * @return type of ClickHouse column
     */
    public static ColumnType parse(String str) {
      try {
        return new org.apache.beam.sdk.io.clickhouse.impl.parser.ColumnTypeParser(
                new StringReader(str))
            .parse();
      } catch (org.apache.beam.sdk.io.clickhouse.impl.parser.ParseException e) {
        throw new IllegalArgumentException("failed to parse", e);
      }
    }

    /**
     * Get default value of a column based on expression.
     *
     * <p>E.g., "CREATE TABLE hits(id Int32, count Int32 DEFAULT &lt;str&gt;)"
     *
     * @param columnType type of ClickHouse expression
     * @param value ClickHouse expression
     * @return value of ClickHouse expression
     */
    public static Object parseDefaultExpression(ColumnType columnType, String value) {
      switch (columnType.typeName()) {
        case INT8:
          return Byte.valueOf(value);
        case INT16:
          return Short.valueOf(value);
        case INT32:
          return Integer.valueOf(value);
        case INT64:
          return Long.valueOf(value);
        case ENUM16:
        case ENUM8:
        case FIXEDSTRING:
        case STRING:
          return value;
        case UINT8:
          return Short.valueOf(value);
        case UINT16:
          return Integer.valueOf(value);
        case UINT32:
          return Long.valueOf(value);
        case UINT64:
          return Long.valueOf(value);
        case BOOL:
          return Boolean.valueOf(value);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + columnType);
      }
    }

    abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TableSchema_ColumnType.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder typeName(TypeName typeName);

      public abstract Builder arrayElementType(ColumnType arrayElementType);

      public abstract Builder nullable(boolean nullable);

      public abstract Builder enumValues(Map<String, Integer> enumValues);

      public abstract Builder fixedStringSize(Integer size);

      public abstract ColumnType build();
    }
  }
}

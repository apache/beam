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
package org.apache.beam.sdk.values;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.SchemaUtils;
import org.apache.beam.sdk.values.RowUtils.CapturingRowCases;
import org.apache.beam.sdk.values.RowUtils.FieldOverride;
import org.apache.beam.sdk.values.RowUtils.FieldOverrides;
import org.apache.beam.sdk.values.RowUtils.RowFieldMatcher;
import org.apache.beam.sdk.values.RowUtils.RowPosition;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

/**
 * {@link Row} is an immutable tuple-like schema to represent one element in a {@link PCollection}.
 * The fields are described with a {@link Schema}.
 *
 * <p>{@link Schema} contains the names and types for each field.
 *
 * <p>There are several ways to build a new Row object. To build a row from scratch using a schema
 * object, {@link Row#withSchema} can be used. Schema fields can be specified by name, and nested
 * fields can be specified using the field selection syntax. For example:
 *
 * <pre>{@code
 * Row row = Row.withSchema(schema)
 *              .withFieldValue("userId", "user1)
 *              .withFieldValue("location.city", "seattle")
 *              .withFieldValue("location.state", "wa")
 *              .build();
 * }</pre>
 *
 * <p>The {@link Row#fromRow} builder can be used to base a row off of another row. The builder can
 * be used to specify values for specific fields, and all the remaining values will be taken from
 * the original row. For example, the following produces a row identical to the above row except for
 * the location.city field.
 *
 * <pre>{@code
 * Row modifiedRow =
 *     Row.fromRow(row)
 *        .withFieldValue("location.city", "tacoma")
 *        .build();
 * }</pre>
 */
@SuppressWarnings({
  "nullness", // TODO(https://github.com/apache/beam/issues/20497)
  "rawtypes"
})
public abstract class Row implements Serializable {
  private final Schema schema;

  Row(Schema schema) {
    this.schema = schema;
  }

  // Abstract methods to be implemented by subclasses that handle object access.

  /** Get value by field index, {@link ClassCastException} is thrown if schema doesn't match. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public abstract <T extends @Nullable Object> T getValue(int fieldIdx);

  /** Return the size of data fields. */
  public abstract int getFieldCount();

  /** Return the list of raw unmodified data values to enable 0-copy code. */
  public abstract List<@Nullable Object> getValues();

  /** Return a list of data values. Any LogicalType values are returned as base values. * */
  public List<Object> getBaseValues() {
    return IntStream.range(0, getFieldCount())
        .mapToObj(i -> getBaseValue(i))
        .collect(Collectors.toList());
  }

  /** Get value by field name, {@link ClassCastException} is thrown if type doesn't match. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T extends @Nullable Object> T getValue(String fieldName) {
    return getValue(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Byte getByte(String fieldName) {
    return getByte(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTES} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public byte @Nullable [] getBytes(String fieldName) {
    return getBytes(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT16} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Short getInt16(String fieldName) {
    return getInt16(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT32} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Integer getInt32(String fieldName) {
    return getInt32(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT64} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Long getInt64(String fieldName) {
    return getInt64(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DECIMAL} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable BigDecimal getDecimal(String fieldName) {
    return getDecimal(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Float getFloat(String fieldName) {
    return getFloat(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Double getDouble(String fieldName) {
    return getDouble(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#STRING} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable String getString(String fieldName) {
    return getString(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable ReadableDateTime getDateTime(String fieldName) {
    return getDateTime(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BOOLEAN} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Boolean getBoolean(String fieldName) {
    return getBoolean(getSchema().indexOf(fieldName));
  }

  /**
   * Get an array value by field name, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  public <T> @Nullable Collection<T> getArray(String fieldName) {
    return getArray(getSchema().indexOf(fieldName));
  }

  /**
   * Get an iterable value by field name, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  public <T> @Nullable Iterable<T> getIterable(String fieldName) {
    return getIterable(getSchema().indexOf(fieldName));
  }

  /**
   * Get a MAP value by field name, {@link IllegalStateException} is thrown if schema doesn't match.
   */
  public <T1, T2> @Nullable Map<T1, T2> getMap(String fieldName) {
    return getMap(getSchema().indexOf(fieldName));
  }

  /**
   * Returns the Logical Type input type for this field. {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public <T extends @Nullable Object> T getLogicalTypeValue(String fieldName, Class<T> clazz) {
    return getLogicalTypeValue(getSchema().indexOf(fieldName), clazz);
  }

  /**
   * Returns the base type for this field. If this is a logical type, we convert to the base value.
   * Otherwise the field itself is returned.
   */
  public <T extends @Nullable Object> T getBaseValue(String fieldName, Class<T> clazz) {
    return getBaseValue(getSchema().indexOf(fieldName), clazz);
  }

  /**
   * Returns the base type for this field. If this is a logical type, we convert to the base value.
   * Otherwise the field itself is returned.
   */
  public @Nullable Object getBaseValue(String fieldName) {
    return getBaseValue(fieldName, Object.class);
  }

  /**
   * Get a {@link TypeName#ROW} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Row getRow(String fieldName) {
    return getRow(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTE} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Byte getByte(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#BYTES} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public byte @Nullable [] getBytes(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT16} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Short getInt16(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT32} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Integer getInt32(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Float getFloat(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Double getDouble(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT64} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  public @Nullable Long getInt64(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link String} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  public @Nullable String getString(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field index, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public @Nullable ReadableDateTime getDateTime(int idx) {
    ReadableInstant instant = getValue(idx);
    return instant == null ? null : new DateTime(instant).withZone(instant.getZone());
  }

  /**
   * Get a {@link BigDecimal} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  public @Nullable BigDecimal getDecimal(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Boolean} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  public @Nullable Boolean getBoolean(int idx) {
    return getValue(idx);
  }

  /**
   * Get an array value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  public <T> @Nullable Collection<T> getArray(int idx) {
    return getValue(idx);
  }

  /**
   * Get an iterable value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  public <T> @Nullable Iterable<T> getIterable(int idx) {
    return getValue(idx);
  }

  /**
   * Get a MAP value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  public <T1, T2> @Nullable Map<T1, T2> getMap(int idx) {
    return getValue(idx);
  }

  /**
   * Returns the Logical Type input type for this field. {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  public <T extends @Nullable Object> T getLogicalTypeValue(int idx, Class<T> clazz) {
    return (T) getValue(idx);
  }

  /**
   * Returns the base type for this field. If this is a logical type, we convert to the base value.
   * Otherwise the field itself is returned.
   */
  public <T extends @Nullable Object> T getBaseValue(int idx, Class<T> clazz) {
    Object value = getValue(idx);
    FieldType fieldType = getSchema().getField(idx).getType();
    if (fieldType.getTypeName().isLogicalType() && value != null) {
      while (fieldType.getTypeName().isLogicalType()) {
        Schema.LogicalType<Object, T> logicalType =
            (Schema.LogicalType<Object, T>) fieldType.getLogicalType();
        value = logicalType.toBaseType(value);
        fieldType = fieldType.getLogicalType().getBaseType();
      }
    }
    return (T) value;
  }

  /**
   * Returns the base type for this field. If this is a logical type, we convert to the base value.
   * Otherwise the field itself is returned.
   */
  public @Nullable Object getBaseValue(int idx) {
    return getBaseValue(idx, Object.class);
  }

  /**
   * Get a {@link Row} value by field index, {@link IllegalStateException} is thrown if schema
   * doesn't match.
   */
  public @Nullable Row getRow(int idx) {
    return getValue(idx);
  }

  /** Return {@link Schema} which describes the fields. */
  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Row)) {
      return false;
    }
    Row other = (Row) o;

    if (!Objects.equals(getSchema(), other.getSchema())) {
      return false;
    }

    for (int i = 0; i < getFieldCount(); i++) {
      if (!Equals.deepEquals(getValue(i), other.getValue(i), getSchema().getField(i).getType())) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int hashCode() {
    int h = 1;
    for (int i = 0; i < getFieldCount(); i++) {
      h = 31 * h + Equals.deepHashCode(getValue(i), getSchema().getField(i).getType());
    }

    return h;
  }

  public static class Equals {
    public static boolean deepEquals(Object a, Object b, Schema.FieldType fieldType) {
      if (a == null || b == null) {
        return a == b;
      } else if (fieldType.getTypeName() == TypeName.LOGICAL_TYPE) {
        Schema.LogicalType<Object, Object> logicalType =
            (Schema.LogicalType<Object, Object>) fieldType.getLogicalType();
        return deepEquals(
            SchemaUtils.toLogicalBaseType(logicalType, a),
            SchemaUtils.toLogicalBaseType(logicalType, b),
            logicalType.getBaseType());
      } else if (fieldType.getTypeName() == Schema.TypeName.BYTES) {
        return Arrays.equals((byte[]) a, (byte[]) b);
      } else if (fieldType.getTypeName() == TypeName.ARRAY) {
        return deepEqualsForCollection(
            (Collection<Object>) a, (Collection<Object>) b, fieldType.getCollectionElementType());
      } else if (fieldType.getTypeName() == TypeName.ITERABLE) {
        return deepEqualsForIterable(
            (Iterable<Object>) a, (Iterable<Object>) b, fieldType.getCollectionElementType());
      } else if (fieldType.getTypeName() == Schema.TypeName.MAP) {
        return deepEqualsForMap(
            (Map<Object, Object>) a, (Map<Object, Object>) b, fieldType.getMapValueType());
      } else {
        return Objects.equals(a, b);
      }
    }

    public static int deepHashCode(Object a, Schema.FieldType fieldType) {
      if (a == null) {
        return 0;
      } else if (fieldType.getTypeName() == TypeName.LOGICAL_TYPE) {
        return deepHashCode(a, fieldType.getLogicalType().getBaseType());
      } else if (fieldType.getTypeName() == Schema.TypeName.BYTES) {
        return Arrays.hashCode((byte[]) a);
      } else if (fieldType.getTypeName().isCollectionType()) {
        return deepHashCodeForIterable((Iterable<Object>) a, fieldType.getCollectionElementType());
      } else if (fieldType.getTypeName() == Schema.TypeName.MAP) {
        return deepHashCodeForMap(
            (Map<Object, Object>) a, fieldType.getMapKeyType(), fieldType.getMapValueType());
      } else {
        return Objects.hashCode(a);
      }
    }

    static <K, V> boolean deepEqualsForMap(Map<K, V> a, Map<K, V> b, Schema.FieldType valueType) {
      if (a == b) {
        return true;
      }

      if (a.size() != b.size()) {
        return false;
      }

      for (Map.Entry<K, V> e : a.entrySet()) {
        K key = e.getKey();
        V value = e.getValue();
        V otherValue = b.get(key);

        if (value == null) {
          if (otherValue != null || !b.containsKey(key)) {
            return false;
          }
        } else {
          if (!deepEquals(value, otherValue, valueType)) {
            return false;
          }
        }
      }

      return true;
    }

    static int deepHashCodeForMap(
        Map<Object, Object> a, Schema.FieldType keyType, Schema.FieldType valueType) {
      int h = 0;

      for (Map.Entry<Object, Object> e : a.entrySet()) {
        Object key = e.getKey();
        Object value = e.getValue();

        h += deepHashCode(key, keyType) ^ deepHashCode(value, valueType);
      }

      return h;
    }

    static boolean deepEqualsForCollection(
        Collection<Object> a, Collection<Object> b, Schema.FieldType elementType) {
      if (a == b) {
        return true;
      }

      if (a.size() != b.size()) {
        return false;
      }

      return deepEqualsForIterable(a, b, elementType);
    }

    static boolean deepEqualsForIterable(
        Iterable<Object> a, Iterable<Object> b, Schema.FieldType elementType) {
      if (a == b) {
        return true;
      }
      Iterator<Object> bIter = b.iterator();
      for (Object currentA : a) {
        if (!bIter.hasNext()) {
          return false;
        }
        if (!deepEquals(currentA, bIter.next(), elementType)) {
          return false;
        }
      }
      return !bIter.hasNext();
    }

    static int deepHashCodeForIterable(Iterable<Object> a, Schema.FieldType elementType) {
      int h = 1;
      for (Object o : a) {
        h = 31 * h + deepHashCode(o, elementType);
      }

      return h;
    }
  }

  @Override
  public String toString() {
    return toString(true);
  }

  /** Convert Row to String. */
  public String toString(boolean includeFieldNames) {
    StringBuilder builder = new StringBuilder();
    builder.append("Row: ");
    builder.append(System.lineSeparator());
    for (int i = 0; i < getSchema().getFieldCount(); ++i) {
      Schema.Field field = getSchema().getField(i);
      if (includeFieldNames) {
        builder.append(field.getName() + ":");
      }
      builder.append(toString(field.getType(), getValue(i), includeFieldNames));
      builder.append(System.lineSeparator());
    }
    return builder.toString();
  }

  private String toString(Schema.FieldType fieldType, Object value, boolean includeFieldNames) {
    if (value == null) {
      return "<null>";
    }
    StringBuilder builder = new StringBuilder();
    switch (fieldType.getTypeName()) {
      case ARRAY:
      case ITERABLE:
        builder.append("[");
        for (Object element : (Iterable<?>) value) {
          builder.append(
              toString(fieldType.getCollectionElementType(), element, includeFieldNames));
          builder.append(", ");
        }
        builder.append("]");
        break;
      case MAP:
        builder.append("{");
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
          builder.append("(");
          builder.append(toString(fieldType.getMapKeyType(), entry.getKey(), includeFieldNames));
          builder.append(", ");
          builder.append(
              toString(fieldType.getMapValueType(), entry.getValue(), includeFieldNames));
          builder.append("), ");
        }
        builder.append("}");
        break;
      case BYTES:
        builder.append(Arrays.toString((byte[]) value));
        break;
      case ROW:
        builder.append(((Row) value).toString(includeFieldNames));
        break;
      default:
        builder.append(value);
    }
    return builder.toString();
  }

  /**
   * Creates a row builder with specified {@link #getSchema()}. {@link Builder#build()} will throw
   * an {@link IllegalArgumentException} if number of fields in {@link #getSchema()} does not match
   * the number of fields specified. If any of the arguments don't match the expected types for the
   * schema fields, {@link Builder#build()} will throw a {@link ClassCastException}.
   */
  public static Builder withSchema(Schema schema) {
    return new Builder(schema);
  }

  /**
   * Creates a row builder based on the specified row. Field values in the new row can be explicitly
   * set using {@link FieldValueBuilder#withFieldValue}. Any values not so overridden will be the
   * same as the values in the original row.
   */
  public static FieldValueBuilder fromRow(Row row) {
    return new FieldValueBuilder(row.getSchema(), row);
  }

  /** Builder for {@link Row} that bases a row on another row. */
  public static class FieldValueBuilder {
    private final Schema schema;
    private final @Nullable Row sourceRow;
    private final FieldOverrides fieldOverrides;

    private FieldValueBuilder(Schema schema, @Nullable Row sourceRow) {
      this.schema = schema;
      this.sourceRow = sourceRow;
      this.fieldOverrides = new FieldOverrides(schema);
    }

    public Schema getSchema() {
      return schema;
    }

    /**
     * Set a field value using the field name. Nested values can be set using the field selection
     * syntax.
     */
    public FieldValueBuilder withFieldValue(String fieldName, Object value) {
      return withFieldValue(FieldAccessDescriptor.withFieldNames(fieldName), value);
    }

    /** Set a field value using the field id. */
    public FieldValueBuilder withFieldValue(Integer fieldId, Object value) {
      return withFieldValue(FieldAccessDescriptor.withFieldIds(fieldId), value);
    }

    /** Set a field value using a FieldAccessDescriptor. */
    public FieldValueBuilder withFieldValue(
        FieldAccessDescriptor fieldAccessDescriptor, Object value) {
      FieldAccessDescriptor fieldAccess = fieldAccessDescriptor.resolve(getSchema());
      checkArgument(fieldAccess.referencesSingleField(), "");
      fieldOverrides.addOverride(fieldAccess, new FieldOverride(value));
      return this;
    }

    /**
     * Sets field values using the field names. Nested values can be set using the field selection
     * syntax.
     */
    public FieldValueBuilder withFieldValues(Map<String, Object> values) {
      values.entrySet().stream()
          .forEach(
              e ->
                  fieldOverrides.addOverride(
                      FieldAccessDescriptor.withFieldNames(e.getKey()).resolve(getSchema()),
                      new FieldOverride(e.getValue())));
      return this;
    }

    /**
     * Sets field values using the FieldAccessDescriptors. Nested values can be set using the field
     * selection syntax.
     */
    public FieldValueBuilder withFieldAccessDescriptors(Map<FieldAccessDescriptor, Object> values) {
      values.entrySet().stream()
          .forEach(e -> fieldOverrides.addOverride(e.getKey(), new FieldOverride(e.getValue())));
      return this;
    }

    public Row build() {
      Row row =
          (Row)
              new RowFieldMatcher()
                  .match(
                      new CapturingRowCases(getSchema(), this.fieldOverrides),
                      FieldType.row(getSchema()),
                      new RowPosition(FieldAccessDescriptor.create()),
                      sourceRow);
      return row;
    }
  }

  /** Builder for {@link Row}. */
  public static class Builder {
    private List<Object> values = Lists.newArrayList();
    private final Schema schema;

    Builder(Schema schema) {
      this.schema = schema;
    }

    /** Return the schema for the row being built. */
    public Schema getSchema() {
      return schema;
    }

    /**
     * Set a field value using the field name. Nested values can be set using the field selection
     * syntax.
     */
    public FieldValueBuilder withFieldValue(String fieldName, Object value) {
      checkState(values.isEmpty());
      return new FieldValueBuilder(schema, null).withFieldValue(fieldName, value);
    }

    /** Set a field value using the field id. */
    public FieldValueBuilder withFieldValue(Integer fieldId, Object value) {
      checkState(values.isEmpty());
      return new FieldValueBuilder(schema, null).withFieldValue(fieldId, value);
    }

    /** Set a field value using a FieldAccessDescriptor. */
    public FieldValueBuilder withFieldValue(
        FieldAccessDescriptor fieldAccessDescriptor, Object value) {
      checkState(values.isEmpty());
      return new FieldValueBuilder(schema, null).withFieldValue(fieldAccessDescriptor, value);
    }

    /**
     * Sets field values using the field names. Nested values can be set using the field selection
     * syntax.
     */
    public FieldValueBuilder withFieldValues(Map<String, Object> values) {
      checkState(this.values.isEmpty());
      return new FieldValueBuilder(schema, null).withFieldValues(values);
    }

    // The following methods allow appending a list of values to the Builder object. The values must
    // be in the same
    // order as the fields in the row. These methods cannot be used in conjunction with
    // withFieldValue or
    // withFieldValues.

    public Builder addValue(@Nullable Object value) {
      this.values.add(value);
      return this;
    }

    public Builder addValues(List<@Nullable Object> values) {
      this.values.addAll(values);
      return this;
    }

    public Builder addValues(Object... values) {
      return addValues(Arrays.asList(values));
    }

    public <T> Builder addArray(Collection<T> values) {
      this.values.add(values);
      return this;
    }

    public Builder addArray(Object... values) {
      addArray(Arrays.asList(values));
      return this;
    }

    public <T> Builder addIterable(Iterable<T> values) {
      this.values.add(values);
      return this;
    }

    // Values are attached. No verification is done, and no conversions are done. LogicalType values
    // must be specified as the base type. This method should be used with great care, as no
    // validation is done. If
    // incorrect values are passed in, it could result in strange errors later in the pipeline. This
    // method is largely
    // used internal to Beam.
    @Internal
    public Row attachValues(List<@Nullable Object> attachedValues) {
      checkState(this.values.isEmpty());
      return new RowWithStorage(schema, attachedValues);
    }

    public Row attachValues(Object... values) {
      return attachValues(Arrays.asList(values));
    }

    public int nextFieldId() {
      return values.size();
    }

    @Internal
    public <T extends @NonNull Object> Row withFieldValueGetters(
        Factory<List<FieldValueGetter<T, ?>>> fieldValueGetterFactory, T getterTarget) {
      checkState(getterTarget != null, "getters require withGetterTarget.");
      return new RowWithGetters<>(schema, fieldValueGetterFactory, getterTarget);
    }

    public Row build() {
      checkNotNull(schema);

      if (!values.isEmpty() && values.size() != schema.getFieldCount()) {
        throw new IllegalArgumentException(
            "Row expected "
                + schema.getFieldCount()
                + String.format(
                    " fields (%s).",
                    schema.getFields().stream()
                        .map(Object::toString)
                        .collect(Collectors.joining(", ")))
                + " initialized with "
                + values.size()
                + " fields.");
      }

      if (!values.isEmpty()) {
        FieldOverrides fieldOverrides = new FieldOverrides(schema, this.values);
        if (!fieldOverrides.isEmpty()) {
          return (Row)
              new RowFieldMatcher()
                  .match(
                      new CapturingRowCases(schema, fieldOverrides),
                      FieldType.row(schema),
                      new RowPosition(FieldAccessDescriptor.create()),
                      null);
        }
      }
      return new RowWithStorage(schema, Collections.emptyList());
    }
  }

  /** Creates a {@link Row} from the list of values and {@link #getSchema()}. */
  public static <T> Collector<T, List<Object>, Row> toRow(Schema schema) {
    return Collector.of(
        () -> new ArrayList<>(schema.getFieldCount()),
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        values -> Row.withSchema(schema).addValues(values).build());
  }

  /** Creates a new record filled with nulls. */
  public static Row nullRow(Schema schema) {
    return Row.withSchema(schema)
        .addValues(Collections.nCopies(schema.getFieldCount(), null))
        .build();
  }

  /** Returns an equivalent {@link Row} with fields lexicographically sorted by their name. */
  public Row sorted() {
    Schema sortedSchema = getSchema().sorted();
    return sortedSchema.getFields().stream()
        .map(
            field -> {
              if (field.getType().getRowSchema() != null) {
                Row innerRow = getValue(field.getName());
                if (innerRow != null) {
                  return innerRow.sorted();
                }
              }
              return (Object) getValue(field.getName());
            })
        .collect(Row.toRow(sortedSchema));
  }

  /** Returns an equivalent {@link Row} with `snake_case` field names. */
  public Row toSnakeCase() {
    return getSchema().getFields().stream()
        .map(
            field -> {
              if (field.getType().getRowSchema() != null) {
                Row innerRow = getValue(field.getName());
                if (innerRow != null) {
                  return innerRow.toSnakeCase();
                }
              }
              return (Object) getValue(field.getName());
            })
        .collect(toRow(getSchema().toSnakeCase()));
  }

  /** Returns an equivalent {@link Row} with `lowerCamelCase` field names. */
  public Row toCamelCase() {
    return getSchema().getFields().stream()
        .map(
            field -> {
              if (field.getType().getRowSchema() != null) {
                Row innerRow = getValue(field.getName());
                if (innerRow != null) {
                  return innerRow.toCamelCase();
                }
              }
              return (Object) getValue(field.getName());
            })
        .collect(toRow(getSchema().toCamelCase()));
  }
}

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

import static org.apache.beam.sdk.values.SchemaVerification.verifyRowValues;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Factory;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;

/**
 * {@link Row} is an immutable tuple-like schema to represent one element in a {@link PCollection}.
 * The fields are described with a {@link Schema}.
 *
 * <p>{@link Schema} contains the names for each field and the coder for the whole record,
 * {see @link Schema#getRowCoder()}.
 */
@Experimental(Kind.SCHEMAS)
public abstract class Row implements Serializable {
  private final Schema schema;

  Row(Schema schema) {
    this.schema = schema;
  }

  // Abstract methods to be implemented by subclasses that handle object access.

  /** Get value by field index, {@link ClassCastException} is thrown if schema doesn't match. */
  @Nullable
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public abstract <T> T getValue(int fieldIdx);

  /** Return the size of data fields. */
  public abstract int getFieldCount();
  /** Return the list of data values. */
  public abstract List<Object> getValues();

  /** Get value by field name, {@link ClassCastException} is thrown if type doesn't match. */
  @Nullable
  @SuppressWarnings("TypeParameterUnusedInFormals")
  public <T> T getValue(String fieldName) {
    return getValue(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Byte getByte(String fieldName) {
    return getByte(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTES} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public byte[] getBytes(String fieldName) {
    return getBytes(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT16} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Short getInt16(String fieldName) {
    return getInt16(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT32} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Integer getInt32(String fieldName) {
    return getInt32(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#INT64} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Long getInt64(String fieldName) {
    return getInt64(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DECIMAL} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public BigDecimal getDecimal(String fieldName) {
    return getDecimal(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Float getFloat(String fieldName) {
    return getFloat(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Double getDouble(String fieldName) {
    return getDouble(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#STRING} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public String getString(String fieldName) {
    return getString(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public ReadableDateTime getDateTime(String fieldName) {
    return getDateTime(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BOOLEAN} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Boolean getBoolean(String fieldName) {
    return getBoolean(getSchema().indexOf(fieldName));
  }

  /**
   * Get an array value by field name, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  @Nullable
  public <T> Collection<T> getArray(String fieldName) {
    return getArray(getSchema().indexOf(fieldName));
  }

  /**
   * Get an iterable value by field name, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  @Nullable
  public <T> Iterable<T> getIterable(String fieldName) {
    return getIterable(getSchema().indexOf(fieldName));
  }

  /**
   * Get a MAP value by field name, {@link IllegalStateException} is thrown if schema doesn't match.
   */
  @Nullable
  public <T1, T2> Map<T1, T2> getMap(String fieldName) {
    return getMap(getSchema().indexOf(fieldName));
  }

  /**
   * Returns the Logical Type input type for this field. {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public <T> T getLogicalTypeValue(String fieldName, Class<T> clazz) {
    return getLogicalTypeValue(getSchema().indexOf(fieldName), clazz);
  }

  /**
   * Get a {@link TypeName#ROW} value by field name, {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Row getRow(String fieldName) {
    return getRow(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link TypeName#BYTE} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Byte getByte(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#BYTES} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public byte[] getBytes(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT16} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Short getInt16(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT32} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Integer getInt32(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#FLOAT} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Float getFloat(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#DOUBLE} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Double getDouble(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#INT64} value by field index, {@link ClassCastException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public Long getInt64(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link String} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  @Nullable
  public String getString(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link TypeName#DATETIME} value by field index, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  @Nullable
  public ReadableDateTime getDateTime(int idx) {
    ReadableInstant instant = getValue(idx);
    return instant == null ? null : new DateTime(instant).withZone(instant.getZone());
  }

  /**
   * Get a {@link BigDecimal} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  @Nullable
  public BigDecimal getDecimal(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Boolean} value by field index, {@link ClassCastException} is thrown if schema
   * doesn't match.
   */
  @Nullable
  public Boolean getBoolean(int idx) {
    return getValue(idx);
  }

  /**
   * Get an array value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  @Nullable
  public <T> Collection<T> getArray(int idx) {
    return getValue(idx);
  }

  /**
   * Get an iterable value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  @Nullable
  public <T> Iterable<T> getIterable(int idx) {
    return getValue(idx);
  }

  /**
   * Get a MAP value by field index, {@link IllegalStateException} is thrown if schema doesn't
   * match.
   */
  @Nullable
  public <T1, T2> Map<T1, T2> getMap(int idx) {
    return getValue(idx);
  }

  /**
   * Returns the Logical Type input type for this field. {@link IllegalStateException} is thrown if
   * schema doesn't match.
   */
  @Nullable
  public <T> T getLogicalTypeValue(int idx, Class<T> clazz) {
    LogicalType logicalType = checkNotNull(getSchema().getField(idx).getType().getLogicalType());
    return (T) logicalType.toInputType(getValue(idx));
  }

  /**
   * Get a {@link Row} value by field index, {@link IllegalStateException} is thrown if schema
   * doesn't match.
   */
  @Nullable
  public Row getRow(int idx) {
    return getValue(idx);
  }

  /** Return {@link Schema} which describes the fields. */
  public Schema getSchema() {
    return schema;
  }

  @Override
  public boolean equals(Object o) {
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
        return deepEquals(a, b, fieldType.getLogicalType().getBaseType());
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
    return "Row:" + Arrays.deepToString(Iterables.toArray(getValues(), Object.class));
  }

  /**
   * Creates a record builder with specified {@link #getSchema()}. {@link Builder#build()} will
   * throw an {@link IllegalArgumentException} if number of fields in {@link #getSchema()} does not
   * match the number of fields specified.
   */
  public static Builder withSchema(Schema schema) {
    return new Builder(schema);
  }

  /** Builder for {@link Row}. */
  public static class Builder {
    private List<Object> values = Lists.newArrayList();
    private boolean attached = false;
    @Nullable private Factory<List<FieldValueGetter>> fieldValueGetterFactory;
    @Nullable private Object getterTarget;
    private Schema schema;

    Builder(Schema schema) {
      this.schema = schema;
    }

    public int nextFieldId() {
      if (fieldValueGetterFactory != null) {
        throw new RuntimeException("Not supported");
      }
      return values.size();
    }

    public Schema getSchema() {
      return schema;
    }

    public Builder addValue(@Nullable Object values) {
      this.values.add(values);
      return this;
    }

    public Builder addValues(List<Object> values) {
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

    // Values are attached. No verification is done, and no conversions are done. LogicalType
    // values must be specified as the base type.
    public Builder attachValues(List<Object> values) {
      this.attached = true;
      this.values = values;
      return this;
    }

    public Builder attachValues(Object... values) {
      return attachValues(Arrays.asList(values));
    }

    public Builder withFieldValueGetters(
        Factory<List<FieldValueGetter>> fieldValueGetterFactory, Object getterTarget) {
      this.fieldValueGetterFactory = fieldValueGetterFactory;
      this.getterTarget = getterTarget;
      return this;
    }

    public Row build() {
      checkNotNull(schema);
      if (!this.values.isEmpty() && fieldValueGetterFactory != null) {
        throw new IllegalArgumentException("Cannot specify both values and getters.");
      }
      if (!this.values.isEmpty()) {
        List<Object> storageValues = attached ? this.values : verifyRowValues(schema, this.values);
        checkState(getterTarget == null, "withGetterTarget requires getters.");
        return new RowWithStorage(schema, storageValues);
      } else if (fieldValueGetterFactory != null) {
        checkState(getterTarget != null, "getters require withGetterTarget.");
        return new RowWithGetters(schema, fieldValueGetterFactory, getterTarget);
      } else {
        return new RowWithStorage(schema, Collections.emptyList());
      }
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
}

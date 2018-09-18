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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collector;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldValueGetterFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;
import org.joda.time.ReadableInstant;
import org.joda.time.base.AbstractInstant;

/**
 * {@link Row} is an immutable tuple-like schema to represent one element in a {@link PCollection}.
 * The fields are described with a {@link Schema}.
 *
 * <p>{@link Schema} contains the names for each field and the coder for the whole record,
 * {see @link Schema#getRowCoder()}.
 */
@Experimental
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
  public <T> List<T> getArray(String fieldName) {
    return getArray(getSchema().indexOf(fieldName));
  }

  /**
   * Get a MAP value by field name, {@link IllegalStateException} is thrown if schema doesn't match.
   */
  @Nullable
  public <T1, T2> Map<T1, T2> getMap(String fieldName) {
    return getMap(getSchema().indexOf(fieldName));
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
  public <T> List<T> getArray(int idx) {
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
    return Objects.equals(getSchema(), other.getSchema())
        && Objects.equals(getValues(), other.getValues());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSchema(), getValues());
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
    @Nullable private FieldValueGetterFactory fieldValueGetterFactory;
    @Nullable private Object getterTarget;
    private Schema schema;

    Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder addValue(Object values) {
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

    public <T> Builder addArray(List<T> values) {
      this.values.add(values);
      return this;
    }

    public Builder addArray(Object... values) {
      addArray(Arrays.asList(values));
      return this;
    }

    public Builder attachValues(List<Object> values) {
      this.attached = true;
      this.values = values;
      return this;
    }

    public Builder withFieldValueGetters(
        FieldValueGetterFactory fieldValueGetterFactory, Object getterTarget) {
      this.fieldValueGetterFactory = fieldValueGetterFactory;
      this.getterTarget = getterTarget;
      return this;
    }

    private List<Object> verify(Schema schema, List<Object> values) {
      List<Object> verifiedValues = Lists.newArrayListWithCapacity(values.size());
      if (schema.getFieldCount() != values.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Field count in Schema (%s) and values (%s) must match",
                schema.getFieldNames(), values));
      }
      for (int i = 0; i < values.size(); ++i) {
        Object value = values.get(i);
        Schema.Field field = schema.getField(i);
        if (value == null) {
          if (!field.getNullable()) {
            throw new IllegalArgumentException(
                String.format("Field %s is not nullable", field.getName()));
          }
          verifiedValues.add(null);
        } else {
          verifiedValues.add(verify(value, field.getType(), field.getName()));
        }
      }
      return verifiedValues;
    }

    private Object verify(Object value, FieldType type, String fieldName) {
      if (TypeName.ARRAY.equals(type.getTypeName())) {
        List<Object> arrayElements = verifyArray(value, type.getCollectionElementType(), fieldName);
        return arrayElements;
      } else if (TypeName.MAP.equals(type.getTypeName())) {
        Map<Object, Object> mapElements =
            verifyMap(value, type.getMapKeyType().getTypeName(), type.getMapValueType(), fieldName);
        return mapElements;
      } else if (TypeName.ROW.equals(type.getTypeName())) {
        return verifyRow(value, fieldName);
      } else {
        return verifyPrimitiveType(value, type.getTypeName(), fieldName);
      }
    }

    private List<Object> verifyArray(
        Object value, FieldType collectionElementType, String fieldName) {
      if (!(value instanceof List)) {
        throw new IllegalArgumentException(
            String.format(
                "For field name %s and array type expected List class. Instead "
                    + "class type was %s.",
                fieldName, value.getClass()));
      }
      List<Object> valueList = (List<Object>) value;
      List<Object> verifiedList = Lists.newArrayListWithCapacity(valueList.size());
      for (Object listValue : valueList) {
        verifiedList.add(verify(listValue, collectionElementType, fieldName));
      }
      return verifiedList;
    }

    private Map<Object, Object> verifyMap(
        Object value, TypeName keyTypeName, FieldType valueType, String fieldName) {
      if (!(value instanceof Map)) {
        throw new IllegalArgumentException(
            String.format(
                "For field name %s and map type expected Map class. Instead "
                    + "class type was %s.",
                fieldName, value.getClass()));
      }
      Map<Object, Object> valueMap = (Map<Object, Object>) value;
      Map<Object, Object> verifiedMap = Maps.newHashMapWithExpectedSize(valueMap.size());
      for (Entry<Object, Object> kv : valueMap.entrySet()) {
        verifiedMap.put(
            verifyPrimitiveType(kv.getKey(), keyTypeName, fieldName),
            verify(kv.getValue(), valueType, fieldName));
      }
      return verifiedMap;
    }

    private Row verifyRow(Object value, String fieldName) {
      if (!(value instanceof Row)) {
        throw new IllegalArgumentException(
            String.format(
                "For field name %s expected Row type. " + "Instead class type was %s.",
                fieldName, value.getClass()));
      }
      // No need to recursively validate the nested Row, since there's no way to build the
      // Row object without it validating.
      return (Row) value;
    }

    private Object verifyPrimitiveType(Object value, TypeName type, String fieldName) {
      if (type.isDateType()) {
        return verifyDateTime(value, fieldName);
      } else {
        switch (type) {
          case BYTE:
            if (value instanceof Byte) {
              return value;
            }
            break;
          case BYTES:
            if (value instanceof ByteBuffer) {
              return ((ByteBuffer) value).array();
            } else if (value instanceof byte[]) {
              return (byte[]) value;
            }
            break;
          case INT16:
            if (value instanceof Short) {
              return value;
            }
            break;
          case INT32:
            if (value instanceof Integer) {
              return value;
            }
            break;
          case INT64:
            if (value instanceof Long) {
              return value;
            }
            break;
          case DECIMAL:
            if (value instanceof BigDecimal) {
              return value;
            }
            break;
          case FLOAT:
            if (value instanceof Float) {
              return value;
            }
            break;
          case DOUBLE:
            if (value instanceof Double) {
              return value;
            }
            break;
          case STRING:
            if (value instanceof String) {
              return value;
            }
            break;
          case BOOLEAN:
            if (value instanceof Boolean) {
              return value;
            }
            break;
          default:
            // Shouldn't actually get here, but we need this case to satisfy linters.
            throw new IllegalArgumentException(
                String.format("Not a primitive type for field name %s: %s", fieldName, type));
        }
        throw new IllegalArgumentException(
            String.format(
                "For field name %s and type %s found incorrect class type %s",
                fieldName, type, value.getClass()));
      }
    }

    private Instant verifyDateTime(Object value, String fieldName) {
      // We support the following classes for datetimes.
      if (value instanceof AbstractInstant) {
        return ((AbstractInstant) value).toInstant();
      } else {
        throw new IllegalArgumentException(
            String.format(
                "For field name %s and DATETIME type got unexpected class %s ",
                fieldName, value.getClass()));
      }
    }

    public Row build() {
      checkNotNull(schema);
      if (!this.values.isEmpty() && fieldValueGetterFactory != null) {
        throw new IllegalArgumentException(("Cannot specify both values and getters."));
      }
      if (!this.values.isEmpty()) {
        List<Object> storageValues = attached ? this.values : verify(schema, this.values);
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

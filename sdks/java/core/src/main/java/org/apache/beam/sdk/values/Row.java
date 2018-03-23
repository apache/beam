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

import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.Collector;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

/**
 * {@link Row} is an immutable tuple-like schema to represent one element in a
 * {@link PCollection}. The fields are described with a {@link Schema}.
 *
 * <p>{@link Schema} contains the names for each field and the coder for the whole
 * record, {see @link Schema#getRowCoder()}.
 *
 * <p>TODO: Rename accessors to match types in Schema. reuvenlax
 */
@Experimental
@AutoValue
public abstract class Row implements Serializable {

  /**
   * Creates a {@link Row} from the list of values and {@link #getSchema()}.
   */
  public static <T> Collector<T, List<Object>, Row> toRow(
      Schema schema) {
    return Collector.of(
        () -> new ArrayList<>(schema.getFieldCount()),
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        values -> Row.withSchema(schema).addValues(values).build());
  }

  /**
   * Creates a new record filled with nulls.
   */
  public static Row nullRow(Schema schema) {
    return
        Row
            .withSchema(schema)
            .addValues(Collections.nCopies(schema.getFieldCount(), null))
            .build();
  }

  /**
   * Get value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public <T> T getValue(String fieldName) {
    return getValue(getSchema().indexOf(fieldName));
  }

  /**
   * Get value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  @Nullable
  public <T> T getValue(int fieldIdx) {
    return (T) getValues().get(fieldIdx);
  }

  /**
   * Get a {@link Byte} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Byte getByte(String fieldName) {
    return getByte(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Short} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Short getInt16(String fieldName) {
    return getInt16(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Integer} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Integer getInteger(String fieldName) {
    return getInteger(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Float} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Float getFloat(String fieldName) {
    return getFloat(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Double} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Double getDouble(String fieldName) {
    return getDouble(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Long} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Long getLong(String fieldName) {
    return getLong(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link String} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public String getString(String fieldName) {
    return getString(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Date} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Date getDate(String fieldName) {
    return getDate(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link GregorianCalendar} value by field name, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(String fieldName) {
    return getGregorianCalendar(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link BigDecimal} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public BigDecimal getBigDecimal(String fieldName) {
    return getBigDecimal(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Boolean} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Boolean getBoolean(String fieldName) {
    return getBoolean(getSchema().indexOf(fieldName));
  }

  /**
   * Get an array value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public <T> List<T> getArray(String fieldName, Class<T> elementType) {
    Preconditions.checkState(
        FieldType.ARRAY.equals(getSchema().getField(fieldName).getTypeDescriptor().getType()));
   // Preconditions.checkState(
     //   FieldType.BYTE.equals(
     //       getSchema().getField(fieldName).getTypeDescriptor().getComponentType()));
    return getArray(getSchema().indexOf(fieldName), elementType);
  }

  /**
   * Get a {@link Row} value by field name, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Row getRow(String fieldName) {
    Preconditions.checkState(
        FieldType.ROW.equals(getSchema().getField(fieldName).getTypeDescriptor().getType()));
    return getRow(getSchema().indexOf(fieldName));
  }

  /**
   * Get a {@link Byte} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Byte getByte(int idx) {
    Preconditions.checkState(
        FieldType.BYTE.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Short} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Short getInt16(int idx) {
    Preconditions.checkState(
        FieldType.INT16.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Integer} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Integer getInteger(int idx) {
    Preconditions.checkState(
        FieldType.INT32.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Float} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Float getFloat(int idx) {
    Preconditions.checkState(
        FieldType.FLOAT.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Double} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Double getDouble(int idx) {
    Preconditions.checkState(
        FieldType.DOUBLE.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Long} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Long getLong(int idx) {
    Preconditions.checkState(
        FieldType.INT64.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link String} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public String getString(int idx) {
    Preconditions.checkState(
        FieldType.STRING.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Date} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Date getDate(int idx) {
    Preconditions.checkState(
        FieldType.DATETIME.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link GregorianCalendar} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link BigDecimal} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public BigDecimal getBigDecimal(int idx) {
    Preconditions.checkState(
        FieldType.DECIMAL.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Boolean} value by field index, {@link ClassCastException} is thrown
   * if schema doesn't match.
   */
  public Boolean getBoolean(int idx) {
    Preconditions.checkState(
        FieldType.BOOLEAN.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Get an array value by field index, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public <T> List<T> getArray(int idx, Class<T> elementType) {
    Preconditions.checkState(
        FieldType.ARRAY.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    // Preconditions.checkState(
    //   FieldType.BYTE.equals(
    //       getSchema().getField(fieldName).getTypeDescriptor().getComponentType()));
    return getValue(idx);
  }

  /**
   * Get a {@link Row} value by field index, {@link IllegalStateException} is thrown
   * if schema doesn't match.
   */
  public Row getRow(int idx) {
    Preconditions.checkState(
        FieldType.ROW.equals(getSchema().getField(idx).getTypeDescriptor().getType()));
    return getValue(idx);
  }

  /**
   * Return the size of data fields.
   */
  public int getFieldCount() {
    return getValues().size();
  }

  /**
   * Return the list of data values.
   */
  public abstract List<Object> getValues();

  /**
   * Return {@link Schema} which describes the fields.
   */
  public abstract Schema getSchema();

  /**
   * Creates a record builder with specified {@link #getSchema()}.
   * {@link Builder#build()} will throw an {@link IllegalArgumentException} if number of fields
   * in {@link #getSchema()} does not match the number of fields specified.
   */
  public static Builder withSchema(Schema schema) {
    return
        new AutoValue_Row.Builder(schema);
  }

  /**
   * Builder for {@link Row}.
   *
   * TODO: Add Schema verification here! reuvenlax
   */
  public static class Builder {
    private List<Object> values = new ArrayList<>();
    private Schema schema;

    Builder(Schema schema) {
      this.schema = schema;
    }

    public Builder addValues(List<Object> values) {
      this.values.addAll(values);
      return this;
    }

    public Builder addValues(Object ... values) {
      return addValues(Arrays.asList(values));
    }

    public Builder addArray(List<Object> values) {
      this.values.add(values);
      return this;
    }

    public Builder addArray(Object ... values) {
      addArray(Arrays.asList(values));
      return this;
    }

    public Row build() {
      checkNotNull(schema);

      if (schema.getFieldCount() != values.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Field count in Schema (%s) and values (%s) must match",
                schema.getFieldNames(), values));
      }
      return new AutoValue_Row(values, schema);
    }
  }
}

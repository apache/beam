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

/**
 * {@link Row} is an immutable tuple-like type to represent one element in a
 * {@link PCollection}. The fields are described with a {@link RowType}.
 *
 * <p>{@link RowType} contains the names for each field and the coder for the whole
 * record, {see @link RowType#getRowCoder()}.
 */
@Experimental
@AutoValue
public abstract class Row implements Serializable {

  /**
   * Creates a {@link Row} from the list of values and {@link #getRowType()}.
   */
  public static <T> Collector<T, List<Object>, Row> toRow(
      RowType rowType) {

    return Collector.of(
        () -> new ArrayList<>(rowType.getFieldCount()),
        List::add,
        (left, right) -> {
          left.addAll(right);
          return left;
        },
        values -> Row.withRowType(rowType).addValues(values).build());
  }

  /**
   * Creates a new record filled with nulls.
   */
  public static Row nullRow(RowType rowType) {
    return
        Row
            .withRowType(rowType)
            .addValues(Collections.nCopies(rowType.getFieldCount(), null))
            .build();
  }

  /**
   * Get value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public <T> T getValue(String fieldName) {
    return getValue(getRowType().indexOf(fieldName));
  }

  /**
   * Get value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  @Nullable
  public <T> T getValue(int fieldIdx) {
    return (T) getValues().get(fieldIdx);
  }

  /**
   * Get a {@link Byte} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Byte getByte(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Short} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Short getShort(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Integer} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Integer getInteger(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Float} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Float getFloat(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Double} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Double getDouble(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Long} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Long getLong(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link String} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public String getString(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Date} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Date getDate(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link GregorianCalendar} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link BigDecimal} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public BigDecimal getBigDecimal(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Boolean} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Boolean getBoolean(String fieldName) {
    return getValue(fieldName);
  }

  /**
   * Get a {@link Byte} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Byte getByte(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Short} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Short getShort(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Integer} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Integer getInteger(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Float} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Float getFloat(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Double} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Double getDouble(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Long} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Long getLong(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link String} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public String getString(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Date} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Date getDate(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link GregorianCalendar} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link BigDecimal} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public BigDecimal getBigDecimal(int idx) {
    return getValue(idx);
  }

  /**
   * Get a {@link Boolean} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Boolean getBoolean(int idx) {
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
   * Return {@link RowType} which describes the fields.
   */
  public abstract RowType getRowType();

  /**
   * Creates a record builder with specified {@link #getRowType()}.
   * {@link Builder#build()} will throw an {@link IllegalArgumentException} if number of fields
   * in {@link #getRowType()} does not match the number of fields specified.
   */
  public static Builder withRowType(RowType rowType) {
    return
        new AutoValue_Row.Builder(rowType);
  }

  /**
   * Builder for {@link Row}.
   */
  public static class Builder {
    private List<Object> values = new ArrayList<>();
    private RowType type;

    Builder(RowType type) {
      this.type = type;
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
      checkNotNull(type);

      if (type.getFieldCount() != values.size()) {
        throw new IllegalArgumentException(
            String.format(
                "Field count in RowType (%s) and values (%s) must match",
                type.fieldNames(), values));
      }
      return new AutoValue_Row(values, type);
    }
  }
}

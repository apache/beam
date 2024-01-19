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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.joda.time.DateTime;
import org.joda.time.Instant;
import org.joda.time.ReadableDateTime;

/**
 * Implements an {@link XmlType} of {@link Row} values for compatible use with {@link
 * javax.xml.bind.JAXBContext}. {@link XmlRowValue} allows {@link
 * XmlWriteSchemaTransformFormatProvider} to convert {@link Row} values to XML strings with no
 * knowledge of the original Java class. {@link #setValue(String, Row)} serves as the algorithm's
 * entry point.
 */
@XmlType
class XmlRowValue implements Serializable {
  @Nullable private Object primitiveValue = null;

  @Nullable private ArrayList<XmlRowValue> valueList = null;

  @Nullable private DateTime dateTimeValue = null;

  @Nullable private HashMap<String, XmlRowValue> nestedValue = null;

  /**
   * A {@link Row}'s value for a primitive type such as {@link FieldType#STRING}, {@link
   * FieldType#DOUBLE}, etc.
   */
  @XmlElement(name = "value")
  @Nullable
  Object getPrimitiveValue() {
    return primitiveValue;
  }

  /**
   * A {@link Row}'s value for a {@link FieldType#DATETIME}, converted using {@link
   * XmlDateTimeAdapter}.
   */
  @XmlElement(name = "value")
  @XmlJavaTypeAdapter(XmlDateTimeAdapter.class)
  @Nullable
  DateTime getDateTimeValue() {
    return dateTimeValue;
  }

  /** A {@link Row}'s value for a {@link TypeName#ARRAY} or {@link TypeName#ITERABLE} type. */
  @XmlElement(name = "array")
  @Nullable
  ArrayList<XmlRowValue> getValueList() {
    return valueList;
  }

  /** A {@link Row}'s value for a nested {@link TypeName#ROW} value. */
  @XmlElement(name = "row")
  @Nullable
  HashMap<String, XmlRowValue> getNestedValue() {
    return nestedValue;
  }

  void setPrimitiveValue(Object primitiveValue) {
    this.primitiveValue = primitiveValue;
  }

  void setValueList(ArrayList<XmlRowValue> valueList) {
    this.valueList = valueList;
  }

  /**
   * The entry point for parsing a {@link Row} record and its value mapped from the key. Primitive
   * types populate {@link #setPrimitiveValue(Object)}. {@link FieldType#DATETIME} values populate
   * {@link #setDateTimeValue(ReadableDateTime)}. {@link TypeName#ARRAY} or {@link
   * TypeName#ITERABLE} values populate {@link #setArrayValue(String, Field, Row)} and {@link
   * TypeName#ROW} nested values populate {@link #setNestedValue(Row)}.
   */
  void setValue(String key, Row parent) {
    Schema schema = parent.getSchema();
    Field field = schema.getField(key);
    FieldType fieldType = field.getType();
    TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case BOOLEAN:
      case BYTE:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
        primitiveValue = parent.getValue(key);
        return;
      case ARRAY:
      case ITERABLE:
        setArrayValue(key, field, parent);
        return;
      case DATETIME:
        setDateTimeValue(key, parent);
        return;
      case ROW:
        Optional<Row> child = Optional.ofNullable(parent.getRow(key));
        checkState(child.isPresent());
        setNestedValue(child.get());
        return;
      default:
        throw new IllegalArgumentException(
            String.format("%s at key %s is not supported", typeName.name(), key));
    }
  }

  private void setArrayValue(String key, Field field, Row parent) {
    Optional<FieldType> collectionFieldType =
        Optional.ofNullable(field.getType().getCollectionElementType());
    checkState(collectionFieldType.isPresent());
    TypeName collectionFieldTypeName = collectionFieldType.get().getTypeName();
    Optional<Iterable<Object>> iterable = Optional.ofNullable(parent.getIterable(key));
    checkState(iterable.isPresent());
    valueList = new ArrayList<>();
    Optional<ArrayList<XmlRowValue>> safeValueList = Optional.of(valueList);

    switch (collectionFieldTypeName) {
      case BOOLEAN:
      case BYTE:
      case DECIMAL:
      case DOUBLE:
      case FLOAT:
      case INT16:
      case INT32:
      case INT64:
      case STRING:
        for (Object element : iterable.get()) {
          XmlRowValue value = new XmlRowValue();
          value.setPrimitiveValue(element);
          safeValueList.get().add(value);
        }
        return;
      case DATETIME:
        for (Object element : iterable.get()) {
          XmlRowValue value = new XmlRowValue();
          value.setDateTimeValue(((Instant) element).toDateTime());
          safeValueList.get().add(value);
        }
        return;
      case ROW:
        for (Object value : iterable.get()) {
          Row nestedValue = (Row) value;
          XmlRowValue element = new XmlRowValue();
          element.setNestedValue(nestedValue);
          safeValueList.get().add(element);
        }
        return;
      default:
        throw new IllegalArgumentException(
            String.format("%s at key %s is not supported", collectionFieldType, key));
    }
  }

  private void setDateTimeValue(String key, Row parent) {
    Optional<ReadableDateTime> value = Optional.ofNullable(parent.getDateTime(key));
    checkState(value.isPresent());
    setDateTimeValue(value.get());
  }

  private void setDateTimeValue(ReadableDateTime value) {
    dateTimeValue = Instant.ofEpochMilli(value.getMillis()).toDateTime();
  }

  private void setNestedValue(Row row) {
    Schema schema = row.getSchema();
    nestedValue = new HashMap<>();
    Optional<HashMap<String, XmlRowValue>> safeNestedValue = Optional.of(nestedValue);
    for (String key : schema.getFieldNames()) {
      XmlRowValue child = new XmlRowValue();
      child.setValue(key, row);
      safeNestedValue.get().put(key, child);
    }
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    XmlRowValue that = (XmlRowValue) o;

    Optional<Object> thatPrimitiveValue = Optional.ofNullable(that.getPrimitiveValue());
    Optional<Object> primitiveValue = Optional.ofNullable(getPrimitiveValue());

    Optional<DateTime> thatPrimitiveDateTime = Optional.ofNullable(that.getDateTimeValue());
    Optional<DateTime> dateTime = Optional.ofNullable(getDateTimeValue());

    Optional<ArrayList<XmlRowValue>> thatValueList = Optional.ofNullable(that.getValueList());
    Optional<ArrayList<XmlRowValue>> valueList = Optional.ofNullable(getValueList());

    Optional<HashMap<String, XmlRowValue>> thatNestedValue =
        Optional.ofNullable(that.getNestedValue());
    Optional<HashMap<String, XmlRowValue>> nestedValue = Optional.ofNullable(getNestedValue());

    return equals(primitiveValue, thatPrimitiveValue)
        && equals(dateTime, thatPrimitiveDateTime)
        && equals(valueList, thatValueList)
        && equals(nestedValue, thatNestedValue);
  }

  private static <T> boolean equals(Optional<T> a, Optional<T> b) {
    if (a.isPresent() && b.isPresent()) {
      return a.get().equals(b.get());
    }
    if (!a.isPresent() && !b.isPresent()) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    // resolves dereference of possibly-null reference
    Optional<Object> primitive = Optional.ofNullable(getPrimitiveValue());
    Optional<ArrayList<XmlRowValue>> list = Optional.ofNullable(getValueList());
    Optional<DateTime> dateTime = Optional.ofNullable(getDateTimeValue());
    Optional<HashMap<String, XmlRowValue>> nested = Optional.ofNullable(getNestedValue());

    int result = primitive.hashCode();
    result = 31 * result + list.hashCode();
    result = 31 * result + dateTime.hashCode();
    result = 31 * result + nested.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "XmlRowValue{" + "primitiveValue=" + primitiveValue + '}';
  }
}

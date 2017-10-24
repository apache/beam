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

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BeamRecordCoder;

/**
 * {@link BeamRecord} is an immutable tuple-like type to represent one element in a
 * {@link PCollection}. The fields are described with a {@link BeamRecordType}.
 *
 * <p>By default, {@link BeamRecordType} only contains the name for each field. It
 * can be extended to support more sophisticated validation by overwriting
 * {@link BeamRecordType#validateValueType(int, Object)}.
 *
 * <p>A Coder {@link BeamRecordCoder} is provided, which wraps the Coder for each data field.
 */
@Experimental
public class BeamRecord implements Serializable {
  //immutable list of field values.
  private List<Object> dataValues;
  private BeamRecordType dataType;

  /**
   * Creates a BeamRecord.
   * @param dataType type of the record
   * @param rawDataValues values of the record, record's size must match size of
   *                      the {@code BeamRecordType}, or can be null, if it is null
   *                      then every field is null.
   */
  public BeamRecord(BeamRecordType dataType, List<Object> rawDataValues) {
    if (dataType.getFieldNames().size() != rawDataValues.size()) {
      throw new IllegalArgumentException(
          "Field count in BeamRecordType(" + dataType.getFieldNames().size()
              + ") and rawDataValues(" + rawDataValues.size() + ") must match!");
    }

    this.dataType = dataType;
    this.dataValues = new ArrayList<>(dataType.getFieldCount());

    for (int idx = 0; idx < dataType.getFieldCount(); ++idx) {
      dataValues.add(null);
    }

    for (int idx = 0; idx < dataType.getFieldCount(); ++idx) {
      addField(idx, rawDataValues.get(idx));
    }
  }

  /**
   * see {@link #BeamRecord(BeamRecordType, List)}.
   */
  public BeamRecord(BeamRecordType dataType, Object... rawdataValues) {
    this(dataType, Arrays.asList(rawdataValues));
  }

  private void addField(int index, Object fieldValue) {
    dataType.validateValueType(index, fieldValue);
    dataValues.set(index, fieldValue);
  }

  /**
   * Get value by field name.
   */
  public Object getFieldValue(String fieldName) {
    return getFieldValue(dataType.getFieldNames().indexOf(fieldName));
  }

  /**
   * Get a {@link Byte} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Byte getByte(String fieldName) {
    return (Byte) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Short} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Short getShort(String fieldName) {
    return (Short) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Integer} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Integer getInteger(String fieldName) {
    return (Integer) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Float} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Float getFloat(String fieldName) {
    return (Float) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Double} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Double getDouble(String fieldName) {
    return (Double) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Long} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Long getLong(String fieldName) {
    return (Long) getFieldValue(fieldName);
  }

  /**
   * Get a {@link String} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public String getString(String fieldName) {
    return (String) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Date} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Date getDate(String fieldName) {
    return (Date) getFieldValue(fieldName);
  }

  /**
   * Get a {@link GregorianCalendar} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(String fieldName) {
    return (GregorianCalendar) getFieldValue(fieldName);
  }

  /**
   * Get a {@link BigDecimal} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public BigDecimal getBigDecimal(String fieldName) {
    return (BigDecimal) getFieldValue(fieldName);
  }

  /**
   * Get a {@link Boolean} value by field name, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Boolean getBoolean(String fieldName) {
    return (Boolean) getFieldValue(fieldName);
  }

  /** Get value by field index. */
  @Nullable
  public Object getFieldValue(int fieldIdx) {
    return dataValues.get(fieldIdx);
  }

  /**
   * Get a {@link Byte} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Byte getByte(int idx) {
    return (Byte) getFieldValue(idx);
  }

  /**
   * Get a {@link Short} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Short getShort(int idx) {
    return (Short) getFieldValue(idx);
  }

  /**
   * Get a {@link Integer} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Integer getInteger(int idx) {
    return (Integer) getFieldValue(idx);
  }

  /**
   * Get a {@link Float} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Float getFloat(int idx) {
    return (Float) getFieldValue(idx);
  }

  /**
   * Get a {@link Double} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Double getDouble(int idx) {
    return (Double) getFieldValue(idx);
  }

  /**
   * Get a {@link Long} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Long getLong(int idx) {
    return (Long) getFieldValue(idx);
  }

  /**
   * Get a {@link String} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public String getString(int idx) {
    return (String) getFieldValue(idx);
  }

  /**
   * Get a {@link Date} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Date getDate(int idx) {
    return (Date) getFieldValue(idx);
  }

  /**
   * Get a {@link GregorianCalendar} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public GregorianCalendar getGregorianCalendar(int idx) {
    return (GregorianCalendar) getFieldValue(idx);
  }

  /**
   * Get a {@link BigDecimal} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public BigDecimal getBigDecimal(int idx) {
    return (BigDecimal) getFieldValue(idx);
  }

  /**
   * Get a {@link Boolean} value by field index, {@link ClassCastException} is thrown
   * if type doesn't match.
   */
  public Boolean getBoolean(int idx) {
    return (Boolean) getFieldValue(idx);
  }

  /**
   * Return the size of data fields.
   */
  public int getFieldCount() {
    return dataValues.size();
  }

  /**
   * Return the list of data values.
   */
  public List<Object> getDataValues() {
    return Collections.unmodifiableList(dataValues);
  }

  /**
   * Return {@link BeamRecordType} which describes the fields.
   */
  public BeamRecordType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    return "BeamRecord [dataValues=" + dataValues + ", dataType=" + dataType + "]";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    BeamRecord other = (BeamRecord) obj;
    return toString().equals(other.toString());
  }

  @Override public int hashCode() {
    return 31 * getDataType().hashCode() + getDataValues().hashCode();
  }
}

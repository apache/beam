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
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * {@link org.apache.beam.sdk.values.BeamRecord}, self-described with
 * {@link BeamRecordType}, represents one element in a
 * {@link org.apache.beam.sdk.values.PCollection}.
 */
@Experimental
public class BeamRecord implements Serializable {
  private List<Object> dataValues;
  private BeamRecordType dataType;

  public BeamRecord(BeamRecordType dataType) {
    this.dataType = dataType;
    this.dataValues = new ArrayList<>();
    for (int idx = 0; idx < dataType.size(); ++idx) {
      dataValues.add(null);
    }
  }

  public BeamRecord(BeamRecordType dataType, List<Object> dataValues) {
    this(dataType);
    for (int idx = 0; idx < dataValues.size(); ++idx) {
      addField(idx, dataValues.get(idx));
    }
  }

  public void addField(String fieldName, Object fieldValue) {
    addField(dataType.getFieldsName().indexOf(fieldName), fieldValue);
  }

  public void addField(int index, Object fieldValue) {
    dataType.validateValueType(index, fieldValue);
    dataValues.set(index, fieldValue);
  }

  public Object getFieldValue(String fieldName) {
    return getFieldValue(dataType.getFieldsName().indexOf(fieldName));
  }

  public Byte getByte(String fieldName) {
    return (Byte) getFieldValue(fieldName);
  }

  public Short getShort(String fieldName) {
    return (Short) getFieldValue(fieldName);
  }

  public Integer getInteger(String fieldName) {
    return (Integer) getFieldValue(fieldName);
  }

  public Float getFloat(String fieldName) {
    return (Float) getFieldValue(fieldName);
  }

  public Double getDouble(String fieldName) {
    return (Double) getFieldValue(fieldName);
  }

  public Long getLong(String fieldName) {
    return (Long) getFieldValue(fieldName);
  }

  public String getString(String fieldName) {
    return (String) getFieldValue(fieldName);
  }

  public Date getDate(String fieldName) {
    return (Date) getFieldValue(fieldName);
  }

  public GregorianCalendar getGregorianCalendar(String fieldName) {
    return (GregorianCalendar) getFieldValue(fieldName);
  }

  public BigDecimal getBigDecimal(String fieldName) {
    return (BigDecimal) getFieldValue(fieldName);
  }

  public Boolean getBoolean(String fieldName) {
    return (Boolean) getFieldValue(fieldName);
  }

  public Object getFieldValue(int fieldIdx) {
    return dataValues.get(fieldIdx);
  }

  public Byte getByte(int idx) {
    return (Byte) getFieldValue(idx);
  }

  public Short getShort(int idx) {
    return (Short) getFieldValue(idx);
  }

  public Integer getInteger(int idx) {
    return (Integer) getFieldValue(idx);
  }

  public Float getFloat(int idx) {
    return (Float) getFieldValue(idx);
  }

  public Double getDouble(int idx) {
    return (Double) getFieldValue(idx);
  }

  public Long getLong(int idx) {
    return (Long) getFieldValue(idx);
  }

  public String getString(int idx) {
    return (String) getFieldValue(idx);
  }

  public Date getDate(int idx) {
    return (Date) getFieldValue(idx);
  }

  public GregorianCalendar getGregorianCalendar(int idx) {
    return (GregorianCalendar) getFieldValue(idx);
  }

  public BigDecimal getBigDecimal(int idx) {
    return (BigDecimal) getFieldValue(idx);
  }

  public Boolean getBoolean(int idx) {
    return (boolean) getFieldValue(idx);
  }

  public int size() {
    return dataValues.size();
  }

  public List<Object> getDataValues() {
    return dataValues;
  }

  public void setDataValues(List<Object> dataValues) {
    this.dataValues = dataValues;
  }

  public BeamRecordType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    return "BeamSqlRow [dataValues=" + dataValues + ", dataType=" + dataType + "]";
  }

  /**
   * Return data fields as key=value.
   */
  public String valueInString() {
    StringBuilder sb = new StringBuilder();
    for (int idx = 0; idx < size(); ++idx) {
      sb.append(
          String.format(",%s=%s", getDataType().getFieldsName().get(idx), getFieldValue(idx)));
    }
    return sb.substring(1);
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

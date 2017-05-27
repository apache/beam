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
package org.apache.beam.dsls.sql.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.beam.dsls.sql.exception.InvalidFieldException;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.Instant;

/**
 * Repersent a generic ROW record in Beam SQL.
 *
 */
public class BeamSQLRow implements Serializable {

  private List<Integer> nullFields = new ArrayList<>();
  private List<Object> dataValues;
  private BeamSQLRecordType dataType;

  private Instant windowStart = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));
  private Instant windowEnd = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));

  public BeamSQLRow(BeamSQLRecordType dataType) {
    this.dataType = dataType;
    this.dataValues = new ArrayList<>();
    for (int idx = 0; idx < dataType.size(); ++idx) {
      dataValues.add(null);
      nullFields.add(idx);
    }
  }

  public BeamSQLRow(BeamSQLRecordType dataType, List<Object> dataValues) {
    this(dataType);
    for (int idx = 0; idx < dataValues.size(); ++idx) {
      addField(idx, dataValues.get(idx));
    }
  }

  public void updateWindowRange(BeamSQLRow upstreamRecord, BoundedWindow window){
    windowStart = upstreamRecord.windowStart;
    windowEnd = upstreamRecord.windowEnd;

    if (window instanceof IntervalWindow) {
      IntervalWindow iWindow = (IntervalWindow) window;
      windowStart = iWindow.start();
      windowEnd = iWindow.end();
    }
  }

  public void addField(String fieldName, Object fieldValue) {
    addField(dataType.getFieldsName().indexOf(fieldName), fieldValue);
  }

  public void addField(int index, Object fieldValue) {
    if (fieldValue == null) {
      return;
    } else {
      if (nullFields.contains(index)) {
        nullFields.remove(nullFields.indexOf(index));
      }
    }

    SqlTypeName fieldType = dataType.getFieldsType().get(index);
    switch (fieldType) {
    case INTEGER:
      if (!(fieldValue instanceof Integer)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case SMALLINT:
      if (!(fieldValue instanceof Short)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case TINYINT:
      if (!(fieldValue instanceof Byte)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case DOUBLE:
      if (!(fieldValue instanceof Double)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case BIGINT:
      if (!(fieldValue instanceof Long)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case FLOAT:
      if (!(fieldValue instanceof Float)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case VARCHAR:
      if (!(fieldValue instanceof String)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case TIME:
      if (!(fieldValue instanceof GregorianCalendar)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    case TIMESTAMP:
      if (!(fieldValue instanceof Date)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      }
      break;
    default:
      throw new UnsupportedDataTypeException(fieldType);
    }
    dataValues.set(index, fieldValue);
  }

  public short getShort(int idx) {
    return (Short) getFieldValue(idx);
  }

  public int getInteger(int idx) {
    return (Integer) getFieldValue(idx);
  }

  public float getFloat(int idx) {
    return (Float) getFieldValue(idx);
  }

  public double getDouble(int idx) {
    return (Double) getFieldValue(idx);
  }

  public long getLong(int idx) {
    return (Long) getFieldValue(idx);
  }

  public String getString(int idx) {
    return (String) getFieldValue(idx);
  }

  public Date getDate(int idx) {
    return (Date) getFieldValue(idx);
  }

  public Object getFieldValue(String fieldName) {
    return getFieldValue(dataType.getFieldsName().indexOf(fieldName));
  }

  public Object getFieldValue(int fieldIdx) {
    if (nullFields.contains(fieldIdx)) {
      return null;
    }

    Object fieldValue = dataValues.get(fieldIdx);
    SqlTypeName fieldType = dataType.getFieldsType().get(fieldIdx);

    switch (fieldType) {
    case INTEGER:
      if (!(fieldValue instanceof Integer)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case SMALLINT:
      if (!(fieldValue instanceof Short)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case TINYINT:
      if (!(fieldValue instanceof Byte)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case DOUBLE:
      if (!(fieldValue instanceof Double)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case BIGINT:
      if (!(fieldValue instanceof Long)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case FLOAT:
      if (!(fieldValue instanceof Float)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case VARCHAR:
      if (!(fieldValue instanceof String)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case TIME:
      if (!(fieldValue instanceof GregorianCalendar)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    case TIMESTAMP:
      if (!(fieldValue instanceof Date)) {
        throw new InvalidFieldException(
            String.format("[%s] doesn't match type [%s]", fieldValue, fieldType));
      } else {
        return fieldValue;
      }
    default:
      throw new UnsupportedDataTypeException(fieldType);
    }
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

  public BeamSQLRecordType getDataType() {
    return dataType;
  }

  public void setDataType(BeamSQLRecordType dataType) {
    this.dataType = dataType;
  }

  public void setNullFields(List<Integer> nullFields) {
    this.nullFields = nullFields;
  }

  public List<Integer> getNullFields() {
    return nullFields;
  }

  public Instant getWindowStart() {
    return windowStart;
  }

  public Instant getWindowEnd() {
    return windowEnd;
  }

  public void setWindowStart(Instant windowStart) {
    this.windowStart = windowStart;
  }

  public void setWindowEnd(Instant windowEnd) {
    this.windowEnd = windowEnd;
  }

  @Override
  public String toString() {
    return "BeamSQLRow [nullFields=" + nullFields + ", dataValues=" + dataValues + ", dataType="
        + dataType + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + "]";
  }

  /**
   * Return data fields as key=value.
   */
  public String valueInString() {
    StringBuffer sb = new StringBuffer();
    for (int idx = 0; idx < size(); ++idx) {
      sb.append(String.format(",%s=%s", dataType.getFieldsName().get(idx), getFieldValue(idx)));
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
    BeamSQLRow other = (BeamSQLRow) obj;
    return toString().equals(other.toString());
  }

}

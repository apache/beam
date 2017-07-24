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
package org.apache.beam.sdk.sd;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

/**
 * Represent a generic ROW record in Beam SQL.
 *
 */
public class BeamRow implements Serializable {
  private static final Map<Integer, Class> SQL_TYPE_TO_JAVA_CLASS = new HashMap<>();
  static {
    SQL_TYPE_TO_JAVA_CLASS.put(Types.TINYINT, Byte.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.SMALLINT, Short.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.INTEGER, Integer.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.BIGINT, Long.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.FLOAT, Float.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.DOUBLE, Double.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.DECIMAL, BigDecimal.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.BOOLEAN, Boolean.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.CHAR, String.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.VARCHAR, String.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.TIME, GregorianCalendar.class);

    SQL_TYPE_TO_JAVA_CLASS.put(Types.DATE, Date.class);
    SQL_TYPE_TO_JAVA_CLASS.put(Types.TIMESTAMP, Date.class);
  }

  private List<Integer> nullFields = new ArrayList<>();
  private List<Object> dataValues;
  private BeamRowType dataType;

  private Instant windowStart = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));
  private Instant windowEnd = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));

  public BeamRow(BeamRowType dataType) {
    this.dataType = dataType;
    this.dataValues = new ArrayList<>();
    for (int idx = 0; idx < dataType.size(); ++idx) {
      dataValues.add(null);
      nullFields.add(idx);
    }
  }

  public BeamRow(BeamRowType dataType, List<Object> dataValues) {
    this(dataType);
    for (int idx = 0; idx < dataValues.size(); ++idx) {
      addField(idx, dataValues.get(idx));
    }
  }

  public void updateWindowRange(BeamRow upstreamRecord, BoundedWindow window){
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

    validateValueType(index, fieldValue);
    dataValues.set(index, fieldValue);
  }

  private void validateValueType(int index, Object fieldValue) {
    int fieldType = dataType.getFieldsType().get(index);
    Class javaClazz = SQL_TYPE_TO_JAVA_CLASS.get(fieldType);
    if (javaClazz == null) {
      throw new UnsupportedOperationException("Data type: " + fieldType + " not supported yet!");
    }

    if (!fieldValue.getClass().equals(javaClazz)) {
      throw new IllegalArgumentException(
          String.format("[%s](%s) doesn't match type [%s]",
              fieldValue, fieldValue.getClass(), fieldType)
      );
    }
  }

  public Object getFieldValue(String fieldName) {
    return getFieldValue(dataType.getFieldsName().indexOf(fieldName));
  }

  public byte getByte(String fieldName) {
    return (Byte) getFieldValue(fieldName);
  }

  public short getShort(String fieldName) {
    return (Short) getFieldValue(fieldName);
  }

  public int getInteger(String fieldName) {
    return (Integer) getFieldValue(fieldName);
  }

  public float getFloat(String fieldName) {
    return (Float) getFieldValue(fieldName);
  }

  public double getDouble(String fieldName) {
    return (Double) getFieldValue(fieldName);
  }

  public long getLong(String fieldName) {
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

  public boolean getBoolean(String fieldName) {
    return (boolean) getFieldValue(fieldName);
  }

  public Object getFieldValue(int fieldIdx) {
    if (nullFields.contains(fieldIdx)) {
      return null;
    }

    return dataValues.get(fieldIdx);
  }

  public byte getByte(int idx) {
    return (Byte) getFieldValue(idx);
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

  public GregorianCalendar getGregorianCalendar(int idx) {
    return (GregorianCalendar) getFieldValue(idx);
  }

  public BigDecimal getBigDecimal(int idx) {
    return (BigDecimal) getFieldValue(idx);
  }

  public boolean getBoolean(int idx) {
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

  public BeamRowType getDataType() {
    return dataType;
  }

  public void setDataType(BeamRowType dataType) {
    this.dataType = dataType;
  }

  public void setNullFields(List<Integer> nullFields) {
    this.nullFields = nullFields;
  }

  public List<Integer> getNullFields() {
    return nullFields;
  }

  /**
   * is the specified field NULL?
   */
  public boolean isNull(int idx) {
    return nullFields.contains(idx);
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
    return "BeamSqlRow [nullFields=" + nullFields + ", dataValues=" + dataValues + ", dataType="
        + dataType + ", windowStart=" + windowStart + ", windowEnd=" + windowEnd + "]";
  }

  /**
   * Return data fields as key=value.
   */
  public String valueInString() {
    StringBuilder sb = new StringBuilder();
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
    BeamRow other = (BeamRow) obj;
    return toString().equals(other.toString());
  }

  @Override public int hashCode() {
    return 31 * (31 * dataType.hashCode() + dataValues.hashCode()) + nullFields.hashCode();
  }
}

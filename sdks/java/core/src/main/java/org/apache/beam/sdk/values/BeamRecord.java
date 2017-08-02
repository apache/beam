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
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.joda.time.Instant;

/**
 * {@link org.apache.beam.sdk.values.BeamRecord}, self-described with
 * {@link BeamRecordTypeProvider}, represents one element in a
 * {@link org.apache.beam.sdk.values.PCollection}.
 */
@Experimental
public class BeamRecord implements Serializable {
  //null values are indexed here, to handle properly in Coder.
  private List<Integer> nullFields = new ArrayList<>();
  private List<Object> dataValues;
  private BeamRecordTypeProvider dataType;

  private Instant windowStart = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MIN_VALUE));
  private Instant windowEnd = new Instant(TimeUnit.MICROSECONDS.toMillis(Long.MAX_VALUE));

  public BeamRecord(BeamRecordTypeProvider dataType) {
    this.dataType = dataType;
    this.dataValues = new ArrayList<>();
    for (int idx = 0; idx < dataType.size(); ++idx) {
      dataValues.add(null);
      nullFields.add(idx);
    }
  }

  public BeamRecord(BeamRecordTypeProvider dataType, List<Object> dataValues) {
    this(dataType);
    for (int idx = 0; idx < dataValues.size(); ++idx) {
      addField(idx, dataValues.get(idx));
    }
  }

  public void updateWindowRange(BeamRecord upstreamRecord, BoundedWindow window){
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

    dataType.validateValueType(index, fieldValue);
    dataValues.set(index, fieldValue);
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

  public BeamRecordTypeProvider getDataType() {
    return dataType;
  }

  public void setDataType(BeamRecordTypeProvider dataType) {
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
    return 31 * (31 * getDataType().hashCode() + getDataValues().hashCode())
        + getNullFields().hashCode();
  }
}

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
package org.beam.sdk.java.sql.schema;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Repersent a generic ROW record in Beam SQL.
 *
 */
@DefaultCoder(AvroCoder.class)
public class BeamSQLRow implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 4569220242480160895L;

  private Map<String, String> dataMap = new HashMap<>();
  @Nullable
  private BeamSQLRecordType dataType;

  @Deprecated
  public BeamSQLRow() {
  }

  public BeamSQLRow(BeamSQLRecordType dataType) {
    super();
    this.dataType = dataType;
  }

  public void addField(String fieldName, Object fieldValue) {
    if (fieldValue != null) {
      dataMap.put(fieldName, fieldValue.toString());
    } else {
      // dataMap.put(fieldName, null);
    }
  }

  public void addField(int index, Object fieldValue) {
    addField(dataType.getFieldsName().get(index), fieldValue);
  }

  public Object getFieldValue(int fieldIdx) {
    return getFieldValue(dataType.getFieldsName().get(fieldIdx),
        dataType.getFieldsType().get(fieldIdx));
  }

  public Object getFieldValue(String fieldName) {
    if (dataType.getFieldsName().indexOf(fieldName) == -1) {
      return null;
    }
    return getFieldValue(fieldName,
        dataType.getFieldsType().get(dataType.getFieldsName().indexOf(fieldName)));
  }

  private Object getFieldValue(String fieldName, String fieldType) {
    if (dataMap.get(fieldName) == null) {
      return null;
    }
    switch (SqlTypeName.valueOf(fieldType)) {
    case INTEGER:
      return Integer.valueOf(dataMap.get(fieldName));
    case VARCHAR:
      return dataMap.get(fieldName);
    case TIMESTAMP: // TODO
    case BIGINT:
      return Long.valueOf(dataMap.get(fieldName));
    default:
      return dataMap.get(fieldName);
    }
  }

  public Map<String, String> getDataMap() {
    return dataMap;
  }

  public void setDataMap(HashMap<String, String> dataMap) {
    this.dataMap = dataMap;
  }

  public BeamSQLRecordType getDataType() {
    return dataType;
  }

  public void setDataType(BeamSQLRecordType dataType) {
    this.dataType = dataType;
  }

  @Override
  public String toString() {
    return "RecordRow [dataMap=" + dataMap + ", dataType=" + dataType + "]";
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dataMap == null) ? 0 : dataMap.hashCode());
    result = prime * result + ((dataType == null) ? 0 : dataType.hashCode());
    return result;
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

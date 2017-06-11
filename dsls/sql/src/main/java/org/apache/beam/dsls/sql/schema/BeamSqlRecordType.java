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
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Field type information in {@link BeamSqlRow}.
 *
 */
public class BeamSqlRecordType implements Serializable {
  private List<String> fieldsName = new ArrayList<>();
  private List<SqlTypeName> fieldsType = new ArrayList<>();

  /**
   * Generate from {@link RelDataType} which is used to create table.
   */
  public static BeamSqlRecordType from(RelDataType tableInfo) {
    BeamSqlRecordType record = new BeamSqlRecordType();
    for (RelDataTypeField f : tableInfo.getFieldList()) {
      record.fieldsName.add(f.getName());
      record.fieldsType.add(f.getType().getSqlTypeName());
    }
    return record;
  }

  public void addField(String fieldName, SqlTypeName fieldType) {
    fieldsName.add(fieldName);
    fieldsType.add(fieldType);
  }

  /**
   * Create an instance of {@link RelDataType} so it can be used to create a table.
   */
  public RelProtoDataType toRelDataType() {
    return new RelProtoDataType() {
      @Override
      public RelDataType apply(RelDataTypeFactory a) {
        FieldInfoBuilder builder = a.builder();
        for (int idx = 0; idx < fieldsName.size(); ++idx) {
          builder.add(fieldsName.get(idx), fieldsType.get(idx));
        }
        return builder.build();
      }
    };
  }

  public int size() {
    return fieldsName.size();
  }

  public List<String> getFieldsName() {
    return fieldsName;
  }

  public void setFieldsName(List<String> fieldsName) {
    this.fieldsName = fieldsName;
  }

  public List<SqlTypeName> getFieldsType() {
    return fieldsType;
  }

  public void setFieldsType(List<SqlTypeName> fieldsType) {
    this.fieldsType = fieldsType;
  }

  @Override
  public String toString() {
    return "RecordType [fieldsName=" + fieldsName + ", fieldsType=" + fieldsType + "]";
  }

}

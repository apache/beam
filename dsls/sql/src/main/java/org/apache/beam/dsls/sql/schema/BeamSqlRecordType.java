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

/**
 * Field type information in {@link BeamSqlRow}.
 *
 */
public class BeamSqlRecordType implements Serializable {
  private List<String> fieldsName = new ArrayList<>();
  private List<Integer> fieldsType = new ArrayList<>();

  public void addField(String fieldName, Integer fieldType) {
    fieldsName.add(fieldName);
    fieldsType.add(fieldType);
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

  public List<Integer> getFieldsType() {
    return fieldsType;
  }

  public void setFieldsType(List<Integer> fieldsType) {
    this.fieldsType = fieldsType;
  }

  @Override
  public String toString() {
    return "RecordType [fieldsName=" + fieldsName + ", fieldsType=" + fieldsType + "]";
  }

}

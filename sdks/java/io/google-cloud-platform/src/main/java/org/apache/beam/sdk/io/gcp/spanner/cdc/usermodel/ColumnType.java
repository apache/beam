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
package org.apache.beam.sdk.io.gcp.spanner.cdc.usermodel;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultCoder(AvroCoder.class)
public class ColumnType implements Serializable {

  private static final long serialVersionUID = 6861617019875340414L;

  private String name;
  private TypeCode type;
  private boolean isPrimaryKey;

  public ColumnType() {}

  @SchemaCreate
  public ColumnType(String name, TypeCode type, boolean isPrimaryKey) {
    this.name = name;
    this.type = type;
    this.isPrimaryKey = isPrimaryKey;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public TypeCode getType() {
    return type;
  }

  public void setType(TypeCode type) {
    this.type = type;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void setPrimaryKey(boolean primaryKey) {
    isPrimaryKey = primaryKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnType that = (ColumnType) o;
    return isPrimaryKey == that.isPrimaryKey
        && Objects.equals(name, that.name)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, isPrimaryKey);
  }
}

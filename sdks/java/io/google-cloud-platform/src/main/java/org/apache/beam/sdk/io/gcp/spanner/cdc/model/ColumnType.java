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
package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import java.io.Serializable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;

@DefaultCoder(AvroCoder.class)
public class ColumnType implements Serializable {

  private static final long serialVersionUID = 6861617019875340414L;

  private String name;
  private TypeCode type;
  private boolean isPrimaryKey;
  private long ordinalPosition;

  /** Default constructor for serialization only. */
  private ColumnType() {}

  @SchemaCreate
  public ColumnType(String name, TypeCode type, boolean isPrimaryKey, long ordinalPosition) {
    this.name = name;
    this.type = type;
    this.isPrimaryKey = isPrimaryKey;
    this.ordinalPosition = ordinalPosition;
  }

  public String getName() {
    return name;
  }

  public TypeCode getType() {
    return type;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public long getOrdinalPosition() {
    return ordinalPosition;
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
    return isPrimaryKey() == that.isPrimaryKey()
        && getOrdinalPosition() == that.getOrdinalPosition()
        && Objects.equal(getName(), that.getName())
        && Objects.equal(getType(), that.getType());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName(), getType(), isPrimaryKey(), getOrdinalPosition());
  }

  @Override
  public String toString() {
    return "ColumnType{"
        + "name='"
        + name
        + '\''
        + ", type="
        + type
        + ", isPrimaryKey="
        + isPrimaryKey
        + ", ordinalPosition="
        + ordinalPosition
        + '}';
  }
}

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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.model;

import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

/**
 * Defines a column type from a Cloud Spanner table with the following information: column name,
 * column type, flag indicating if column is primary key and column position in the table.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
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

  /** The name of the column. */
  public String getName() {
    return name;
  }

  /** The type of the column. */
  public TypeCode getType() {
    return type;
  }

  /** True if the column is part of the primary key, false otherwise. */
  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  /** The position of the column in the table. */
  public long getOrdinalPosition() {
    return ordinalPosition;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnType)) {
      return false;
    }
    ColumnType that = (ColumnType) o;
    return isPrimaryKey == that.isPrimaryKey
        && ordinalPosition == that.ordinalPosition
        && Objects.equals(name, that.name)
        && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type, isPrimaryKey, ordinalPosition);
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

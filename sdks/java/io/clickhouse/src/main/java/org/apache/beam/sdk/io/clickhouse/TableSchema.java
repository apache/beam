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
package org.apache.beam.sdk.io.clickhouse;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;

/** A descriptor for ClickHouse table schema. */
@Experimental(Experimental.Kind.SOURCE_SINK)
@AutoValue
public abstract class TableSchema implements Serializable {

  public abstract List<Column> columns();

  public static TableSchema of(List<Column> columns) {
    return new AutoValue_TableSchema(columns);
  }

  /** A column in ClickHouse table. */
  @AutoValue
  public abstract static class Column implements Serializable {
    public abstract String name();

    public abstract ColumnType columnType();

    public static Column of(String name, ColumnType columnType) {
      return new AutoValue_TableSchema_Column(name, columnType);
    }
  }

  /** An enumeration of possible types in ClickHouse. */
  public enum TypeName {
    // Primitive types
    DATE,
    DATETIME,
    FLOAT32,
    FLOAT64,
    INT8,
    INT16,
    INT32,
    INT64,
    STRING,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    // Composite types
    ARRAY
  }

  /** A descriptor for a column type. */
  @AutoValue
  public abstract static class ColumnType implements Serializable {
    public static final ColumnType DATE = ColumnType.of(TypeName.DATE);
    public static final ColumnType DATETIME = ColumnType.of(TypeName.DATETIME);
    public static final ColumnType FLOAT32 = ColumnType.of(TypeName.FLOAT32);
    public static final ColumnType FLOAT64 = ColumnType.of(TypeName.FLOAT64);
    public static final ColumnType INT8 = ColumnType.of(TypeName.INT8);
    public static final ColumnType INT16 = ColumnType.of(TypeName.INT16);
    public static final ColumnType INT32 = ColumnType.of(TypeName.INT32);
    public static final ColumnType INT64 = ColumnType.of(TypeName.INT64);
    public static final ColumnType STRING = ColumnType.of(TypeName.STRING);
    public static final ColumnType UINT8 = ColumnType.of(TypeName.UINT8);
    public static final ColumnType UINT16 = ColumnType.of(TypeName.UINT16);
    public static final ColumnType UINT32 = ColumnType.of(TypeName.UINT32);
    public static final ColumnType UINT64 = ColumnType.of(TypeName.UINT64);

    public abstract TypeName typeName();

    @Nullable
    public abstract ColumnType arrayElementType();

    public static ColumnType of(TypeName typeName) {
      return ColumnType.builder().typeName(typeName).build();
    }

    public static ColumnType array(ColumnType arrayElementType) {
      return ColumnType.builder()
          .typeName(TypeName.ARRAY)
          .arrayElementType(arrayElementType)
          .build();
    }

    /** Parse string with ClickHouse type to {@link ColumnType}. */
    public static ColumnType parse(String str) {
      try {
        return new org.apache.beam.sdk.io.clickhouse.impl.parser.ColumnTypeParser(
                new StringReader(str))
            .parse();
      } catch (org.apache.beam.sdk.io.clickhouse.impl.parser.ParseException e) {
        throw new IllegalArgumentException("failed to parse", e);
      }
    }

    public static Builder builder() {
      return new AutoValue_TableSchema_ColumnType.Builder();
    }

    @AutoValue.Builder
    abstract static class Builder {

      public abstract Builder typeName(TypeName typeName);

      public abstract Builder arrayElementType(ColumnType arrayElementType);

      public abstract ColumnType build();
    }
  }
}

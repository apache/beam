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
package org.apache.beam.sdk.io.gcp.spanner;

import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.Type;
import com.google.common.base.Objects;
import com.google.common.collect.ArrayListMultimap;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Encapsulates Cloud Spanner Schema.
 */
class SpannerSchema implements Serializable {
  private final List<String> tables;
  private final ArrayListMultimap<String, Column> columns;
  private final ArrayListMultimap<String, KeyPart> keyParts;

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link SpannerSchema}.
   */
  static class Builder {
    private final ArrayListMultimap<String, Column> columns = ArrayListMultimap.create();
    private final ArrayListMultimap<String, KeyPart> keyParts = ArrayListMultimap.create();

    public Builder addColumn(String table, String name, String type) {
      addColumn(table, Column.create(name.toLowerCase(), type));
      return this;
    }

    private Builder addColumn(String table, Column column) {
      columns.put(table.toLowerCase(), column);
      return this;
    }

    public Builder addKeyPart(String table, String column, boolean desc) {
      keyParts.put(table, KeyPart.create(column.toLowerCase(), desc));
      return this;
    }

    public SpannerSchema build() {
      return new SpannerSchema(columns, keyParts);
    }
  }

  private SpannerSchema(ArrayListMultimap<String, Column> columns,
      ArrayListMultimap<String, KeyPart> keyParts) {
    this.columns = columns;
    this.keyParts = keyParts;
    tables = new ArrayList<>(columns.keySet());
  }

  public List<String> getTables() {
    return tables;
  }

  public List<Column> getColumns(String table) {
    return columns.get(table);
  }

  public List<KeyPart> getKeyParts(String table) {
    return keyParts.get(table);
  }

  @AutoValue
  abstract static class KeyPart implements Serializable {
    static KeyPart create(String field, boolean desc) {
      return new AutoValue_SpannerSchema_KeyPart(field, desc);
    }

    abstract String getField();

    abstract boolean isDesc();
  }

  @AutoValue
  abstract static class Column implements Serializable {

    static Column create(String name, Type type) {
      return new AutoValue_SpannerSchema_Column(name, type);
    }

    static Column create(String name, String spannerType) {
      return create(name, parseSpannerType(spannerType));
    }

    public abstract String getName();

    public abstract Type getType();

    private static Type parseSpannerType(String spannerType) {
      spannerType = spannerType.toUpperCase();
      if (spannerType.equals("BOOL")) {
        return Type.bool();
      }
      if (spannerType.equals("INT64")) {
        return Type.int64();
      }
      if (spannerType.equals("FLOAT64")) {
        return Type.float64();
      }
      if (spannerType.startsWith("STRING")) {
        return Type.string();
      }
      if (spannerType.startsWith("BYTES")) {
        return Type.bytes();
      }
      if (spannerType.equals("TIMESTAMP")) {
        return Type.timestamp();
      }
      if (spannerType.equals("DATE")) {
        return Type.date();
      }

      if (spannerType.startsWith("ARRAY")) {
        // Substring "ARRAY<xxx>"
        String spannerArrayType = spannerType.substring(6, spannerType.length() - 1);
        Type itemType = parseSpannerType(spannerArrayType);
        return Type.array(itemType);
      }
      throw new IllegalArgumentException("Unknown spanner type " + spannerType);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SpannerSchema that = (SpannerSchema) o;
    return Objects.equal(tables, that.tables) && Objects.equal(columns, that.columns) && Objects
        .equal(keyParts, that.keyParts);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tables, columns, keyParts);
  }
}

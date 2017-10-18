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
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates Cloud Spanner Schema.
 */
class SpannerSchema implements Serializable {
  private final List<String> tables;
  private final Map<String, Integer> tableIndex;
  private final Map<String, List<Column>> columns;
  private final Map<String, List<KeyPart>> keyParts;

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder for {@link SpannerSchema}.
   */
  static class Builder {
    private final Map<String, List<Column>> columns = new HashMap<>();
    private final Map<String, List<KeyPart>> keyParts = new HashMap<>();

    public void addColumn(String table, String name, String type) {
      addColumn(table, Column.create(name.toLowerCase(), type));
    }

    private void addColumn(String table, Column column) {
      List<Column> list = columns.get(table);
      if (list == null) {
        list = new ArrayList<>();
        columns.put(table.toLowerCase(), list);
      }
      list.add(column);
    }

    public void addKeyPart(String table, String column, boolean desc) {
      List<KeyPart> list = keyParts.get(table);
      if (list == null) {
        list = new ArrayList<>();
        keyParts.put(table.toLowerCase(), list);
      }
      list.add(KeyPart.create(column.toLowerCase(), desc));
    }

    public SpannerSchema build() {
      return new SpannerSchema(columns, keyParts);
    }
  }

  private SpannerSchema(Map<String, List<Column>> columns, Map<String, List<KeyPart>> keyParts) {
    this.columns = columns;
    this.keyParts = keyParts;
    tables = new ArrayList<>(columns.keySet());
    tableIndex = new HashMap<>(tables.size());
    Collections.sort(tables);
    for (int i = 0; i < tables.size(); i++) {
      tableIndex.put(tables.get(i), i);
    }
  }

  public int getTableIndex(String tableName) {
    Integer result = tableIndex.get(tableName);
    Preconditions.checkArgument(result != null, "Table %s not found", tableName);
    return result;
  }

  public String getTableName(int index) {
    Preconditions.checkArgument(index < tables.size(), "Invalid table index %d", index);
    return tables.get(index);
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
}

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
import java.io.Serializable;
import java.util.List;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** Encapsulates Cloud Spanner Schema. */
@AutoValue
abstract class SpannerSchema implements Serializable {
  abstract ImmutableList<String> tables();

  abstract ImmutableListMultimap<String, Column> columns();

  abstract ImmutableListMultimap<String, KeyPart> keyParts();

  abstract ImmutableTable<String, String, Long> cellsMutatedPerColumn();

  abstract ImmutableMap<String, Long> cellsMutatedPerRow();

  public static Builder builder() {
    return new AutoValue_SpannerSchema.Builder();
  }

  /** Builder for {@link SpannerSchema}. */
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTables(ImmutableList<String> tablesBuilder);

    abstract ImmutableListMultimap.Builder<String, Column> columnsBuilder();

    abstract ImmutableListMultimap.Builder<String, KeyPart> keyPartsBuilder();

    abstract ImmutableTable.Builder<String, String, Long> cellsMutatedPerColumnBuilder();

    abstract ImmutableMap.Builder<String, Long> cellsMutatedPerRowBuilder();

    abstract ImmutableListMultimap<String, Column> columns();

    abstract ImmutableTable<String, String, Long> cellsMutatedPerColumn();

    @VisibleForTesting
    public Builder addColumn(String table, String name, String type) {
      return addColumn(table, name, type, 1L);
    }

    public Builder addColumn(String table, String name, String type, long cellsMutated) {
      String tableLower = table.toLowerCase();
      String nameLower = name.toLowerCase();

      columnsBuilder().put(tableLower, Column.create(nameLower, type));
      cellsMutatedPerColumnBuilder().put(tableLower, nameLower, cellsMutated);
      return this;
    }

    public Builder addKeyPart(String table, String column, boolean desc) {
      keyPartsBuilder().put(table.toLowerCase(), KeyPart.create(column.toLowerCase(), desc));
      return this;
    }

    abstract SpannerSchema autoBuild();

    public final SpannerSchema build() {
      // precompute the number of cells that are mutated for operations affecting
      // an entire row such as a single key delete.
      cellsMutatedPerRowBuilder()
          .putAll(
              Maps.transformValues(
                  cellsMutatedPerColumn().rowMap(),
                  entry -> entry.values().stream().mapToLong(Long::longValue).sum()));

      setTables(ImmutableList.copyOf(columns().keySet()));

      return autoBuild();
    }
  }

  public List<String> getTables() {
    return tables();
  }

  public List<Column> getColumns(String table) {
    return columns().get(table.toLowerCase());
  }

  public List<KeyPart> getKeyParts(String table) {
    return keyParts().get(table.toLowerCase());
  }

  /** Return the total number of cells affected when the specified column is mutated. */
  public long getCellsMutatedPerColumn(String table, String column) {
    return cellsMutatedPerColumn().row(table.toLowerCase()).getOrDefault(column.toLowerCase(), 1L);
  }

  /** Return the total number of cells affected with the given row is deleted. */
  public long getCellsMutatedPerRow(String table) {
    return cellsMutatedPerRow().getOrDefault(table.toLowerCase(), 1L);
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
      if ("BOOL".equals(spannerType)) {
        return Type.bool();
      }
      if ("INT64".equals(spannerType)) {
        return Type.int64();
      }
      if ("FLOAT64".equals(spannerType)) {
        return Type.float64();
      }
      if (spannerType.startsWith("STRING")) {
        return Type.string();
      }
      if (spannerType.startsWith("BYTES")) {
        return Type.bytes();
      }
      if ("TIMESTAMP".equals(spannerType)) {
        return Type.timestamp();
      }
      if ("DATE".equals(spannerType)) {
        return Type.date();
      }
      if ("NUMERIC".equals(spannerType)) {
        return Type.numeric();
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

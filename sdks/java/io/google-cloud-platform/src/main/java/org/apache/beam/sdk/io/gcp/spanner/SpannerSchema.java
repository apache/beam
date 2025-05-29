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
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/** Encapsulates Cloud Spanner Schema. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class SpannerSchema implements Serializable {

  abstract ImmutableList<String> tables();

  abstract Dialect dialect();

  abstract ImmutableListMultimap<String, Column> columns();

  abstract ImmutableListMultimap<String, KeyPart> keyParts();

  abstract ImmutableTable<String, String, Long> cellsMutatedPerColumn();

  abstract ImmutableMap<String, Long> cellsMutatedPerRow();

  public static Builder builder() {
    return builder(Dialect.GOOGLE_STANDARD_SQL);
  }

  public static Builder builder(Dialect dialect) {
    return new AutoValue_SpannerSchema.Builder().setDialect(dialect);
  }

  /** Builder for {@link SpannerSchema}. */
  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTables(ImmutableList<String> tablesBuilder);

    abstract Builder setDialect(Dialect dialect);

    abstract Dialect dialect();

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

      columnsBuilder().put(tableLower, Column.create(nameLower, type, dialect()));
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
  public abstract static class KeyPart implements Serializable {
    static KeyPart create(String field, boolean desc) {
      return new AutoValue_SpannerSchema_KeyPart(field, desc);
    }

    public abstract String getField();

    abstract boolean isDesc();
  }

  @AutoValue
  public abstract static class Column implements Serializable {

    static Column create(String name, Type type) {
      return new AutoValue_SpannerSchema_Column(name, type);
    }

    static Column create(String name, String spannerType, Dialect dialect) {
      return create(name, parseSpannerType(spannerType, dialect));
    }

    public abstract String getName();

    public abstract Type getType();

    private static final Map<String, Type> GOOGLE_STANDARD_SQL_TYPE_MAP =
        ImmutableMap.<String, Type>builder()
            .put("BOOL", Type.bool())
            .put("INT64", Type.int64())
            .put("FLOAT32", Type.float32())
            .put("FLOAT64", Type.float64())
            .put("UUID", Type.string())
            .put("TOKENLIST", Type.bytes())
            .put("TIMESTAMP", Type.timestamp())
            .put("DATE", Type.date())
            .put("NUMERIC", Type.numeric())
            .put("JSON", Type.json())
            .build();
    private static final Map<String, Type> POSTGRES_TYPE_MAP =
        ImmutableMap.<String, Type>builder()
            .put("BOOLEAN", Type.bool())
            .put("BIGINT", Type.int64())
            .put("REAL", Type.float32())
            .put("DOUBLE PRECISION", Type.float64())
            .put("TEXT", Type.string())
            .put("BYTEA", Type.bytes())
            .put("TIMESTAMP WITH TIME ZONE", Type.timestamp())
            .put("DATE", Type.date())
            .put("SPANNER.COMMIT_TIMESTAMP", Type.timestamp())
            .put("SPANNER.TOKENLIST", Type.bytes())
            .put("UUID", Type.string())
            .build();

    private static Type parseSpannerType(String spannerType, Dialect dialect) {
      String originalSpannerType = spannerType;
      spannerType = spannerType.toUpperCase();
      switch (dialect) {
        case GOOGLE_STANDARD_SQL:
          Type type = GOOGLE_STANDARD_SQL_TYPE_MAP.get(spannerType);
          if (type != null) {
            return type;
          }
          if (spannerType.startsWith("STRING")) {
            return Type.string();
          }
          if (spannerType.startsWith("BYTES")) {
            return Type.bytes();
          }
          if (spannerType.startsWith("ARRAY")) {
            // find 'xxx' in string ARRAY<xxxxx>
            // Graph DBs may have suffixes, eg ARRAY<FLOAT32>(vector_length=>256)
            //
            Pattern pattern = Pattern.compile("ARRAY<([^>]+)>");
            Matcher matcher = pattern.matcher(originalSpannerType);

            if (matcher.find()) {
              String spannerArrayType = matcher.group(1).trim();
              Type itemType = parseSpannerType(spannerArrayType, dialect);
              return Type.array(itemType);
            } else {
              // Handle the case where the regex doesn't match (invalid ARRAY type)
              throw new IllegalArgumentException("Invalid ARRAY type: " + originalSpannerType);
            }
          }
          if (spannerType.startsWith("PROTO")) {
            // Substring "PROTO<xxx>"
            String spannerProtoType =
                originalSpannerType.substring(6, originalSpannerType.length() - 1);
            return Type.proto(spannerProtoType);
          }
          if (spannerType.startsWith("ENUM")) {
            // Substring "ENUM<xxx>"
            String spannerEnumType =
                originalSpannerType.substring(5, originalSpannerType.length() - 1);
            return Type.protoEnum(spannerEnumType);
          }
          throw new IllegalArgumentException("Unknown spanner type " + spannerType);
        case POSTGRESQL:
          Pattern pattern = Pattern.compile("([^\\[]+)\\[\\]");
          Matcher m = pattern.matcher(spannerType);
          if (m.find()) {
            // Substring "xxx[]" or "xxx[] vector length yyy"
            // Must check array type first
            String spannerArrayType = m.group(1);
            Type itemType = parseSpannerType(spannerArrayType, dialect);
            return Type.array(itemType);
          }
          type = POSTGRES_TYPE_MAP.get(spannerType);
          if (type != null) {
            return type;
          }
          if (spannerType.startsWith("CHARACTER VARYING")) {
            return Type.string();
          }
          if (spannerType.startsWith("NUMERIC")) {
            return Type.pgNumeric();
          }
          if (spannerType.startsWith("JSONB")) {
            return Type.pgJsonb();
          }
          throw new IllegalArgumentException("Unknown spanner type " + spannerType);
        default:
          throw new IllegalArgumentException("Unrecognized dialect: " + dialect.name());
      }
    }
  }
}

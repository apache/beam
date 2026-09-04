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
package org.apache.beam.sdk.extensions.sql.meta.provider.delta;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.TableUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.DefaultTableFilter;
import org.apache.beam.sdk.extensions.sql.meta.ProjectSupport;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rex.RexNode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

class DeltaTable extends SchemaBaseBeamTable {
  @VisibleForTesting static final String VERSION_FIELD = "version";
  @VisibleForTesting static final String TIMESTAMP_FIELD = "timestamp";
  @VisibleForTesting static final String HADOOP_CONFIG_FIELD = "hadoop_config";
  @VisibleForTesting static final String HADOOP_CONFIG_CAMEL_FIELD = "hadoopConfig";

  static final String BEAM_WRITE_PROPERTY = "beam.write.";
  static final String BEAM_READ_PROPERTY = "beam.read.";

  @VisibleForTesting final String tableLocation;
  @VisibleForTesting final @Nullable Long version;
  @VisibleForTesting final @Nullable String timestamp;
  @VisibleForTesting final @Nullable Map<String, String> hadoopConfig;

  DeltaTable(Table table) {
    this(
        checkArgumentNotNull(
            table.getLocation(),
            "Delta Lake table location must be specified (catalog-based tables are not supported)."),
        table);
  }

  DeltaTable(String tableLocation, Table table) {
    super(table.getSchema());
    this.schema = table.getSchema();
    this.tableLocation = tableLocation;

    Long parsedVersion = null;
    String parsedTimestamp = null;
    Map<String, String> parsedHadoopConfig = new HashMap<>();

    ObjectNode properties = table.getProperties();
    for (Map.Entry<String, JsonNode> property : properties.properties()) {
      String key = property.getKey();
      String lowerKey = key.toLowerCase();
      JsonNode val = property.getValue();

      if (lowerKey.startsWith(BEAM_WRITE_PROPERTY)) {
        // TODO: Support writing to Delta Lake tables once a Delta Lake sink is
        // available.
        throw new IllegalArgumentException(
            String.format(
                "Beam write property '%s' is not supported. Writing to Delta Lake tables is currently not supported.",
                key));
      } else if (lowerKey.startsWith(BEAM_READ_PROPERTY)) {
        // none supported yet
        throw new IllegalArgumentException("Unknown Beam read property: " + key);
      } else if (lowerKey.equalsIgnoreCase(VERSION_FIELD)) {
        parsedVersion = parseVersion(val);
      } else if (lowerKey.equalsIgnoreCase(TIMESTAMP_FIELD)) {
        parsedTimestamp = val.asText();
      } else if (lowerKey.equalsIgnoreCase(HADOOP_CONFIG_FIELD)
          || lowerKey.equalsIgnoreCase(HADOOP_CONFIG_CAMEL_FIELD)) {
        parseHadoopConfig(val, parsedHadoopConfig);
      } else {
        throw new IllegalArgumentException(String.format("Unknown property '%s'", key));
      }
    }

    if (parsedVersion != null && parsedTimestamp != null) {
      throw new IllegalArgumentException("Cannot set both version and timestamp.");
    }

    this.version = parsedVersion;
    this.timestamp = parsedTimestamp;
    this.hadoopConfig = parsedHadoopConfig.isEmpty() ? null : parsedHadoopConfig;
  }

  private static Long parseVersion(JsonNode val) {
    if (val.isNumber()) {
      return val.asLong();
    }
    return Long.parseLong(val.asText());
  }

  private static void parseHadoopConfig(JsonNode val, Map<String, String> targetMap) {
    if (val.isObject()) {
      Map<String, String> map =
          TableUtils.getObjectMapper()
              .convertValue(val, new TypeReference<Map<String, String>>() {});
      if (map != null) {
        targetMap.putAll(map);
      }
    } else if (val.isTextual()) {
      try {
        Map<String, String> map =
            TableUtils.getObjectMapper()
                .readValue(val.asText(), new TypeReference<Map<String, String>>() {});
        if (map != null) {
          targetMap.putAll(map);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Failed to parse hadoop_config string as JSON", e);
      }
    }
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(Managed.read(Managed.DELTA_LAKE).withConfig(getBaseConfig()))
        .getSinglePCollection();
  }

  @Override
  public PCollection<Row> buildIOReader(
      PBegin begin, BeamSqlTableFilter filters, List<String> fieldNames) {
    // TODO: Support predicate pushdown and column pruning when supported by DeltaIO
    // / Managed Delta
    // Lake source.
    String error = "%s does not support predicate/project push-down, yet non-empty %s is passed.";
    if (!(filters instanceof DefaultTableFilter)) {
      throw new UnsupportedOperationException(
          String.format(error, this.getClass().getName(), "'filters'"));
    }
    if (!fieldNames.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format(error, this.getClass().getName(), "'fieldNames'"));
    }
    return buildIOReader(begin);
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    // TODO: Support writing to Delta Lake tables once a Delta Lake sink is
    // available.
    throw new UnsupportedOperationException(
        "Writing to Delta Lake tables is currently not supported.");
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public ProjectSupport supportsProjects() {
    // TODO: Support project pushdown / column pruning when supported by DeltaIO /
    // Managed Delta
    // Lake source.
    return ProjectSupport.NONE;
  }

  @Override
  public BeamSqlTableFilter constructFilter(List<RexNode> filter) {
    // TODO: Support predicate pushdown when supported by DeltaIO / Managed Delta
    // Lake source.
    return new DefaultTableFilter(filter);
  }

  private Map<String, Object> getBaseConfig() {
    ImmutableMap.Builder<String, Object> managedConfigBuilder = ImmutableMap.builder();
    managedConfigBuilder.put("table", tableLocation);
    if (version != null) {
      managedConfigBuilder.put(VERSION_FIELD, version);
    }
    if (timestamp != null) {
      managedConfigBuilder.put(TIMESTAMP_FIELD, timestamp);
    }
    if (hadoopConfig != null && !hadoopConfig.isEmpty()) {
      managedConfigBuilder.put(HADOOP_CONFIG_FIELD, hadoopConfig);
    }
    return managedConfigBuilder.build();
  }
}

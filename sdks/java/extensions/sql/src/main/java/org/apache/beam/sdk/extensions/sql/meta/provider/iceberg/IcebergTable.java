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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

class IcebergTable extends SchemaBaseBeamTable {
  @VisibleForTesting static final String CATALOG_PROPERTIES_FIELD = "catalog_properties";
  @VisibleForTesting static final String HADOOP_CONFIG_PROPERTIES_FIELD = "config_properties";
  @VisibleForTesting static final String CATALOG_NAME_FIELD = "catalog_name";

  @VisibleForTesting
  static final String TRIGGERING_FREQUENCY_FIELD = "triggering_frequency_seconds";

  @VisibleForTesting final String tableIdentifier;
  @VisibleForTesting @Nullable Map<String, String> catalogProps;
  @VisibleForTesting @Nullable Map<String, String> configProps;
  @VisibleForTesting @Nullable String catalogName;
  @VisibleForTesting @Nullable Integer triggeringFrequency;

  IcebergTable(Table table) {
    super(table.getSchema());
    this.schema = table.getSchema();
    this.tableIdentifier = checkArgumentNotNull(table.getLocation());
    ObjectNode properties = table.getProperties();
    ObjectMapper objectMapper = new ObjectMapper();
    if (properties.has(CATALOG_PROPERTIES_FIELD)) {
      this.catalogProps =
          objectMapper.convertValue(properties.get(CATALOG_PROPERTIES_FIELD), HashMap.class);
    }
    if (properties.has(HADOOP_CONFIG_PROPERTIES_FIELD)) {
      this.configProps =
          objectMapper.convertValue(properties.get(HADOOP_CONFIG_PROPERTIES_FIELD), HashMap.class);
    }
    if (properties.has(CATALOG_NAME_FIELD)) {
      this.catalogName = properties.get(CATALOG_NAME_FIELD).asText();
    }
    if (properties.has(TRIGGERING_FREQUENCY_FIELD)) {
      this.triggeringFrequency = properties.get(TRIGGERING_FREQUENCY_FIELD).asInt();
    }
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply(Managed.read(Managed.ICEBERG).withConfig(getBaseConfig()))
        .getSinglePCollection();
  }

  @Override
  public POutput buildIOWriter(PCollection<Row> input) {
    ImmutableMap.Builder<String, Object> configBuilder = ImmutableMap.builder();
    configBuilder.putAll(getBaseConfig());
    if (triggeringFrequency != null) {
      configBuilder.put(TRIGGERING_FREQUENCY_FIELD, triggeringFrequency);
    }
    return input.apply(Managed.write(Managed.ICEBERG).withConfig(configBuilder.build()));
  }

  private Map<String, Object> getBaseConfig() {
    ImmutableMap.Builder<String, Object> configBuilder = ImmutableMap.builder();
    configBuilder.put("table", tableIdentifier);
    if (catalogName != null) {
      configBuilder.put(CATALOG_NAME_FIELD, catalogName);
    }
    if (catalogProps != null) {
      configBuilder.put(CATALOG_PROPERTIES_FIELD, catalogProps);
    }
    if (configProps != null) {
      configBuilder.put(HADOOP_CONFIG_PROPERTIES_FIELD, configProps);
    }
    return configBuilder.build();
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }
}

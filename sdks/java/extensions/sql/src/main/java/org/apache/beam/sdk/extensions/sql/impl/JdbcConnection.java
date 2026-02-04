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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.catalog.CatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteConnection;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.Schema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.schema.SchemaPlus;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Beam JDBC Connection.
 *
 * <p>Implements and delegates to {@link CalciteConnection}, adds Beam-specific helper methods.
 * {@link BeamCalciteSchema BeamCalciteSchemas} keep reference to this connection. Pipeline options
 * are stored here.
 */
public class JdbcConnection extends CalciteConnectionWrapper {
  /**
   * Connection string parameters that begin with {@code "beam."} will be interpreted as {@link
   * PipelineOptions}.
   */
  private static final String PIPELINE_OPTION_PREFIX = "beam.";

  private Map<String, String> pipelineOptionsMap;
  private @Nullable PipelineOptions pipelineOptions;

  private JdbcConnection(CalciteConnection connection) throws SQLException {
    super(connection);
    this.pipelineOptionsMap = Collections.emptyMap();
  }

  /**
   * Wraps and initializes the initial connection created by Calcite.
   *
   * <p>Sets the pipeline options, replaces the initial non-functional top-level schema with schema
   * created by {@link BeamCalciteSchemaFactory}.
   */
  static JdbcConnection initialize(CalciteConnection connection) {
    try {
      String currentSchemaName =
          checkStateNotNull(
              connection.getSchema(), "When trying to initialize JdbcConnection: No schema set.");

      JdbcConnection jdbcConnection = new JdbcConnection(connection);
      jdbcConnection.setPipelineOptionsMap(extractPipelineOptions(connection));
      jdbcConnection.setSchema(
          currentSchemaName,
          BeamCalciteSchemaFactory.catalogFromInitialEmptySchema(jdbcConnection));
      return jdbcConnection;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads the connection properties starting with {@link #PIPELINE_OPTION_PREFIX} and converts them
   * to a map of pipeline options.
   */
  private static Map<String, String> extractPipelineOptions(CalciteConnection calciteConnection) {
    return calciteConnection.getProperties().entrySet().stream()
        .map(entry -> KV.of(entry.getKey().toString(), entry.getValue().toString()))
        .filter(kv -> kv.getKey().startsWith(PIPELINE_OPTION_PREFIX))
        .map(kv -> KV.of(kv.getKey().substring(PIPELINE_OPTION_PREFIX.length()), kv.getValue()))
        .collect(Collectors.toMap(KV::getKey, KV::getValue));
  }

  Map<String, String> getPipelineOptionsMap() {
    return pipelineOptionsMap;
  }

  /**
   * Only called from the {@link BeamCalciteSchema}. This is needed to enable the `{@code SET
   * pipelineOption = blah}` syntax
   */
  public void setPipelineOptionsMap(Map<String, String> pipelineOptionsMap) {
    this.pipelineOptionsMap = ImmutableMap.copyOf(pipelineOptionsMap);
  }

  public void setPipelineOptions(PipelineOptions pipelineOptions) {
    this.pipelineOptions = pipelineOptions;
  }

  public @Nullable PipelineOptions getPipelineOptions() {
    return this.pipelineOptions;
  }

  /** Get the current default schema from the root schema. */
  Schema getCurrentBeamSchema() {
    return CalciteSchema.from(getCurrentSchemaPlus()).schema;
  }

  /** Calcite-created {@link SchemaPlus} wrapper for the current schema. */
  public SchemaPlus getCurrentSchemaPlus() {
    String currentSchema;
    try {
      currentSchema = checkStateNotNull(getSchema(), "Current schema not set");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return checkStateNotNull(
        getRootSchema().getSubSchema(currentSchema),
        "SubSchema not found in `%s`: %s",
        getRootSchema().getName(),
        currentSchema);
  }

  /**
   * Sets the top-level schema '{@code name}' to '{@code tableProvider}'.
   *
   * <p>Overrides the schema if it exists.
   */
  void setSchema(String name, TableProvider tableProvider) {
    BeamCalciteSchema beamCalciteSchema = new BeamCalciteSchema(name, this, tableProvider);
    getRootSchema().add(name, beamCalciteSchema);
  }

  /** Like {@link #setSchema(String, TableProvider)} but using a {@link CatalogManager}. */
  void setSchema(String name, CatalogManager catalogManager) {
    CatalogManagerSchema catalogManagerSchema = new CatalogManagerSchema(this, catalogManager);
    getRootSchema().add(name, catalogManagerSchema);
  }
}

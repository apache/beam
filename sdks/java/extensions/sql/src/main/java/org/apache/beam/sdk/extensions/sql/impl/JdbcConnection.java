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

import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteConnection;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
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
  private PipelineOptions pipelineOptions;

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
  static @Nullable JdbcConnection initialize(CalciteConnection connection) {
    try {
      if (connection == null) {
        return null;
      }

      JdbcConnection jdbcConnection = new JdbcConnection(connection);
      jdbcConnection.setPipelineOptionsMap(extractPipelineOptions(connection));
      jdbcConnection.setSchema(
          connection.getSchema(), BeamCalciteSchemaFactory.fromInitialEmptySchema(jdbcConnection));
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

  public PipelineOptions getPipelineOptions() {
    return this.pipelineOptions;
  }

  /** Get the current default schema from the root schema. */
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <T> T getCurrentBeamSchema() {
    try {
      return (T) CalciteSchema.from(getRootSchema().getSubSchema(getSchema())).schema;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /** Calcite-created {@link SchemaPlus} wrapper for the current schema. */
  public SchemaPlus getCurrentSchemaPlus() {
    try {
      return getRootSchema().getSubSchema(getSchema());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Sets the top-level schema '{@code name}' to '{@code tableProvider}'.
   *
   * <p>Overrides the schema if it exists.
   */
  void setSchema(String name, TableProvider tableProvider) {
    BeamCalciteSchema beamCalciteSchema = new BeamCalciteSchema(this, tableProvider);
    getRootSchema().add(name, beamCalciteSchema);
  }
}

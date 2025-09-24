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
package org.apache.beam.sdk.io.jdbc.providers;

import static org.apache.beam.sdk.io.jdbc.JdbcUtil.MSSQL;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import java.util.Collections;
import java.util.List;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.jdbc.JdbcWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class WriteToSqlServerSchemaTransformProvider extends JdbcWriteSchemaTransformProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(WriteToSqlServerSchemaTransformProvider.class);

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.SQL_SERVER_WRITE);
  }

  @Override
  public String description() {
    return inheritedDescription("SQL Server", "WriteToSqlServer", "sqlserver", 1433);
  }

  @Override
  protected String jdbcType() {
    return MSSQL;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcWriteSchemaTransformConfiguration configuration) {
    String jdbcType = configuration.getJdbcType();
    if (jdbcType != null && !jdbcType.isEmpty() && !jdbcType.equals(jdbcType())) {
      LOG.warn(
          "Wrong JDBC type. Expected '{}' but got '{}'. Overriding with '{}'.",
          jdbcType(),
          jdbcType,
          jdbcType());
      configuration = configuration.toBuilder().setJdbcType(jdbcType()).build();
    }

    List<@org.checkerframework.checker.nullness.qual.Nullable String> connectionInitSql =
        configuration.getConnectionInitSql();
    if (connectionInitSql != null && !connectionInitSql.isEmpty()) {
      throw new IllegalArgumentException("SQL Server does not support connectionInitSql.");
    }

    // Override "connectionInitSql" for sqlserver
    configuration = configuration.toBuilder().setConnectionInitSql(Collections.emptyList()).build();
    return new SqlServerWriteSchemaTransform(configuration);
  }

  public static class SqlServerWriteSchemaTransform extends JdbcWriteSchemaTransform {
    public SqlServerWriteSchemaTransform(JdbcWriteSchemaTransformConfiguration config) {
      super(config, MSSQL);
      config.validate(MSSQL);
    }
  }
}

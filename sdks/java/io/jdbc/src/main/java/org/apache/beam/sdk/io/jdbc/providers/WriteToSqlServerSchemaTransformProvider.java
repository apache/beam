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

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.io.jdbc.JdbcWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class WriteToSqlServerSchemaTransformProvider extends JdbcWriteSchemaTransformProvider {

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:sql_server_write:v1";
  }

  @Override
  public String description() {
    return inheritedDescription("SQL Server", "WriteToSqlServer", "sqlserver", 1433);
  }

  @Override
  protected String jdbcType() {
    return MSSQL;
  }
}

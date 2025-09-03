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

import static org.apache.beam.sdk.io.jdbc.JdbcUtil.MYSQL;
import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;

import com.google.auto.service.AutoService;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.io.jdbc.JdbcWriteSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

@AutoService(SchemaTransformProvider.class)
public class WriteToMySqlSchemaTransformProvider extends JdbcWriteSchemaTransformProvider {

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return getUrn(ExternalTransforms.ManagedTransforms.Urns.MYSQL_WRITE);
  }

  @Override
  public String description() {
    return inheritedDescription("MySQL", "WriteToMySql", "mysql", 3306);
  }

  @Override
  protected String jdbcType() {
    return MYSQL;
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcWriteSchemaTransformConfiguration configuration) {
    String jdbcType = configuration.getJdbcType();
    if (jdbcType != null && !jdbcType.equals(jdbcType())) {
      throw new IllegalArgumentException(
          String.format("Wrong JDBC type. Expected '%s' but got '%s'", jdbcType(), jdbcType));
    }
    return super.from(configuration);
  }
}

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

import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;

public class IcebergTableProvider extends InMemoryMetaTableProvider {
  private IcebergCatalogConfig catalogConfig = IcebergCatalogConfig.builder().build();

  public static IcebergTableProvider create() {
    return new IcebergTableProvider();
  }

  public IcebergTableProvider withCatalogProperties(Map<String, String> catalogProperties) {
    catalogConfig = catalogConfig.toBuilder().setCatalogProperties(catalogProperties).build();
    return this;
  }

  public IcebergTableProvider withHadoopConfProperties(Map<String, String> hadoopConfProperties) {
    catalogConfig = catalogConfig.toBuilder().setConfigProperties(hadoopConfProperties).build();
    return this;
  }

  public IcebergTableProvider withCatalogName(String name) {
    catalogConfig = catalogConfig.toBuilder().setCatalogName(name).build();
    return this;
  }

  @Override
  public String getTableType() {
    return "iceberg";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new IcebergTable(table, catalogConfig);
  }
}

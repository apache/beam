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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class IcebergTableProvider extends InMemoryMetaTableProvider {
  // TODO(ahmedabu98): extend this to the IO implementation so
  //  other SDKs can make use of it too
  private static final String BEAM_HADOOP_PREFIX = "beam.catalog.hadoop";
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;

  public IcebergTableProvider(String name, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> catalogProps = ImmutableMap.builder();
    ImmutableMap.Builder<String, String> hadoopProps = ImmutableMap.builder();

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(BEAM_HADOOP_PREFIX)) {
        hadoopProps.put(entry.getKey(), entry.getValue());
      } else {
        catalogProps.put(entry.getKey(), entry.getValue());
      }
    }

    catalogConfig =
        IcebergCatalogConfig.builder()
            .setCatalogName(name)
            .setCatalogProperties(catalogProps.build())
            .setConfigProperties(hadoopProps.build())
            .build();
  }

  @Override
  public String getTableType() {
    return "iceberg";
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new IcebergTable(table, catalogConfig);
  }

  @Override
  public boolean supportsPartitioning() {
    return true;
  }
}

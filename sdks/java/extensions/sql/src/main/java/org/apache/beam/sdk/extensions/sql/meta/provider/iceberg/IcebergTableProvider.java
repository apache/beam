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
  private static final String BEAM_HADOOP_PREFIX = "beam.catalog.%s.hadoop";
  @VisibleForTesting IcebergCatalogConfig catalogConfig = IcebergCatalogConfig.builder().build();

  public static IcebergTableProvider create() {
    return new IcebergTableProvider();
  }

  static IcebergTableProvider create(String name, Map<String, String> properties) {
    IcebergTableProvider provider = new IcebergTableProvider();
    provider.initialize(name, properties);
    return provider;
  }

  public void initialize(String name, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> catalogProps = ImmutableMap.builder();
    ImmutableMap.Builder<String, String> hadoopProps = ImmutableMap.builder();
    String hadoopPrefix = String.format(BEAM_HADOOP_PREFIX, name);

    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(hadoopPrefix)) {
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
}

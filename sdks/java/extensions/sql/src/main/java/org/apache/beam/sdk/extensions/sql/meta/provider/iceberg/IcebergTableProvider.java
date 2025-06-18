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
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.TableAlreadyExistsException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A table provider for Iceberg tables. CREATE and DROP operations are performed on real external
 * tables.
 */
public class IcebergTableProvider implements TableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableProvider.class);
  // TODO(ahmedabu98): extend this to the IO implementation so
  //  other SDKs can make use of it too
  private static final String BEAM_HADOOP_PREFIX = "beam.catalog.hadoop";
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;
  private final Map<String, Table> tables = new HashMap<>();

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
  public void createTable(Table table) {
    try {
      catalogConfig.createTable(
          checkStateNotNull(table.getLocation()), table.getSchema(), table.getPartitionFields());
    } catch (TableAlreadyExistsException e) {
      LOG.info(
          "Iceberg table '{}' already exists at location '{}'.",
          table.getName(),
          table.getLocation());
    }
    tables.put(table.getName(), table);
  }

  @Override
  public void dropTable(String tableName) {
    Table table =
        checkArgumentNotNull(getTable(tableName), "Table '%s' is not registered.", tableName);
    String location = checkStateNotNull(table.getLocation());
    if (catalogConfig.dropTable(location)) {
      LOG.info("Dropped table '{}' (location: '{}').", tableName, location);
    } else {
      LOG.info(
          "Ignoring DROP TABLE call for '{}' (location: '{}') because it does not exist.",
          tableName,
          location);
    }
    tables.remove(tableName);
  }

  @Override
  public Map<String, Table> getTables() {
    return tables;
  }

  @Override
  public BeamSqlTable buildBeamSqlTable(Table table) {
    return new IcebergTable(table, catalogConfig);
  }

  @Override
  public boolean supportsPartitioning(Table table) {
    return true;
  }
}

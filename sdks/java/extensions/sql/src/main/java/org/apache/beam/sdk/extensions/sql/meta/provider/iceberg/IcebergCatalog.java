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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalog;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

public class IcebergCatalog extends InMemoryCatalog {
  // TODO(ahmedabu98): extend this to the IO implementation so
  //  other SDKs can make use of it too
  private static final String BEAM_HADOOP_PREFIX = "beam.catalog.hadoop";
  private final Map<String, IcebergMetastore> metaStores = new HashMap<>();
  @VisibleForTesting final IcebergCatalogConfig catalogConfig;

  public IcebergCatalog(String name, Map<String, String> properties) {
    super(name, properties);

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
  public IcebergMetastore metaStore(String db) {
    metaStores.putIfAbsent(db, new IcebergMetastore(db, catalogConfig));
    return metaStores.get(db);
  }

  @Override
  public String type() {
    return "iceberg";
  }

  @Override
  public boolean createDatabase(String database) {
    return catalogConfig.createNamespace(database);
  }

  @Override
  public void useDatabase(String database) {
    checkArgument(databaseExists(database), "Database '%s' does not exist.");
    currentDatabase = database;
  }

  @Override
  public boolean databaseExists(String db) {
    return catalogConfig.namespaceExists(db);
  }

  @Override
  public boolean dropDatabase(String database, boolean cascade) {
    boolean removed = catalogConfig.dropNamespace(database, cascade);
    metaStores.remove(database);
    if (database.equals(currentDatabase)) {
      currentDatabase = null;
    }
    return removed;
  }
}

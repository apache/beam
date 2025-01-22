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
package org.apache.beam.sdk.io.iceberg.catalog;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.iceberg.hive.testing.HiveMetastoreExtension;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

/**
 * Read and write tests using HiveCatalog.
 *
 * <p>Spins up a local Hive metastore to manage the Iceberg table. Warehouse path is set to a GCS
 * bucket.
 */
public class HiveCatalogIT extends IcebergCatalogBaseIT {
  private static HiveMetastoreExtension hiveMetastoreExtension;
  private static final String TEST_DB = "test_db";

  @Override
  public String tableId() {
    return String.format("%s.%s", TEST_DB, testName.getMethodName());
  }

  @Override
  public void catalogSetup() throws Exception {
    hiveMetastoreExtension = new HiveMetastoreExtension(warehouse);
    hiveMetastoreExtension.createDatabase(TEST_DB);
  }

  @Override
  public Catalog createCatalog() {
    return CatalogUtil.loadCatalog(
        "org.apache.iceberg.hive.HiveCatalog",
        "hive_" + catalogName,
        ImmutableMap.of(
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        hiveMetastoreExtension.hiveConf());
  }

  @Override
  public void catalogCleanup() throws Exception {
    if (hiveMetastoreExtension != null) {
      hiveMetastoreExtension.cleanup();
    }
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    String metastoreUri = hiveMetastoreExtension.metastoreUri();

    Map<String, String> confProperties =
        ImmutableMap.<String, String>builder().put("hive.metastore.uris", metastoreUri).build();

    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("name", "hive_" + catalogName)
        .put("config_properties", confProperties)
        .build();
  }
}

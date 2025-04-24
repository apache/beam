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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.iceberg.catalog.hiveutils.HiveMetastoreExtension;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Read and write tests using {@link HiveCatalog}.
 *
 * <p>Spins up a local Hive metastore to manage the Iceberg table. Warehouse path is set to a GCS
 * bucket.
 */
public class HiveCatalogIT extends IcebergCatalogBaseIT {
  private static HiveMetastoreExtension hiveMetastoreExtension;
  private long salt = System.nanoTime();

  private String testDb() {
    return "test_db_" + testName.getMethodName();
  }

  @Override
  public String tableId() {
    return String.format("%s.%s", testDb(), "test_table" + "_" + salt);
  }

  @BeforeClass
  public static void setUpClass() throws MetaException {
    String warehouse = warehouse(HiveCatalogIT.class);
    hiveMetastoreExtension = new HiveMetastoreExtension(warehouse);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveMetastoreExtension != null) {
      hiveMetastoreExtension.cleanup();
    }
  }

  @Override
  public void catalogSetup() throws Exception {
    String dbPath = hiveMetastoreExtension.metastore().getDatabasePath(testDb());
    Database db = new Database(testDb(), "description", dbPath, Maps.newHashMap());
    hiveMetastoreExtension.metastoreClient().createDatabase(db);
  }

  @Override
  public Catalog createCatalog() {
    salt += System.nanoTime();
    return CatalogUtil.loadCatalog(
        HiveCatalog.class.getName(),
        "hive_" + catalogName,
        ImmutableMap.of(
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
            String.valueOf(TimeUnit.SECONDS.toMillis(10))),
        hiveMetastoreExtension.hiveConf());
  }

  @Override
  public void catalogCleanup() throws Exception {
    if (hiveMetastoreExtension != null) {
      List<String> tables = hiveMetastoreExtension.metastoreClient().getAllTables(testDb());
      for (String table : tables) {
        hiveMetastoreExtension.metastoreClient().dropTable(testDb(), table, true, false);
      }
      hiveMetastoreExtension.metastoreClient().dropDatabase(testDb());
    }
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    String metastoreUri = hiveMetastoreExtension.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);

    Map<String, String> confProperties =
        ImmutableMap.<String, String>builder()
            .put(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUri)
            .build();

    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("catalog_name", "hive_" + catalogName)
        .put(
            "catalog_properties",
            ImmutableMap.of("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO"))
        .put("config_properties", confProperties)
        .build();
  }
}

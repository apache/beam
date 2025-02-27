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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.iceberg.catalog.hiveutils.HiveMetastoreExtension;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read and write tests using {@link HiveCatalog}.
 *
 * <p>Spins up a local Hive metastore to manage the Iceberg table. Warehouse path is set to a GCS
 * bucket.
 */
public class HiveCatalogIT extends IcebergCatalogBaseIT {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalogIT.class);
  private static HiveMetastoreExtension hiveMetastoreExtension;

  private static String testDb() {
    return "test_db";
  }

  @Override
  public String tableId() {
    return String.format("%s.%s%s_%d", testDb(), "test_table_", testName.getMethodName(), salt);
  }

  @Override
  public void verifyTableExists(TableIdentifier tableIdentifier) throws Exception {
    // Wait and verify that the table exists
    for (int i = 0; i < 30; i++) { // Retry up to 30 times with 1 sec delay
      List<String> tables = hiveMetastoreExtension.metastoreClient().getAllTables(testDb());
      if (tables.contains(tableIdentifier.name())) {
        LOG.info("Table {} is now visible in the catalog.", tableIdentifier.name());
        break;
      }
      if (i % 10 == 0) {
        for (String table : tables) {
          LOG.info("TABLE EXISTING IN HIVE: {}", table);
        }
      }
      LOG.warn("Table {} is not visible yet, retrying... (attempt {}/{})", tableIdentifier.name(), i + 1, 30);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    String warehouse = warehouse(HiveCatalogIT.class, UUID.randomUUID().toString());
    hiveMetastoreExtension = new HiveMetastoreExtension(warehouse);
    String dbPath = hiveMetastoreExtension.metastore().getDatabasePath(testDb());
    Database db = new Database(testDb(), "description", dbPath, Maps.newHashMap());
    hiveMetastoreExtension.metastoreClient().createDatabase(db);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (hiveMetastoreExtension != null) {
      hiveMetastoreExtension.metastoreClient().dropDatabase(testDb());
      hiveMetastoreExtension.cleanup();
    }
  }

  @Override
  public Catalog createCatalog() {
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
        if (table.contains(String.valueOf(salt))) {
          hiveMetastoreExtension.metastoreClient().dropTable(testDb(), table, true, false);
        }
      }
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

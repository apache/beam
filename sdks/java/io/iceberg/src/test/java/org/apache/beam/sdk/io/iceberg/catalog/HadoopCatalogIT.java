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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopCatalogIT extends IcebergCatalogBaseIT {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopCatalogIT.class);

  @Override
  public String tableId() {
    return testName.getMethodName() + ".test_table_" + salt;
  }

  @Override
  public void verifyTableExists(TableIdentifier tableIdentifier) {
    // Wait and verify that the table exists
    for (int i = 0; i < 20; i++) { // Retry up to 10 times with 1 sec delay
      HadoopCatalog hadoopCatalog = (HadoopCatalog) catalog;
      List<TableIdentifier> tables = hadoopCatalog.listTables(Namespace.of(testName.getMethodName()));
      if (tables.contains(tableIdentifier)) {
        LOG.info("Table {} is now visible in the catalog.", tableIdentifier.name());
        break;
      }
      LOG.warn("Table {} is not visible yet, retrying... (attempt {}/{})", tableIdentifier.name(), i + 1, 20);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public Integer numRecords() {
    return 100;
  }

  @Override
  public Catalog createCatalog() {
    Configuration catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", OPTIONS.getProject());
    catalogHadoopConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");

    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(catalogHadoopConf);
    catalog.initialize("hadoop_" + catalogName, ImmutableMap.of("warehouse", warehouse));

    return catalog;
  }

  @Override
  public void catalogCleanup() throws IOException {
    HadoopCatalog hadoopCatalog = (HadoopCatalog) catalog;
    List<TableIdentifier> tables = hadoopCatalog.listTables(Namespace.of(testName.getMethodName()));
    for (TableIdentifier identifier : tables) {
      if (identifier.name().contains(String.valueOf(salt))) {
        hadoopCatalog.dropTable(identifier);
      }
    }
    hadoopCatalog.close();
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put(
            "catalog_properties",
            ImmutableMap.<String, String>builder()
                .put("type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
                .put("warehouse", warehouse)
                .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .build())
        .build();
  }
}

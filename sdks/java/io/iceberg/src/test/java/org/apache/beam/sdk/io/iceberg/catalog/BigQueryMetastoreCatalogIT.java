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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public class BigQueryMetastoreCatalogIT extends IcebergCatalogBaseIT {
  static final String BQMS_CATALOG = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  static final String DATASET = "managed_iceberg_bqms_tests_no_delete";
  static final long SALT = System.nanoTime();

  @Override
  public String tableId() {
    return DATASET + "." + testName.getMethodName() + "_" + SALT;
  }

  @Override
  public Catalog createCatalog() {
    return CatalogUtil.loadCatalog(
        BQMS_CATALOG,
        "bqms_" + catalogName,
        ImmutableMap.<String, String>builder()
            .put("gcp_project", options.getProject())
            .put("gcp_location", "us-central1")
            .put("warehouse", warehouse)
            .build(),
        new Configuration());
  }

  @Override
  public void catalogCleanup() {
    for (TableIdentifier tableIdentifier : catalog.listTables(Namespace.of(DATASET))) {
      // only delete tables that were created in this test run
      if (tableIdentifier.name().contains(String.valueOf(SALT))) {
        catalog.dropTable(tableIdentifier);
      }
    }
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put(
            "catalog_properties",
            ImmutableMap.<String, String>builder()
                .put("gcp_project", options.getProject())
                .put("gcp_location", "us-central1")
                .put("warehouse", warehouse)
                .put("catalog-impl", BQMS_CATALOG)
                .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .build())
        .build();
  }
}

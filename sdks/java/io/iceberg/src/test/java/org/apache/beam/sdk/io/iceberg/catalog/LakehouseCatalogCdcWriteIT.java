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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.BeforeClass;

/** {@link IcebergCdcWriteBaseIT} against the Lakehouse REST catalog. */
public class LakehouseCatalogCdcWriteIT extends IcebergCdcWriteBaseIT {
  private static Map<String, String> catalogProps;

  private static final String LAKEHOUSE_WAREHOUSE =
      System.getProperty("beam.iceberg.biglake.warehouse", "gs://managed-iceberg-biglake-its");

  @BeforeClass
  public static void setup() {
    warehouse = LAKEHOUSE_WAREHOUSE;
    catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
            .put("warehouse", LAKEHOUSE_WAREHOUSE)
            .put("header.x-goog-user-project", OPTIONS.getProject())
            .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
            .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
            .build();
  }

  @After
  public void after() {
    // The base class points its cleanup at this warehouse.
    warehouse = LAKEHOUSE_WAREHOUSE;
  }

  @Override
  public String type() {
    return "lakehouse";
  }

  @Override
  public Catalog createCatalog() {
    RESTCatalog restCatalog = new RESTCatalog();
    restCatalog.initialize(catalogName, catalogProps);
    return restCatalog;
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("catalog_properties", catalogProps)
        .build();
  }
}

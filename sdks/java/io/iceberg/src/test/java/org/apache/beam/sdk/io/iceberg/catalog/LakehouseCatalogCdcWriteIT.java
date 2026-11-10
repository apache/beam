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
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.LakehouseTestCatalog;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.BeforeClass;

/** {@link IcebergCdcWriteBaseIT} against the Lakehouse REST catalog. */
public class LakehouseCatalogCdcWriteIT extends IcebergCdcWriteBaseIT {
  private static Map<String, String> catalogProps;

  @BeforeClass
  public static void setup() {
    warehouse = LakehouseTestCatalog.defaultLocation();
    catalogProps = LakehouseTestCatalog.catalogProperties();
  }

  @After
  public void after() throws IOException {
    // Lakehouse keeps a dropped table's files, so remove them before the base class drops the
    // namespace.
    LakehouseTestCatalog.dropTablesAndFiles(catalog, namespace());
    // The base class points its cleanup at this warehouse.
    warehouse = LakehouseTestCatalog.defaultLocation();
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

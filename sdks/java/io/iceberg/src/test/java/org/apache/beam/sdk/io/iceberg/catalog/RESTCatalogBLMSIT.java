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
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.BeforeClass;

/** Tests for {@link org.apache.iceberg.rest.RESTCatalog} using BigLake Metastore. */
public class RESTCatalogBLMSIT extends IcebergCatalogBaseIT {
  private static Map<String, String> catalogProps;

  // Using a special bucket for this test class because
  // BigLake does not support using subfolders as a warehouse (yet).
  // Overridable for local runs against a different project's catalog, e.g.
  // -Dbeam.iceberg.biglake.warehouse=gs://my-bucket (bucket-backed catalogs are named after
  // their bucket).
  private static final String BIGLAKE_WAREHOUSE =
      System.getProperty("beam.iceberg.biglake.warehouse", "gs://managed-iceberg-biglake-its");

  @BeforeClass
  public static void setup() {
    warehouse = BIGLAKE_WAREHOUSE;
    catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", "https://biglake.googleapis.com/iceberg/v1/restcatalog")
            .put("warehouse", BIGLAKE_WAREHOUSE)
            .put("header.x-goog-user-project", OPTIONS.getProject())
            .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
            .put("rest.auth.type", "org.apache.iceberg.gcp.auth.GoogleAuthManager")
            .build();
  }

  @After
  public void after() {
    // making sure the cleanup path is directed at the correct warehouse
    warehouse = BIGLAKE_WAREHOUSE;
  }

  @Override
  public String type() {
    return "biglake";
  }

  @Override
  public String bigQueryTableSpec(String tableId) {
    // BigQuery surfaces Lakehouse runtime catalog (BigLake metastore REST) tables via 4-part
    // project.catalog.namespace.table identifiers; the catalog id of a bucket-backed catalog is
    // the bucket name. Requires the caller to hold biglake.* read permissions (e.g.
    // roles/biglake.viewer) in addition to the usual BigQuery roles.
    TableIdentifier identifier = TableIdentifier.parse(tableId);
    String catalogId = BIGLAKE_WAREHOUSE.replace("gs://", "");
    return String.format(
        "%s.%s.%s.%s", OPTIONS.getProject(), catalogId, identifier.namespace(), identifier.name());
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

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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.IOException;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.BeforeClass;

/** Tests for {@link org.apache.iceberg.rest.RESTCatalog} using BigLake Metastore. */
public class RESTCatalogBLMSIT extends IcebergCatalogBaseIT {
  private static final String BIGLAKE_URI =
      "https://biglake.googleapis.com/iceberg/v1beta/restcatalog";
  private static Map<String, String> catalogProps;

  @BeforeClass
  public static void setup() throws IOException {
    GoogleCredentials credentials =
        ServiceAccountCredentials.getApplicationDefault()
            .createScoped("https://www.googleapis.com/auth/cloud-platform");
    credentials.refreshIfExpired();
    String accessToken = credentials.getAccessToken().getTokenValue();

    catalogProps =
        ImmutableMap.<String, String>builder()
            .put("type", "rest")
            .put("uri", BIGLAKE_URI)
            .put("warehouse", "gs://managed-iceberg-integration-tests-us-central1")
            .put("oauth2-server-uri", "https://oauth2.googleapis.com/token")
            .put("header.x-goog-user-project", OPTIONS.getProject())
            .put("rest-metrics-reporting-enabled", "false")
            .put("token", accessToken)
            .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
            .build();
  }

  @Override
  public String type() {
    return "rest";
  }

  @Override
  public Catalog createCatalog() {
    return CatalogUtil.loadCatalog(
        RESTCatalog.class.getName(), catalogName, catalogProps, new Configuration());
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("catalog_properties", catalogProps)
        .build();
  }
}

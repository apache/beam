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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class HadoopCatalogIT extends IcebergCatalogBaseIT {
  @Override
  public Integer numRecords() {
    return 100;
  }

  @Override
  public Catalog createCatalog() {
    Configuration catalogHadoopConf = new Configuration();
    catalogHadoopConf.set("fs.gs.project.id", options.getProject());
    catalogHadoopConf.set("fs.gs.auth.type", "APPLICATION_DEFAULT");

    HadoopCatalog catalog = new HadoopCatalog();
    catalog.setConf(catalogHadoopConf);
    catalog.initialize("hadoop_" + catalogName, ImmutableMap.of("warehouse", warehouse));

    return catalog;
  }

  @Override
  public void catalogCleanup() throws IOException {
    ((HadoopCatalog) catalog).close();
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
                .build())
        .build();
  }
}

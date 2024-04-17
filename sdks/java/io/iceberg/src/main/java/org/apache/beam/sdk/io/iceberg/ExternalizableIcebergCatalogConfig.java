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
package org.apache.beam.sdk.io.iceberg;

import com.google.common.base.Preconditions;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper to more precisely serialize a {@link IcebergCatalogConfig}.
 *
 * <p>Only includes properties used in {@link IcebergCatalogConfig#catalog()} to generate a {@link
 * org.apache.iceberg.catalog.Catalog}:
 *
 * <ul>
 *   <li>{@link IcebergCatalogConfig#getName()}
 *   <li>{@link IcebergCatalogConfig#getIcebergCatalogType()}
 *   <li>{@link IcebergCatalogConfig#getCatalogImplementation()}
 *   <li>{@link IcebergCatalogConfig#getFileIOImplementation()}
 *   <li>{@link IcebergCatalogConfig#getWarehouseLocation()}
 *   <li>{@link IcebergCatalogConfig#getMetricsReporterImplementation()}
 *   <li>{@link IcebergCatalogConfig#getCacheEnabled()}
 *   <li>{@link IcebergCatalogConfig#getCacheCaseSensitive()} ()}
 *   <li>{@link IcebergCatalogConfig#getCacheExpirationIntervalMillis()}
 *   <li>{@link IcebergCatalogConfig#getConfiguration()}
 * </ul>
 *
 * *
 */
public class ExternalizableIcebergCatalogConfig implements Externalizable {
  private static final long serialVersionUID = 0L;

  private @Nullable IcebergCatalogConfig catalogConfig;

  // Keep this in sync with IcebergCatalogConfig properties map
  static List<String> PROPERTY_KEYS =
      ImmutableList.<String>builder()
          .add("name")
          .add(CatalogUtil.ICEBERG_CATALOG_TYPE)
          .add(CatalogProperties.CATALOG_IMPL)
          .add(CatalogProperties.FILE_IO_IMPL)
          .add(CatalogProperties.WAREHOUSE_LOCATION)
          .add(CatalogProperties.METRICS_REPORTER_IMPL)
          .add(CatalogProperties.CACHE_ENABLED)
          .add(CatalogProperties.CACHE_CASE_SENSITIVE)
          .add(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS)
          .build();

  public ExternalizableIcebergCatalogConfig() {}

  public ExternalizableIcebergCatalogConfig(@Nullable IcebergCatalogConfig catalogConfig) {
    if (catalogConfig == null) {
      throw new NullPointerException("Configuration must not be null.");
    }
    this.catalogConfig = catalogConfig;
  }

  public @Nullable IcebergCatalogConfig get() {
    return catalogConfig;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    if (catalogConfig == null) {
      return;
    }
    Map<String, String> properties = new HashMap<>(PROPERTY_KEYS.size());
    properties.put("name", catalogConfig.getName());
    properties.putAll(Preconditions.checkNotNull(catalogConfig).properties());
    for (String prop : PROPERTY_KEYS) {
      out.writeUTF(properties.getOrDefault(prop, ""));
    }
    if (catalogConfig != null && catalogConfig.getConfiguration() != null) {
      catalogConfig.getConfiguration().write(out);
    }
  }

  private @Nullable String orNull(String value) {
    return Strings.isNullOrEmpty(value) ? null : value;
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    IcebergCatalogConfig.Builder builder =
        IcebergCatalogConfig.builder()
            .setName(in.readUTF())
            .setIcebergCatalogType(orNull(in.readUTF()))
            .setCatalogImplementation(orNull(in.readUTF()))
            .setFileIOImplementation(orNull(in.readUTF()))
            .setWarehouseLocation(orNull(in.readUTF()))
            .setMetricsReporterImplementation(orNull(in.readUTF()))
            .setCacheEnabled(Boolean.parseBoolean(in.readUTF()))
            .setCacheCaseSensitive(Boolean.parseBoolean(in.readUTF()))
            .setCacheExpirationIntervalMillis(Long.parseLong(in.readUTF()));
    if (in.available() > 0) {
      Configuration hadoopConf = new Configuration();
      hadoopConf.readFields(in);
      builder = builder.setConfiguration(hadoopConf);
    }
    catalogConfig = builder.build();
  }
}

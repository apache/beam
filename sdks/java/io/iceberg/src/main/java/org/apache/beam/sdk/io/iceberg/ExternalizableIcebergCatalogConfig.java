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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A wrapper to more precisely serialize a {@link IcebergCatalogConfig} object. {@link
 * IcebergCatalogConfig} is an AutoValue class, which raises some complications when trying to have
 * it directly implement {@link Externalizable}. Hence, this class is used to wrap around the {@link
 * IcebergCatalogConfig} object when serializing and deserializing. *
 */
public class ExternalizableIcebergCatalogConfig implements Externalizable {
  private static final long serialVersionUID = 0L;

  private @Nullable IcebergCatalogConfig catalogConfig;

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
  @SuppressWarnings("nullness")
  public void writeExternal(ObjectOutput out) throws IOException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("name", catalogConfig.getName());
    properties.put("icebergCatalogType", catalogConfig.getIcebergCatalogType());
    properties.put("catalogImplementation", catalogConfig.getCatalogImplementation());
    properties.put("fileIOImplementation", catalogConfig.getFileIOImplementation());
    properties.put("warehouseLocation", catalogConfig.getWarehouseLocation());
    properties.put(
        "metricsReporterImplementation", catalogConfig.getMetricsReporterImplementation());
    properties.put("cacheEnabled", catalogConfig.getCacheEnabled());
    properties.put("cacheCaseSensitive", catalogConfig.getCacheCaseSensitive());
    properties.put(
        "cacheExpirationIntervalMillis", catalogConfig.getCacheExpirationIntervalMillis());
    properties.put("ioManifestCacheEnabled", catalogConfig.getIOManifestCacheEnabled());
    properties.put(
        "ioManifestCacheExpirationIntervalMillis",
        catalogConfig.getIOManifestCacheExpirationIntervalMillis());
    properties.put("ioManifestCacheMaxTotalBytes", catalogConfig.getIOManifestCacheMaxTotalBytes());
    properties.put(
        "ioManifestCacheMaxContentLength", catalogConfig.getIOManifestCacheMaxContentLength());
    properties.put("uri", catalogConfig.getUri());
    properties.put("clientPoolSize", catalogConfig.getClientPoolSize());
    properties.put("clientPoolEvictionIntervalMs", catalogConfig.getClientPoolEvictionIntervalMs());
    properties.put("clientPoolCacheKeys", catalogConfig.getClientPoolCacheKeys());
    properties.put("lockImplementation", catalogConfig.getLockImplementation());
    properties.put("lockHeartbeatIntervalMillis", catalogConfig.getLockHeartbeatIntervalMillis());
    properties.put("lockHeartbeatTimeoutMillis", catalogConfig.getLockHeartbeatTimeoutMillis());
    properties.put("lockHeartbeatThreads", catalogConfig.getLockHeartbeatThreads());
    properties.put("lockAcquireIntervalMillis", catalogConfig.getLockAcquireIntervalMillis());
    properties.put("lockAcquireTimeoutMillis", catalogConfig.getLockAcquireTimeoutMillis());
    properties.put("appIdentifier", catalogConfig.getAppIdentifier());
    properties.put("user", catalogConfig.getUser());
    properties.put("authSessionTimeoutMillis", catalogConfig.getAuthSessionTimeoutMillis());
    properties.put(
        "configuration",
        catalogConfig.getConfiguration() == null
            ? null
            : new SerializableConfiguration(catalogConfig.getConfiguration()));

    out.writeObject(properties);
  }

  @Override
  @SuppressWarnings("nullness")
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    Map<String, Object> properties = (Map<String, Object>) in.readObject();
    catalogConfig =
        IcebergCatalogConfig.builder()
            .setName((String) properties.get("name"))
            .setIcebergCatalogType((String) properties.get("icebergCatalogType"))
            .setCatalogImplementation((String) properties.get("catalogImplementation"))
            .setFileIOImplementation((String) properties.get("fileIOImplementation"))
            .setWarehouseLocation((String) properties.get("warehouseLocation"))
            .setMetricsReporterImplementation(
                (String) properties.get("metricsReporterImplementation"))
            .setCacheEnabled((Boolean) properties.get("cacheEnabled"))
            .setCacheCaseSensitive((Boolean) properties.get("cacheCaseSensitive"))
            .setCacheExpirationIntervalMillis(
                (Long) properties.get("cacheExpirationIntervalMillis"))
            .setIOManifestCacheEnabled((Boolean) properties.get("ioManifestCacheEnabled"))
            .setIOManifestCacheExpirationIntervalMillis(
                (Long) properties.get("ioManifestCacheExpirationIntervalMillis"))
            .setIOManifestCacheMaxTotalBytes((Long) properties.get("ioManifestCacheMaxTotalBytes"))
            .setIOManifestCacheMaxContentLength(
                (Long) properties.get("ioManifestCacheMaxContentLength"))
            .setUri((String) properties.get("uri"))
            .setClientPoolSize((Integer) properties.get("clientPoolSize"))
            .setClientPoolEvictionIntervalMs((Long) properties.get("clientPoolEvictionIntervalMs"))
            .setClientPoolCacheKeys((String) properties.get("clientPoolCacheKeys"))
            .setLockImplementation((String) properties.get("lockImplementation"))
            .setLockHeartbeatIntervalMillis((Long) properties.get("lockHeartbeatIntervalMillis"))
            .setLockHeartbeatTimeoutMillis((Long) properties.get("lockHeartbeatTimeoutMillis"))
            .setLockHeartbeatThreads((Integer) properties.get("lockHeartbeatThreads"))
            .setLockAcquireIntervalMillis((Long) properties.get("lockAcquireIntervalMillis"))
            .setLockAcquireTimeoutMillis((Long) properties.get("lockAcquireTimeoutMillis"))
            .setAppIdentifier((String) properties.get("appIdentifier"))
            .setUser((String) properties.get("user"))
            .setAuthSessionTimeoutMillis((Long) properties.get("authSessionTimeoutMillis"))
            .setConfiguration(
                properties.get("configuration") == null
                    ? null
                    : ((SerializableConfiguration) properties.get("configuration")).get())
            .build();
  }
}

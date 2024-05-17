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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.checkerframework.dataflow.qual.Pure;

@AutoValue
public abstract class IcebergCatalogConfig implements Serializable {

  @Pure
  public abstract String getName();

  /* Core Properties */
  @Pure
  public abstract @Nullable String getIcebergCatalogType();

  @Pure
  public abstract @Nullable String getCatalogImplementation();

  @Pure
  public abstract @Nullable String getFileIOImplementation();

  @Pure
  public abstract @Nullable String getWarehouseLocation();

  @Pure
  public abstract @Nullable String getMetricsReporterImplementation();

  /* Caching */
  @Pure
  public abstract boolean getCacheEnabled();

  @Pure
  public abstract boolean getCacheCaseSensitive();

  @Pure
  public abstract long getCacheExpirationIntervalMillis();

  @Pure
  public abstract boolean getIOManifestCacheEnabled();

  @Pure
  public abstract long getIOManifestCacheExpirationIntervalMillis();

  @Pure
  public abstract long getIOManifestCacheMaxTotalBytes();

  @Pure
  public abstract long getIOManifestCacheMaxContentLength();

  @Pure
  public abstract @Nullable String getUri();

  @Pure
  public abstract int getClientPoolSize();

  @Pure
  public abstract long getClientPoolEvictionIntervalMs();

  @Pure
  public abstract @Nullable String getClientPoolCacheKeys();

  @Pure
  public abstract @Nullable String getLockImplementation();

  @Pure
  public abstract long getLockHeartbeatIntervalMillis();

  @Pure
  public abstract long getLockHeartbeatTimeoutMillis();

  @Pure
  public abstract int getLockHeartbeatThreads();

  @Pure
  public abstract long getLockAcquireIntervalMillis();

  @Pure
  public abstract long getLockAcquireTimeoutMillis();

  @Pure
  public abstract @Nullable String getAppIdentifier();

  @Pure
  public abstract @Nullable String getUser();

  @Pure
  public abstract long getAuthSessionTimeoutMillis();

  @Pure
  public abstract @Nullable Configuration getConfiguration();

  @Pure
  public static Builder builder() {
    return new AutoValue_IcebergCatalogConfig.Builder()
        .setIcebergCatalogType(null)
        .setCatalogImplementation(null)
        .setFileIOImplementation(null)
        .setWarehouseLocation(null)
        .setMetricsReporterImplementation(null) // TODO: Set this to our implementation
        .setCacheEnabled(CatalogProperties.CACHE_ENABLED_DEFAULT)
        .setCacheCaseSensitive(CatalogProperties.CACHE_CASE_SENSITIVE_DEFAULT)
        .setCacheExpirationIntervalMillis(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
        .setIOManifestCacheEnabled(CatalogProperties.IO_MANIFEST_CACHE_ENABLED_DEFAULT)
        .setIOManifestCacheExpirationIntervalMillis(
            CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
        .setIOManifestCacheMaxTotalBytes(
            CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT)
        .setIOManifestCacheMaxContentLength(
            CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT)
        .setUri(null)
        .setClientPoolSize(CatalogProperties.CLIENT_POOL_SIZE_DEFAULT)
        .setClientPoolEvictionIntervalMs(
            CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT)
        .setClientPoolCacheKeys(null)
        .setLockImplementation(null)
        .setLockHeartbeatIntervalMillis(CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT)
        .setLockHeartbeatTimeoutMillis(CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT)
        .setLockHeartbeatThreads(CatalogProperties.LOCK_HEARTBEAT_THREADS_DEFAULT)
        .setLockAcquireIntervalMillis(CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS_DEFAULT)
        .setLockAcquireTimeoutMillis(CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT)
        .setAppIdentifier(null)
        .setUser(null)
        .setAuthSessionTimeoutMillis(CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT)
        .setConfiguration(null);
  }

  @Pure
  public ImmutableMap<String, String> properties() {
    return new PropertyBuilder()
        .put(CatalogUtil.ICEBERG_CATALOG_TYPE, getIcebergCatalogType())
        .put(CatalogProperties.CATALOG_IMPL, getCatalogImplementation())
        .put(CatalogProperties.FILE_IO_IMPL, getFileIOImplementation())
        .put(CatalogProperties.WAREHOUSE_LOCATION, getWarehouseLocation())
        .put(CatalogProperties.METRICS_REPORTER_IMPL, getMetricsReporterImplementation())
        .put(CatalogProperties.CACHE_ENABLED, getCacheEnabled())
        .put(CatalogProperties.CACHE_CASE_SENSITIVE, getCacheCaseSensitive())
        .put(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS, getCacheExpirationIntervalMillis())
        .build();
  }

  public org.apache.iceberg.catalog.Catalog catalog() {
    Configuration conf = getConfiguration();
    if (conf == null) {
      conf = new Configuration();
    }
    return CatalogUtil.buildIcebergCatalog(getName(), properties(), conf);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /* Core Properties */
    public abstract Builder setName(String name);

    public abstract Builder setIcebergCatalogType(@Nullable String icebergType);

    public abstract Builder setCatalogImplementation(@Nullable String catalogImpl);

    public abstract Builder setFileIOImplementation(@Nullable String fileIOImpl);

    public abstract Builder setWarehouseLocation(@Nullable String warehouse);

    public abstract Builder setMetricsReporterImplementation(@Nullable String metricsImpl);

    /* Caching */
    public abstract Builder setCacheEnabled(boolean cacheEnabled);

    public abstract Builder setCacheCaseSensitive(boolean cacheCaseSensitive);

    public abstract Builder setCacheExpirationIntervalMillis(long expiration);

    public abstract Builder setIOManifestCacheEnabled(boolean enabled);

    public abstract Builder setIOManifestCacheExpirationIntervalMillis(long expiration);

    public abstract Builder setIOManifestCacheMaxTotalBytes(long bytes);

    public abstract Builder setIOManifestCacheMaxContentLength(long length);

    public abstract Builder setUri(@Nullable String uri);

    public abstract Builder setClientPoolSize(int size);

    public abstract Builder setClientPoolEvictionIntervalMs(long interval);

    public abstract Builder setClientPoolCacheKeys(@Nullable String keys);

    public abstract Builder setLockImplementation(@Nullable String lockImplementation);

    public abstract Builder setLockHeartbeatIntervalMillis(long interval);

    public abstract Builder setLockHeartbeatTimeoutMillis(long timeout);

    public abstract Builder setLockHeartbeatThreads(int threads);

    public abstract Builder setLockAcquireIntervalMillis(long interval);

    public abstract Builder setLockAcquireTimeoutMillis(long timeout);

    public abstract Builder setAppIdentifier(@Nullable String id);

    public abstract Builder setUser(@Nullable String user);

    public abstract Builder setAuthSessionTimeoutMillis(long timeout);

    public abstract Builder setConfiguration(@Nullable Configuration conf);

    public abstract IcebergCatalogConfig build();
  }
}

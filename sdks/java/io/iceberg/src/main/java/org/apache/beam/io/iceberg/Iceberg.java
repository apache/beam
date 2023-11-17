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
package org.apache.beam.io.iceberg;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.io.iceberg.util.PropertyBuilder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;

@SuppressWarnings("all") // TODO: Remove this once development is stable.
public class Iceberg {

  public static String DEFAULT_CATALOG_NAME = "default";

  public enum ScanType {
    TABLE,
    BATCH
  }

  public enum WriteFormat {
    AVRO,
    PARQUET,
    ORC
  }

  public static Catalog catalog(String name) {
    return Catalog.builder().name(name).build();
  }

  public static Catalog catalog() {
    return catalog(DEFAULT_CATALOG_NAME);
  }

  @AutoValue
  public abstract static class Scan implements Serializable {

    public abstract ScanType getType();

    public abstract Catalog getCatalog();

    public abstract ImmutableList<String> getTable();

    public abstract Schema getSchema();

    public abstract @Nullable Expression getFilter();

    public abstract @Nullable Boolean getCaseSensitive();

    public abstract ImmutableMap<String, String> getOptions();

    public abstract @Nullable Long getSnapshot();

    public abstract @Nullable Long getTimestamp();

    public abstract @Nullable Long getFromSnapshotInclusive();

    public abstract @Nullable String getFromSnapshotRefInclusive();

    public abstract @Nullable Long getFromSnapshotExclusive();

    public abstract @Nullable String getFromSnapshotRefExclusive();

    public abstract @Nullable Long getToSnapshot();

    public abstract @Nullable String getToSnapshotRef();

    public abstract @Nullable String getTag();

    public abstract @Nullable String getBranch();

    public static Scan.Builder builder() {
      return new AutoValue_Iceberg_Scan.Builder()
          .type(ScanType.TABLE)
          .filter(null)
          .caseSensitive(null)
          .options(ImmutableMap.of())
          .snapshot(null)
          .timestamp(null)
          .fromSnapshotInclusive(null)
          .fromSnapshotRefInclusive(null)
          .fromSnapshotExclusive(null)
          .fromSnapshotRefExclusive(null)
          .toSnapshot(null)
          .toSnapshotRef(null)
          .tag(null)
          .branch(null);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder type(ScanType type);

      public abstract Builder catalog(Catalog catalog);

      public abstract Builder table(ImmutableList<String> table);

      public Builder table(String... table) {
        return table(ImmutableList.copyOf(table));
      }

      public abstract Builder schema(Schema schema);

      public abstract Builder filter(@Nullable Expression filter);

      public abstract Builder caseSensitive(@Nullable Boolean caseSensitive);

      public abstract Builder options(ImmutableMap<String, String> options);

      public abstract Builder snapshot(@Nullable Long snapshot);

      public abstract Builder timestamp(@Nullable Long timestamp);

      public abstract Builder fromSnapshotInclusive(@Nullable Long fromInclusive);

      public abstract Builder fromSnapshotRefInclusive(@Nullable String ref);

      public abstract Builder fromSnapshotExclusive(@Nullable Long fromExclusive);

      public abstract Builder fromSnapshotRefExclusive(@Nullable String ref);

      public abstract Builder toSnapshot(@Nullable Long snapshot);

      public abstract Builder toSnapshotRef(@Nullable String ref);

      public abstract Builder tag(@Nullable String tag);

      public abstract Builder branch(@Nullable String branch);

      public abstract Scan build();
    }
  }

  @AutoValue
  public abstract static class Catalog implements Serializable {

    public abstract String getName();

    /* Core Properties */
    public abstract @Nullable String getIcebergCatalogType();

    public abstract @Nullable String getCatalogImplementation();

    public abstract @Nullable String getFileIOImplementation();

    public abstract @Nullable String getWarehouseLocation();

    public abstract @Nullable String getMetricsReporterImplementation();

    /* Caching */
    public abstract boolean getCacheEnabled();

    public abstract boolean getCacheCaseSensitive();

    public abstract long getCacheExpirationIntervalMillis();

    public abstract boolean getIOManifestCacheEnabled();

    public abstract long getIOManifestCacheExpirationIntervalMillis();

    public abstract long getIOManifestCacheMaxTotalBytes();

    public abstract long getIOManifestCacheMaxContentLength();

    public abstract @Nullable String getUri();

    public abstract int getClientPoolSize();

    public abstract long getClientPoolEvictionIntervalMs();

    public abstract @Nullable String getClientPoolCacheKeys();

    public abstract @Nullable String getLockImplementation();

    public abstract long getLockHeartbeatIntervalMillis();

    public abstract long getLockHeartbeatTimeoutMillis();

    public abstract int getLockHeartbeatThreads();

    public abstract long getLockAcquireIntervalMillis();

    public abstract long getLockAcquireTimeoutMillis();

    public abstract @Nullable String getAppIdentifier();

    public abstract @Nullable String getUser();

    public abstract long getAuthSessionTimeoutMillis();

    public abstract @Nullable Configuration getConfiguration();

    public static Catalog.Builder builder() {
      return new AutoValue_Iceberg_Catalog.Builder()
          .icebergCatalogType(null)
          .catalogImplementation(null)
          .fileIOImplementation(null)
          .warehouseLocation(null)
          .metricsReporterImplementation(null) // TODO: Set this to our implementation
          .cacheEnabled(CatalogProperties.CACHE_ENABLED_DEFAULT)
          .cacheCaseSensitive(CatalogProperties.CACHE_CASE_SENSITIVE_DEFAULT)
          .cacheExpirationIntervalMillis(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
          .iOManifestCacheEnabled(CatalogProperties.IO_MANIFEST_CACHE_ENABLED_DEFAULT)
          .iOManifestCacheExpirationIntervalMillis(
              CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_DEFAULT)
          .iOManifestCacheMaxTotalBytes(CatalogProperties.IO_MANIFEST_CACHE_MAX_TOTAL_BYTES_DEFAULT)
          .iOManifestCacheMaxContentLength(
              CatalogProperties.IO_MANIFEST_CACHE_MAX_CONTENT_LENGTH_DEFAULT)
          .uri(null)
          .clientPoolSize(CatalogProperties.CLIENT_POOL_SIZE_DEFAULT)
          .clientPoolEvictionIntervalMs(
              CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS_DEFAULT)
          .clientPoolCacheKeys(null)
          .lockImplementation(null)
          .lockHeartbeatIntervalMillis(CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT)
          .lockHeartbeatTimeoutMillis(CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT)
          .lockHeartbeatThreads(CatalogProperties.LOCK_HEARTBEAT_THREADS_DEFAULT)
          .lockAcquireIntervalMillis(CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS_DEFAULT)
          .lockAcquireTimeoutMillis(CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT)
          .appIdentifier(null)
          .user(null)
          .authSessionTimeoutMillis(CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT)
          .configuration(null);
    }

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

    public Table.Builder table() {
      return new AutoValue_Iceberg_Table.Builder().catalog(this);
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder name(String name);

      /* Core Properties */
      public abstract Builder icebergCatalogType(@Nullable String icebergType);

      public abstract Builder catalogImplementation(@Nullable String catalogImpl);

      public abstract Builder fileIOImplementation(@Nullable String fileIOImpl);

      public abstract Builder warehouseLocation(@Nullable String warehouse);

      public abstract Builder metricsReporterImplementation(@Nullable String metricsImpl);

      /* Caching */
      public abstract Builder cacheEnabled(boolean cacheEnabled);

      public abstract Builder cacheCaseSensitive(boolean cacheCaseSensitive);

      public abstract Builder cacheExpirationIntervalMillis(long expiration);

      public abstract Builder iOManifestCacheEnabled(boolean enabled);

      public abstract Builder iOManifestCacheExpirationIntervalMillis(long expiration);

      public abstract Builder iOManifestCacheMaxTotalBytes(long bytes);

      public abstract Builder iOManifestCacheMaxContentLength(long length);

      public abstract Builder uri(@Nullable String uri);

      public abstract Builder clientPoolSize(int size);

      public abstract Builder clientPoolEvictionIntervalMs(long interval);

      public abstract Builder clientPoolCacheKeys(@Nullable String keys);

      public abstract Builder lockImplementation(@Nullable String lockImplementation);

      public abstract Builder lockHeartbeatIntervalMillis(long interval);

      public abstract Builder lockHeartbeatTimeoutMillis(long timeout);

      public abstract Builder lockHeartbeatThreads(int threads);

      public abstract Builder lockAcquireIntervalMillis(long interval);

      public abstract Builder lockAcquireTimeoutMillis(long timeout);

      public abstract Builder appIdentifier(@Nullable String id);

      public abstract Builder user(@Nullable String user);

      public abstract Builder authSessionTimeoutMillis(long timeout);

      public abstract Builder configuration(@Nullable Configuration conf);

      public abstract Catalog build();

      public Builder withProperties(Map<String, Object> properties) {
        return this;
      }
    }
  }

  @AutoValue
  public abstract static class Table implements Serializable {

    public abstract @Nullable Catalog catalog();

    public abstract @Nullable List<String> tablePath();

    public TableIdentifier identifier() {
      return TableIdentifier.of(tablePath().toArray(new String[0]));
    }

    public org.apache.iceberg.Table table() {
      return catalog().catalog().loadTable(identifier());
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder catalog(Catalog catalog);

      public abstract Builder tablePath(List<String> tablePath);

      public abstract Table build();
    }
  }

  public static class Write<ElementT>
      extends PTransform<PCollection<ElementT>, IcebergWriteResult> {

    private final DynamicDestinations<ElementT, String> dynamicDestinations;
    private final Catalog catalog;

    private final SerializableBiFunction<Record, ElementT, Record> toRecord;

    public Write(
        Catalog catalog,
        DynamicDestinations<ElementT, String> dynamicDestinations,
        SerializableBiFunction<Record, ElementT, Record> toRecord) {
      this.catalog = catalog;
      this.dynamicDestinations = dynamicDestinations;
      this.toRecord = toRecord;
    }

    @Override
    public IcebergWriteResult expand(PCollection<ElementT> input) {
      try {
        return input
            .apply(
                "Set Output Location",
                new PrepareWrite<ElementT, String, ElementT>(
                    dynamicDestinations, SerializableFunctions.identity(), input.getCoder()))
            .apply(
                "Dynamic Write",
                new IcebergSink<String, ElementT>(
                    dynamicDestinations,
                    dynamicDestinations.getDestinationCoderWithDefault(
                        input.getPipeline().getCoderRegistry()),
                    RecordWriterFactory.tableRecords(toRecord, dynamicDestinations),
                    TableFactory.forCatalog(catalog)));
      } catch (Exception e) {
        RuntimeException e1 = new RuntimeException("Unable to expand transforms");
        e1.addSuppressed(e);
        throw e1;
      }
    }
  }
}

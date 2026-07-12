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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Process-wide cache for Iceberg {@link Table}s.
 *
 * <p>Entries are keyed by catalog configuration and table identifier, so one machine can share
 * table metadata across source and sink threads without colliding when different catalogs contain
 * the same identifier. The underlying catalog is only resolved from {@link IcebergCatalogConfig}
 * when the table has to be loaded. Refreshes are synchronized per table entry: if another thread
 * refreshed after a caller started its request, the caller reuses that refresh instead of making
 * another catalog call.
 */
@Internal
public class TableCache {
  static final Duration DEFAULT_REFRESH_INTERVAL = Duration.ofMinutes(2);

  private static final Cache<CacheKey, CachedTable> TABLES =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();

  /** Returns the cached table, loading it from the catalog on a cache miss. */
  public static Table get(IcebergCatalogConfig catalogConfig, TableIdentifier identifier) {
    return get(catalogConfig, identifier, () -> catalogConfig.catalog().loadTable(identifier));
  }

  /** Returns the cached table for a string identifier, loading it on a cache miss. */
  public static Table get(IcebergCatalogConfig catalogConfig, String identifier) {
    return get(catalogConfig, TableIdentifier.parse(identifier));
  }

  /** Returns the cached table, using the given loader only on a cache miss. */
  public static Table get(
      IcebergCatalogConfig catalogConfig, TableIdentifier identifier, Callable<Table> loader) {
    return getEntry(catalogConfig, identifier, loader).table;
  }

  /** Returns the cached table after forcing a refresh of any pre-existing cache entry. */
  public static Table getRefreshed(IcebergCatalogConfig catalogConfig, TableIdentifier identifier) {
    Instant refreshRequestTime = Instant.now();
    CachedTable cachedTable =
        getEntry(catalogConfig, identifier, () -> catalogConfig.catalog().loadTable(identifier));
    cachedTable.refreshIfOlderThan(refreshRequestTime);
    return cachedTable.table;
  }

  /** Returns the cached table for a string identifier after refreshing any pre-existing entry. */
  public static Table getRefreshed(IcebergCatalogConfig catalogConfig, String identifier) {
    return getRefreshed(catalogConfig, TableIdentifier.parse(identifier));
  }

  public static Table getAndRefreshIfStale(IcebergCatalogConfig catalogConfig, String identifier) {
    return getAndRefreshIfStale(catalogConfig, TableIdentifier.parse(identifier));
  }

  /**
   * Returns the cached table, refreshing it only if it is older than {@link
   * #DEFAULT_REFRESH_INTERVAL}.
   */
  public static Table getAndRefreshIfStale(
      IcebergCatalogConfig catalogConfig, TableIdentifier identifier) {
    return getAndRefreshIfStale(
        catalogConfig, identifier, () -> catalogConfig.catalog().loadTable(identifier));
  }

  /** Returns the cached table, using the loader on a miss and refreshing stale entries. */
  public static Table getAndRefreshIfStale(
      IcebergCatalogConfig catalogConfig, TableIdentifier identifier, Callable<Table> loader) {
    CachedTable cachedTable = getEntry(catalogConfig, identifier, loader);
    cachedTable.refreshIfOlderThan(Instant.now().minus(DEFAULT_REFRESH_INTERVAL));
    return cachedTable.table;
  }

  private static CachedTable getEntry(
      IcebergCatalogConfig catalogConfig, TableIdentifier identifier, Callable<Table> loader) {
    CacheKey key = new CacheKey(catalogConfig, identifier);
    try {
      return TABLES.get(key, () -> new CachedTable(loader.call(), Instant.now()));
    } catch (ExecutionException | UncheckedExecutionException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(
          "Encountered a problem fetching table " + identifier + " from cache.", e);
    }
  }

  @VisibleForTesting
  static long size() {
    return TABLES.size();
  }

  @VisibleForTesting
  static void invalidateAll() {
    TABLES.invalidateAll();
  }

  @VisibleForTesting
  static void put(
      IcebergCatalogConfig catalogConfig,
      TableIdentifier identifier,
      Table table,
      Instant lastRefreshTime) {
    TABLES.put(new CacheKey(catalogConfig, identifier), new CachedTable(table, lastRefreshTime));
  }

  @VisibleForTesting
  static void markStale(IcebergCatalogConfig catalogConfig, TableIdentifier identifier) {
    CachedTable cachedTable = TABLES.getIfPresent(new CacheKey(catalogConfig, identifier));
    if (cachedTable != null) {
      cachedTable.lastRefreshTime = Instant.EPOCH;
    }
  }

  private static class CachedTable {
    private final Table table;
    private volatile Instant lastRefreshTime;

    private CachedTable(Table table, Instant lastRefreshTime) {
      this.table = table;
      this.lastRefreshTime = lastRefreshTime;
    }

    private void refreshIfOlderThan(Instant refreshRequestTime) {
      if (lastRefreshTime.isAfter(refreshRequestTime)) {
        return;
      }
      synchronized (this) {
        if (lastRefreshTime.isBefore(refreshRequestTime)) {
          table.refresh();
          lastRefreshTime = Instant.now();
        }
      }
    }
  }

  private static class CacheKey {
    private final IcebergCatalogConfig catalogConfig;
    private final TableIdentifier identifier;

    private CacheKey(IcebergCatalogConfig catalogConfig, TableIdentifier identifier) {
      this.catalogConfig = catalogConfig;
      this.identifier = identifier;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof CacheKey)) {
        return false;
      }
      CacheKey other = (CacheKey) obj;
      return catalogConfig.equals(other.catalogConfig) && identifier.equals(other.identifier);
    }

    @Override
    public int hashCode() {
      return 31 * catalogConfig.hashCode() + identifier.hashCode();
    }
  }
}

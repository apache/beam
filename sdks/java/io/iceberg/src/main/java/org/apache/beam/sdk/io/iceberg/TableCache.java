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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.Futures;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ListenableFuture;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.jetbrains.annotations.NotNull;

/** Utility to fetch and cache Iceberg {@link Table}s. */
class TableCache {
  private static final Map<String, Catalog> CATALOG_CACHE = new ConcurrentHashMap<>();
  private static final LoadingCache<String, Table> INTERNAL_CACHE =
      CacheBuilder.newBuilder()
          .expireAfterAccess(1, TimeUnit.HOURS)
          .refreshAfterWrite(3, TimeUnit.MINUTES)
          .build(
              new CacheLoader<String, Table>() {
                @Override
                public @NotNull Table load(@NotNull String identifier) {
                  return checkStateNotNull(CATALOG_CACHE.get(identifier))
                      .loadTable(TableIdentifier.parse(identifier));
                }

                @Override
                public @NotNull ListenableFuture<Table> reload(
                    @NotNull String unusedIdentifier, @NotNull Table table) {
                  table.refresh();
                  return Futures.immediateFuture(table);
                }
              });;

  static Table get(String identifier) {
    checkStateNotNull(INTERNAL_CACHE, "Please call TableCache.setup() first.");
    try {
      return INTERNAL_CACHE.get(identifier);
    } catch (ExecutionException e) {
      throw new RuntimeException(
          "Encountered a problem fetching table " + identifier + " from cache.", e);
    }
  }

  /** Forces a table refresh and returns. */
  static Table getRefreshed(String identifier) {
    checkStateNotNull(INTERNAL_CACHE, "Please call TableCache.setup() first.");
    INTERNAL_CACHE.refresh(identifier);
    return get(identifier);
  }

  static void setup(IcebergScanConfig scanConfig) {
    CATALOG_CACHE.putIfAbsent(
        scanConfig.getTableIdentifier(), scanConfig.getCatalogConfig().catalog());
  }
}

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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Utility to fetch and cache Iceberg {@link Table}s. */
class TableCache {
  private static final Cache<TableIdentifier, Table> CACHE =
      CacheBuilder.newBuilder().expireAfterWrite(3, TimeUnit.MINUTES).build();

  static Table get(TableIdentifier identifier, Catalog catalog) {
    try {
      return CACHE.get(identifier, () -> catalog.loadTable(identifier));
    } catch (ExecutionException e) {
      throw new RuntimeException(
          "Encountered a problem fetching table " + identifier + " from cache.", e);
    }
  }

  static Table get(String identifier, Catalog catalog) {
    return get(TableIdentifier.parse(identifier), catalog);
  }

  static Table getRefreshed(TableIdentifier identifier, Catalog catalog) {
    @Nullable Table table = CACHE.getIfPresent(identifier);
    if (table == null) {
      return get(identifier, catalog);
    }
    table.refresh();
    CACHE.put(identifier, table);
    return table;
  }

  static Table getRefreshed(String identifier, Catalog catalog) {
    return getRefreshed(TableIdentifier.parse(identifier), catalog);
  }
}

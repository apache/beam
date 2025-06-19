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
package org.apache.beam.sdk.extensions.sql.meta.catalog;

import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamCalciteSchema;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Top-level authority that manages {@link Catalog}s. Used inside the root {@link
 * BeamCalciteSchema}.
 *
 * <p>Implementations should have a way of determining which catalog is currently active, and
 * produce it when {@link #currentCatalog()} is invoked.
 *
 * <p>When {@link #registerTableProvider(String, TableProvider)} is called, the provider should
 * become available for all catalogs.
 */
@Internal
public interface CatalogManager {
  /** Creates and stores a catalog of a particular type. */
  void createCatalog(String name, String type, Map<String, String> properties);

  /** Switches the active catalog. */
  void useCatalog(String name);

  /** Produces the currently active catalog. */
  Catalog currentCatalog();

  /** Attempts to fetch the catalog with this name. May produce null if it does not exist. */
  @Nullable
  Catalog getCatalog(String name);

  /** Drops the catalog with this name. No-op if the catalog already does not exist. */
  void dropCatalog(String name);

  /**
   * Registers a {@link TableProvider} and propagates it to all the {@link Catalog} instances
   * available to this manager.
   */
  void registerTableProvider(String name, TableProvider tableProvider);

  default void registerTableProvider(TableProvider tp) {
    registerTableProvider(tp.getTableType(), tp);
  }
}

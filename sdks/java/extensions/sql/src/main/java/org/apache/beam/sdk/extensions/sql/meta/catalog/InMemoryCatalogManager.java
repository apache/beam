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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

public class InMemoryCatalogManager implements CatalogManager {
  private final Map<String, Catalog> catalogs = new HashMap<>();
  private final Map<String, TableProvider> tableProviderMap = new HashMap<>();
  private String currentCatalogName;

  public InMemoryCatalogManager() {
    this.catalogs.put("default", new InMemoryCatalog("default", Collections.emptyMap()));
    this.currentCatalogName = "default";
  }

  @Override
  public void createCatalog(String name, String type, Map<String, String> properties) {
    Preconditions.checkState(
        !catalogs.containsKey(name), "Catalog with name '%s' already exists.", name);
    for (Catalog catalog : ServiceLoader.load(Catalog.class, getClass().getClassLoader())) {
      if (catalog.type().equalsIgnoreCase(type)) {
        catalog.initialize(name, properties);
        tableProviderMap.values().forEach(catalog.metaStore()::registerProvider);
        catalogs.put(name, catalog);
        return;
      }
    }

    throw new UnsupportedOperationException(
        String.format("Could not create catalog '%s' of unknown type: '%s'", name, type));
  }

  @Override
  public void useCatalog(String name) {
    if (!catalogs.containsKey(name)) {
      throw new IllegalArgumentException("Catalog not found: " + name);
    }
    this.currentCatalogName = name;
  }

  @Override
  public Catalog currentCatalog() {
    return checkStateNotNull(catalogs.get(currentCatalogName));
  }

  @Override
  public boolean catalogExists(String name) {
    return catalogs.containsKey(name);
  }

  @Override
  public void removeCatalog(String name) {
    catalogs.remove(name);
  }

  @Override
  public void registerTableProvider(String name, TableProvider tableProvider) {
    tableProviderMap.put(name, tableProvider);
    catalogs.values().forEach(catalog -> catalog.metaStore().registerProvider(tableProvider));
  }

  @VisibleForTesting
  public Catalog getCatalog(String name) {
    return checkStateNotNull(catalogs.get(name));
  }
}

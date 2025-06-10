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
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;

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

    Catalog catalog = findAndCreateCatalog(name, type, properties);
    tableProviderMap.values().forEach(catalog.metaStore()::registerProvider);
    catalogs.put(name, catalog);
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
  public @Nullable Catalog getCatalog(String name) {
    return catalogs.get(name);
  }

  @Override
  public void dropCatalog(String name) {
    catalogs.remove(name);
  }

  @Override
  public void registerTableProvider(String name, TableProvider tableProvider) {
    tableProviderMap.put(name, tableProvider);
    catalogs.values().forEach(catalog -> catalog.metaStore().registerProvider(tableProvider));
  }

  private Catalog findAndCreateCatalog(String name, String type, Map<String, String> properties) {
    ImmutableList.Builder<Catalog> list = ImmutableList.builder();
    for (CatalogRegistrar catalogRegistrar :
        ServiceLoader.load(CatalogRegistrar.class, getClass().getClassLoader())) {
      for (Class<? extends Catalog> catalogClass : catalogRegistrar.getCatalogs()) {
        Catalog catalog = createCatalogInstance(catalogClass, name, properties);
        if (catalog.type().equalsIgnoreCase(type)) {
          list.add(catalog);
        }
      }
    }

    List<Catalog> foundCatalogs = list.build();

    if (foundCatalogs.size() > 1) {
      throw new IllegalStateException(
          String.format(
              "Could not create catalog '%s': "
                  + "expected only one implementation for type '%s' but found %s",
              name, type, foundCatalogs.size()));
    } else if (foundCatalogs.isEmpty()) {
      throw new UnsupportedOperationException(
          String.format("Could not find type '%s' for catalog '%s'.", type, name));
    }

    return foundCatalogs.get(0);
  }

  private Catalog createCatalogInstance(
      Class<? extends Catalog> catalogClass, String name, Map<String, String> properties) {
    try {
      return catalogClass.getConstructor(String.class, Map.class).newInstance(name, properties);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(
          String.format("Encountered an error when constructing Catalog '%s'", name), e);
    }
  }
}

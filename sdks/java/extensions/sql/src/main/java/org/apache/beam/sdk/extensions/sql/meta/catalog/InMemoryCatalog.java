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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.apache.beam.sdk.util.Preconditions;
import org.checkerframework.checker.nullness.qual.Nullable;

public class InMemoryCatalog implements Catalog {
  private final String name;
  private final Map<String, String> properties;
  private final InMemoryMetaStore metaStore = new InMemoryMetaStore();
  private final HashSet<String> databases = new HashSet<>(Collections.singleton(DEFAULT));
  protected @Nullable String currentDatabase = DEFAULT;

  public InMemoryCatalog(String name, Map<String, String> properties) {
    this.name = name;
    this.properties = properties;
  }

  @Override
  public String type() {
    return "local";
  }

  @Override
  public String name() {
    return Preconditions.checkStateNotNull(
        name, getClass().getSimpleName() + " has not been initialized");
  }

  @Override
  public MetaStore metaStore() {
    return metaStore;
  }

  @Override
  public Map<String, String> properties() {
    return Preconditions.checkStateNotNull(properties, "InMemoryCatalog has not been initialized");
  }

  @Override
  public boolean createDatabase(String database) {
    return databases.add(database);
  }

  @Override
  public void useDatabase(String database) {
    checkArgument(listDatabases().contains(database), "Database '%s' does not exist.");
    currentDatabase = database;
  }

  @Override
  public @Nullable String currentDatabase() {
    return currentDatabase;
  }

  @Override
  public boolean dropDatabase(String database, boolean cascade) {
    checkState(!cascade, getClass().getSimpleName() + " does not support CASCADE.");

    boolean removed = databases.remove(database);
    if (database.equals(currentDatabase)) {
      currentDatabase = null;
    }
    return removed;
  }

  @Override
  public Set<String> listDatabases() {
    return databases;
  }
}

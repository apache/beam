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
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.MetaStore;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Represents a named and configurable container for managing tables. Is defined with a type and
 * configuration properties. Uses an underlying {@link MetaStore} to manage tables and table
 * providers.
 */
@Internal
public interface Catalog {
  // Default database name
  String DEFAULT = "default";

  /** A type that defines this catalog. */
  String type();

  /**
   * Returns the underlying {@link MetaStore} for this database. Creates a new {@link MetaStore} if
   * one does not exist yet.
   */
  MetaStore metaStore(String database);

  /**
   * Produces the currently active database. Can be null if no database is active.
   *
   * @return the current active database
   */
  @Nullable
  String currentDatabase();

  /**
   * Creates a database with this name.
   *
   * @param databaseName
   * @return true if the database was created, false otherwise.
   */
  boolean createDatabase(String databaseName);

  /** Returns true if the database exists. */
  boolean databaseExists(String db);

  /**
   * Switches to use the specified database.
   *
   * @param databaseName
   */
  void useDatabase(String databaseName);

  /**
   * Drops the database with this name. If cascade is true, the catalog should first drop all tables
   * contained in this database.
   *
   * @param databaseName
   * @param cascade
   * @return true if the database was dropped, false otherwise.
   */
  boolean dropDatabase(String databaseName, boolean cascade);

  /** The name of this catalog, specified by the user. */
  String name();

  /** User-specified configuration properties. */
  Map<String, String> properties();

  /** Registers this {@link TableProvider} and propagates it to underlying {@link MetaStore}s. */
  void registerTableProvider(TableProvider provider);

  /**
   * Returns all the {@link TableProvider}s available to this {@link Catalog}, organized by type.
   */
  Map<String, TableProvider> tableProviders();
}

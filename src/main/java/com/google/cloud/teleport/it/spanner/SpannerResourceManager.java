/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableList;

/** Interface for managing spanner resources in integration tests. */
public interface SpannerResourceManager {

  /**
   * Creates a table given a CREATE TABLE DDL statement.
   *
   * <p>Note: Implementations may do instance creation and database creation here.
   *
   * @param statement The CREATE TABLE DDL statement.
   * @throws IllegalStateException if method is called after resources have been cleaned up.
   */
  void createTable(String statement) throws IllegalStateException;

  /**
   * Writes a given record into a table. This method requires {@link
   * SpannerResourceManager#createTable(String)} to be called for the target table beforehand.
   *
   * @param tableRecord A mutation object representing the table record.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  void write(Mutation tableRecord) throws IllegalStateException;

  /**
   * Writes a collection of table records into one or more tables. This method requires {@link
   * SpannerResourceManager#createTable(String)} to be called for the target table beforehand.
   *
   * @param tableRecords A collection of mutation objects representing table records.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  void write(Iterable<Mutation> tableRecords) throws IllegalStateException;

  /**
   * Reads all the rows in a table. This method requires {@link
   * SpannerResourceManager#createTable(String)} to be called for the target table beforehand.
   *
   * @param tableId The id of the table to read rows from.
   * @param columnNames The table's column names.
   * @return A ResultSet object containing all the rows in the table.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  ImmutableList<Struct> readTableRecords(String tableId, String... columnNames)
      throws IllegalStateException;

  /**
   * Reads all the rows in a table.This method requires {@link
   * SpannerResourceManager#createTable(String)} to be called for the target table beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @param columnNames A collection of the table's column names.
   * @return A ResultSet object containing all the rows in the table.
   * @throws IllegalStateException if method is called after resources have been cleaned up or if
   *     the manager object has no instance or database.
   */
  ImmutableList<Struct> readTableRecords(String tableId, Iterable<String> columnNames)
      throws IllegalStateException;

  /**
   * Deletes all created resources (instance, database, and tables) and cleans up all Spanner
   * sessions, making the manager object unusable.
   */
  void cleanupAll();
}

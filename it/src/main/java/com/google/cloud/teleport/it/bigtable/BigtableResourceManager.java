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
package com.google.cloud.teleport.it.bigtable;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;
import org.threeten.bp.Duration;

/** Interface for managing bigtable resources in integration tests. */
public interface BigtableResourceManager {

  /**
   * Creates a Bigtable instance in which all clusters, nodes and tables will exist.
   *
   * @param clusters Collection of BigtableResourceManagerCluster objects to associate with the
   *     given Bigtable instance.
   * @throws BigtableResourceManagerException if there is an error creating the instance in
   *     Bigtable.
   */
  void createInstance(Iterable<BigtableResourceManagerCluster> clusters);

  /**
   * Return the instance ID this Resource Manager uses to create and manage tables in.
   *
   * @return the instance ID.
   */
  String getInstanceId();

  /**
   * Returns the project ID this Resource Manager is configured to operate on.
   *
   * @return the project ID.
   */
  String getProjectId();

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>The columns in this table will be automatically garbage collected after one hour.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  void createTable(String tableId, Iterable<String> columnFamilies);

  /**
   * Creates a table within the current instance given a table ID and a collection of column family
   * names.
   *
   * <p>The columns in this table will be automatically garbage collected once they reach the age
   * specified by {@code maxAge}.
   *
   * <p>Note: Implementations may do instance creation here, if one does not already exist.
   *
   * @param tableId The id of the table.
   * @param columnFamilies A collection of column family names for the table.
   * @param maxAge Sets the maximum age the columns can persist before being garbage collected.
   * @throws BigtableResourceManagerException if there is an error creating the table in Bigtable.
   */
  void createTable(String tableId, Iterable<String> columnFamilies, Duration maxAge);

  /**
   * Writes a given row into a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRow A mutation object representing the table row.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  void write(RowMutation tableRow);

  /**
   * Writes a collection of table rows into one or more tables. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableRows A collection of mutation objects representing table rows.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  void write(Iterable<RowMutation> tableRows);

  /**
   * Reads all the rows in a table. This method requires {@link
   * BigtableResourceManager#createTable(String, Iterable)} to be called for the target table
   * beforehand.
   *
   * @param tableId The id of table to read rows from.
   * @return A List object containing all the rows in the table.
   * @throws BigtableResourceManagerException if method is called after resources have been cleaned
   *     up, if the manager object has no instance, if the table does not exist or if there is an
   *     IOException when attempting to retrieve the bigtable data client.
   */
  ImmutableList<Row> readTable(String tableId);

  /**
   * Deletes all created resources (instance and tables) and cleans up all Bigtable clients, making
   * the manager object unusable.
   *
   * @throws BigtableResourceManagerException if there is an error deleting the instance or tables
   *     in Bigtable.
   */
  void cleanupAll();
}

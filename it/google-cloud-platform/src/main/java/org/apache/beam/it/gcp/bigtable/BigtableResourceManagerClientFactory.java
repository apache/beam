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
package org.apache.beam.it.gcp.bigtable;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;

/**
 * Class for storing the 3 Bigtable clients used to manage the Instance(s), Table(s) and Data
 * mutations performed by the resource manager.
 *
 * <p>This class is intended to simplify mock injections of Bigtable clients during unit testing
 */
public class BigtableResourceManagerClientFactory {

  private final BigtableInstanceAdminSettings bigtableInstanceAdminSettings;
  private final BigtableTableAdminSettings bigtableTableAdminSettings;
  private final BigtableDataSettings bigtableDataSettings;

  public BigtableResourceManagerClientFactory(
      BigtableInstanceAdminSettings instanceSettings,
      BigtableTableAdminSettings tableSettings,
      BigtableDataSettings dataSettings) {

    this.bigtableInstanceAdminSettings = instanceSettings;
    this.bigtableTableAdminSettings = tableSettings;
    this.bigtableDataSettings = dataSettings;
  }

  /**
   * Returns the Instance admin client for managing Bigtable instances.
   *
   * @return the instance admin client
   */
  public BigtableInstanceAdminClient bigtableInstanceAdminClient() {
    try {
      return BigtableInstanceAdminClient.create(bigtableInstanceAdminSettings);
    } catch (IOException e) {
      throw new BigtableResourceManagerException(
          "Failed to create bigtable instance admin client.", e);
    }
  }

  /**
   * Returns the Table admin client for managing Bigtable tables.
   *
   * @return the table admin client
   */
  public BigtableTableAdminClient bigtableTableAdminClient() {
    try {
      return BigtableTableAdminClient.create(bigtableTableAdminSettings);
    } catch (IOException e) {
      throw new BigtableResourceManagerException(
          "Failed to create bigtable table admin client.", e);
    }
  }

  /**
   * Returns the Data client for reading and writing data to Bigtable tables.
   *
   * @return the data client
   */
  public BigtableDataClient bigtableDataClient() {
    try {
      return BigtableDataClient.create(bigtableDataSettings);
    } catch (IOException e) {
      throw new BigtableResourceManagerException("Failed to create bigtable data client.", e);
    }
  }
}

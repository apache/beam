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

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.admin.v2.models.StorageType;

/**
 * Class for storing the metadata of a Bigtable cluster object.
 *
 * <p>A cluster belongs to a single bigtable instance and represents the service in a given zone. A
 * cluster can have multiple nodes operating on the data. The cluster also has a storage type of
 * either SSD or HDD depending on the user's needs.
 */
@AutoValue
public abstract class BigtableResourceManagerCluster {

  public static BigtableResourceManagerCluster create(
      String clusterId, String zone, int numNodes, StorageType storageType) {
    return new AutoValue_BigtableResourceManagerCluster(clusterId, zone, numNodes, storageType);
  }

  /**
   * Returns the cluster ID of the Bigtable cluster object.
   *
   * @return the ID of the Bigtable cluster.
   */
  public abstract String clusterId();

  /**
   * Returns the operating zone of the Bigtable cluster object.
   *
   * @return the zone of the Bigtable cluster.
   */
  public abstract String zone();

  /**
   * Returns the number of nodes the Bigtable cluster object should be configured with.
   *
   * @return the number of nodes for the Bigtable cluster.
   */
  public abstract int numNodes();

  /**
   * Returns the type of storage the Bigtable cluster object should be configured with (SSD or HDD).
   *
   * @return the storage type of the Bigtable cluster.
   */
  public abstract StorageType storageType();
}

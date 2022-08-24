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

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link com.google.cloud.teleport.it.bigtable.BigtableResourceManagerCluster}. */
@RunWith(JUnit4.class)
public class BigtableResourceManagerClusterTest {

  private static final String CLUSTER_ID = "cluster-id";
  private static final String CLUSTER_ZONE = "us-central1-a";
  private static final int CLUSTER_NUM_NODES = 1;
  private static final StorageType CLUSTER_STORAGE_TYPE = StorageType.SSD;

  @Test
  public void testBigtableResourceManagerClusterBuilderSetsCorrectValues() {
    BigtableResourceManagerCluster cluster =
        BigtableResourceManagerCluster.create(
            CLUSTER_ID, CLUSTER_ZONE, CLUSTER_NUM_NODES, CLUSTER_STORAGE_TYPE);

    assertThat(cluster.clusterId()).isEqualTo(CLUSTER_ID);
    assertThat(cluster.zone()).isEqualTo(CLUSTER_ZONE);
    assertThat(cluster.numNodes()).isEqualTo(CLUSTER_NUM_NODES);
    assertThat(cluster.storageType()).isEqualTo(CLUSTER_STORAGE_TYPE);
  }
}

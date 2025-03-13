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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.checkValidTableId;
import static org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils.generateDefaultClusters;
import static org.junit.Assert.assertThrows;

import com.google.cloud.bigtable.admin.v2.models.StorageType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigtableResourceManagerUtils}. */
@RunWith(JUnit4.class)
public class BigtableResourceManagerUtilsTest {
  private static final String TEST_ID = "test-id";
  private static final String ZONE = "us-central1-a";
  private static final int NUM_NODES = 1;
  private static final StorageType STORAGE_TYPE = StorageType.SSD;

  @Test
  public void testGenerateDefaultClustersShouldWorkWhenAllParametersValid() {
    Iterable<BigtableResourceManagerCluster> cluster =
        generateDefaultClusters(TEST_ID, ZONE, NUM_NODES, STORAGE_TYPE);
    BigtableResourceManagerCluster thisCluster = cluster.iterator().next();

    assertThat(thisCluster.clusterId()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
    assertThat(thisCluster.zone()).isEqualTo(ZONE);
    assertThat(thisCluster.numNodes()).isEqualTo(NUM_NODES);
    assertThat(thisCluster.storageType()).isEqualTo(STORAGE_TYPE);
  }

  @Test
  public void testGenerateDefaultClustersShouldThrowErrorWhenTestIdIsEmpty() {
    assertThrows(
        IllegalArgumentException.class,
        () -> generateDefaultClusters("", ZONE, NUM_NODES, STORAGE_TYPE));
  }

  @Test
  public void testGenerateDefaultClustersShouldShortenTestIdWhenTooLong() {
    Iterable<BigtableResourceManagerCluster> cluster =
        generateDefaultClusters("longer-id", ZONE, NUM_NODES, STORAGE_TYPE);
    assertThat(cluster.iterator().next().clusterId()).matches("longer--\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testCheckValidTableIdWhenIdIsTooShort() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId(""));
  }

  @Test
  public void testCheckValidTableIdWhenIdIsTooLong() {
    assertThrows(
        IllegalArgumentException.class,
        () -> checkValidTableId("really-really-really-really-long-table-id"));
  }

  @Test
  public void testCheckValidTableIdWhenIdContainsIllegalCharacter() {
    assertThrows(IllegalArgumentException.class, () -> checkValidTableId("table-id%"));
  }

  @Test
  public void testCheckValidTableIdWhenIdIsValid() {
    checkValidTableId("table-id_valid.Test1");
  }
}

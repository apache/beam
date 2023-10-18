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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.admin.v2.models.Table.ReplicationState;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.verification.Times;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link BigtableResourceManager}. */
@RunWith(JUnit4.class)
public class BigtableResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  private static final String TEST_ID = "test-id";
  private static final String TABLE_ID = "table-id";
  private static final String PROJECT_ID = "test-project";

  private static final String CLUSTER_ID = "cluster-id";
  private static final String CLUSTER_ZONE = "us-central1-a";
  private static final int CLUSTER_NUM_NODES = 1;
  private static final StorageType CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private BigtableResourceManager testManager;
  private List<BigtableResourceManagerCluster> cluster;

  @Before
  public void setUp() throws IOException {
    testManager =
        new BigtableResourceManager(
            BigtableResourceManager.builder(TEST_ID, PROJECT_ID, null),
            bigtableResourceManagerClientFactory);
    cluster =
        ImmutableList.of(
            BigtableResourceManagerCluster.create(
                CLUSTER_ID, CLUSTER_ZONE, CLUSTER_NUM_NODES, CLUSTER_STORAGE_TYPE));
  }

  @Test
  public void testCreateResourceManagerCreatesCorrectIdValues() throws IOException {
    BigtableResourceManager rm =
        new BigtableResourceManager(
            BigtableResourceManager.builder(TEST_ID, PROJECT_ID, null),
            bigtableResourceManagerClientFactory);

    assertThat(rm.getInstanceId()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
    assertThat(rm.getProjectId()).matches(PROJECT_ID);
  }

  @Test
  public void testCreateInstanceShouldThrowExceptionWhenClientFailsToCreateInstance() {
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient().createInstance(any()))
        .thenThrow(IllegalStateException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldThrowErrorWhenInstanceAdminClientFailsToClose() {
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldWorkWhenBigtableDoesNotThrowAnyError() {
    testManager.createInstance(cluster);

    verify(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .createInstance(any());
  }

  @Test
  public void testCreateTableShouldNotCreateInstanceWhenInstanceAlreadyExists() {
    setupReadyTable();

    testManager.createInstance(cluster);
    Mockito.lenient()
        .when(
            bigtableResourceManagerClientFactory
                .bigtableInstanceAdminClient()
                .createInstance(any()))
        .thenThrow(IllegalStateException.class);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(false);
    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));
  }

  @Test
  public void testCreateTableShouldCreateInstanceWhenInstanceDoesNotExist() {
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient().createInstance(any()))
        .thenThrow(IllegalStateException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenNoColumnFamilyGiven() {
    assertThrows(
        IllegalArgumentException.class, () -> testManager.createTable(TABLE_ID, new ArrayList<>()));
  }

  @Test
  public void testCreateTableShouldNotCreateTableWhenTableAlreadyExists() {
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAdminClientFailsToCreateTable() {
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(false);
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().createTable(any()))
        .thenThrow(RuntimeException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAdminClientFailsToClose() {
    setupReadyTable();
    BigtableTableAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableTableAdminClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldWorkWhenBigtableDoesNotThrowAnyError() {
    setupReadyTable();

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(false);

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));

    verify(bigtableResourceManagerClientFactory.bigtableTableAdminClient()).createTable(any());
  }

  @Test
  public void testWriteShouldThrowErrorWhenInstanceDoesNotExist() {
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldExitEarlyWhenNoRowMutationsGiven() {
    testManager.createInstance(cluster);

    Mockito.lenient()
        .when(bigtableResourceManagerClientFactory.bigtableDataClient())
        .thenThrow(RuntimeException.class);

    testManager.write(ImmutableList.of());
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToInstantiate() {
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientFactory.bigtableDataClient())
        .thenThrow(BigtableResourceManagerException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToSendMutations() {
    testManager.createInstance(cluster);
    BigtableDataClient mockClient = bigtableResourceManagerClientFactory.bigtableDataClient();
    doThrow(RuntimeException.class).when(mockClient).mutateRow(any());

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToClose() {
    testManager.createInstance(cluster);
    BigtableDataClient mockClient = bigtableResourceManagerClientFactory.bigtableDataClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
  }

  @Test
  public void testWriteShouldWorkWhenBigtableDoesNotThrowAnyError() {
    testManager.createInstance(cluster);

    testManager.write(RowMutation.create(TABLE_ID, "sample-key"));
    testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key")));

    verify(bigtableResourceManagerClientFactory.bigtableDataClient(), times(2)).mutateRow(any());
  }

  @Test
  public void testReadTableShouldThrowErrorWhenInstanceDoesNotExist() {
    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenTableDoesNotExist() {
    testManager.createInstance(cluster);

    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToInstantiate() {
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    when(bigtableResourceManagerClientFactory.bigtableDataClient())
        .thenThrow(BigtableResourceManagerException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToReadRows() {
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    when(bigtableResourceManagerClientFactory.bigtableDataClient().readRows(any()))
        .thenThrow(NotFoundException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenReadRowsReturnsNull() {
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    when(bigtableResourceManagerClientFactory.bigtableDataClient().readRows(any()))
        .thenReturn(null);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToClose() {
    testManager.createInstance(cluster);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    BigtableDataClient mockClient = bigtableResourceManagerClientFactory.bigtableDataClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldWorkWhenBigtableDoesNotThrowAnyError() {
    testManager.createInstance(cluster);

    ServerStream<Row> mockReadRows = mock(ServerStream.class, Answers.RETURNS_DEEP_STUBS);
    Row mockRow = mock(Row.class);
    when(bigtableResourceManagerClientFactory.bigtableDataClient().readRows(any()))
        .thenReturn(mockReadRows);
    when(mockReadRows.iterator().hasNext()).thenReturn(true, false);
    when(mockReadRows.iterator().next()).thenReturn(mockRow);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);

    testManager.readTable(TABLE_ID);

    verify(bigtableResourceManagerClientFactory.bigtableDataClient()).readRows(any());
  }

  @Test
  public void testCleanupAllCallsDeleteInstance() {
    testManager.createInstance(cluster);
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).deleteInstance(anyString());

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesInstanceAdminClient() {
    testManager.createInstance(cluster);
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesTableAdminClient() {
    testManager.createInstance(cluster);
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).deleteInstance(any());

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceFailsToDelete() {
    testManager.createInstance(cluster);
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).deleteInstance(anyString());

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceAdminClientFailsToClose() {
    testManager.createInstance(cluster);
    BigtableInstanceAdminClient mockClient =
        bigtableResourceManagerClientFactory.bigtableInstanceAdminClient();
    doThrow(RuntimeException.class).when(mockClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldNotCleanupStaticInstance() throws IOException {
    String instanceId = "static-instance";
    testManager =
        new BigtableResourceManager(
            BigtableResourceManager.builder(TEST_ID, PROJECT_ID, null)
                .setInstanceId(instanceId)
                .useStaticInstance(),
            bigtableResourceManagerClientFactory);

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(false);

    setupReadyTable();

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));

    testManager.cleanupAll();
    verify(bigtableResourceManagerClientFactory.bigtableTableAdminClient()).deleteTable(TABLE_ID);
    verify(bigtableResourceManagerClientFactory.bigtableTableAdminClient(), new Times(1))
        .deleteTable(anyString());
    verify(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient(), never())
        .deleteInstance(any());
  }

  private void setupReadyTable() {
    Map<String, ReplicationState> allReplicated = new HashMap<>();
    allReplicated.put(CLUSTER_ID, ReplicationState.READY);

    when(bigtableResourceManagerClientFactory
            .bigtableTableAdminClient()
            .getTable(TABLE_ID)
            .getReplicationStatesByClusterId())
        .thenReturn(allReplicated);
  }

  @Test
  public void testCleanupAllShouldWorkWhenBigtableDoesNotThrowAnyError() {
    setupReadyTable();

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));

    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient().exists(anyString()))
        .thenReturn(true);
    testManager.readTable(TABLE_ID);

    testManager.cleanupAll();

    verify(bigtableResourceManagerClientFactory.bigtableDataClient()).close();
    verify(bigtableResourceManagerClientFactory.bigtableTableAdminClient(), never())
        .deleteTable(TABLE_ID);
    verify(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .deleteInstance(anyString());
  }
}

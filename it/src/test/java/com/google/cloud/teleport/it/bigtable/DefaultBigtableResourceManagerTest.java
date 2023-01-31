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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link com.google.cloud.teleport.it.bigtable.DefaultBigtableResourceManager}. */
@RunWith(JUnit4.class)
public class DefaultBigtableResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private ServerStream<Row> readRows;

  @Mock private Iterator<Row> rows;
  @Mock private Row row;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private BigtableResourceManagerClientFactory bigtableResourceManagerClientFactory;

  @Mock private BigtableInstanceAdminClient bigtableInstanceAdminClient;
  @Mock private BigtableTableAdminClient bigtableTableAdminClient;
  @Mock private BigtableDataClient bigtableDataClient;

  @Mock private CredentialsProvider credentialsProvider;

  private static final String TEST_ID = "test-id";
  private static final String TABLE_ID = "table-id";
  private static final String PROJECT_ID = "test-project";

  private static final String CLUSTER_ID = "cluster-id";
  private static final String CLUSTER_ZONE = "us-central1-a";
  private static final int CLUSTER_NUM_NODES = 1;
  private static final StorageType CLUSTER_STORAGE_TYPE = StorageType.SSD;

  private DefaultBigtableResourceManager testManager;
  private Iterable<BigtableResourceManagerCluster> cluster;

  @Before
  public void setUp() throws IOException {
    testManager =
        new DefaultBigtableResourceManager(
            TEST_ID, PROJECT_ID, bigtableResourceManagerClientFactory);
    cluster =
        ImmutableList.of(
            BigtableResourceManagerCluster.create(
                CLUSTER_ID, CLUSTER_ZONE, CLUSTER_NUM_NODES, CLUSTER_STORAGE_TYPE));
  }

  @Test
  public void testCreateResourceManagerCreatesCorrectIdValues() {
    assertThat(testManager.getInstanceId()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
    assertThat(testManager.getProjectId()).matches(PROJECT_ID);
  }

  @Test
  public void testResourceManagerBuilderGeneratesBigtableResourceManagerClientFactory()
      throws IOException {
    testManager =
        DefaultBigtableResourceManager.builder(TEST_ID, PROJECT_ID)
            .setCredentialsProvider(credentialsProvider)
            .build();
    assertThat(testManager.getInstanceId()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
    assertThat(testManager.getProjectId()).matches(PROJECT_ID);
  }

  @Test
  public void testCreateInstanceShouldThrowExceptionWhenInstanceAlreadyExists() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    testManager.createInstance(cluster);

    assertThrows(IllegalStateException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldThrowExceptionWhenClientFailsToCreateInstance() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    when(bigtableInstanceAdminClient.createInstance(any())).thenThrow(IllegalStateException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldThrowErrorWhenInstanceAdminClientFailsToClose() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.createInstance(cluster));
  }

  @Test
  public void testCreateInstanceShouldWorkWhenBigtableDoesNotThrowAnyError() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    testManager.createInstance(cluster);

    verify(bigtableInstanceAdminClient).createInstance(any());
  }

  @Test
  public void testCreateTableShouldNotCreateInstanceWhenInstanceAlreadyExists() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    testManager.createInstance(cluster);
    Mockito.lenient()
        .when(bigtableInstanceAdminClient.createInstance(any()))
        .thenThrow(IllegalStateException.class);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(false);
    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));
  }

  @Test
  public void testCreateTableShouldCreateInstanceWhenInstanceDoesNotExist() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    when(bigtableInstanceAdminClient.createInstance(any())).thenThrow(IllegalStateException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenNoColumnFamilyGiven() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    assertThrows(
        IllegalArgumentException.class, () -> testManager.createTable(TABLE_ID, new ArrayList<>()));
  }

  @Test
  public void testCreateTableShouldNotCreateTableWhenTableAlreadyExists() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAdminClientFailsToCreateTable() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(false);
    when(bigtableTableAdminClient.createTable(any())).thenThrow(RuntimeException.class);

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldThrowErrorWhenTableAdminClientFailsToClose() {
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    doThrow(RuntimeException.class).when(bigtableTableAdminClient).close();

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.createTable(TABLE_ID, ImmutableList.of("cf1")));
  }

  @Test
  public void testCreateTableShouldWorkWhenBigtableDoesNotThrowAnyError() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(false);

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));

    verify(bigtableTableAdminClient).createTable(any());
  }

  @Test
  public void testWriteShouldThrowErrorWhenInstanceDoesNotExist() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        IllegalStateException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldExitEarlyWhenNoRowMutationsGiven() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    testManager.createInstance(cluster);

    Mockito.lenient()
        .when(bigtableResourceManagerClientFactory.bigtableDataClient())
        .thenThrow(RuntimeException.class);

    testManager.write(ImmutableList.of());
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToInstantiate() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

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
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    doThrow(RuntimeException.class).when(bigtableDataClient).mutateRow(any());

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key"))));
  }

  @Test
  public void testWriteShouldThrowErrorWhenDataClientFailsToClose() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    doThrow(RuntimeException.class).when(bigtableDataClient).close();

    assertThrows(
        BigtableResourceManagerException.class,
        () -> testManager.write(RowMutation.create(TABLE_ID, "sample-key")));
  }

  @Test
  public void testWriteShouldWorkWhenBigtableDoesNotThrowAnyError() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    testManager.write(RowMutation.create(TABLE_ID, "sample-key"));
    testManager.write(ImmutableList.of(RowMutation.create(TABLE_ID, "sample-key")));

    verify(bigtableDataClient, times(2)).mutateRow(any());
  }

  @Test
  public void testReadTableShouldThrowErrorWhenInstanceDoesNotExist() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenTableDoesNotExist() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    testManager.createInstance(cluster);

    assertThrows(IllegalStateException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToInstantiate() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableResourceManagerClientFactory.bigtableDataClient())
        .thenThrow(BigtableResourceManagerException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToReadRows() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableDataClient.readRows(any())).thenThrow(NotFoundException.class);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenReadRowsReturnsNull() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    when(bigtableDataClient.readRows(any())).thenReturn(null);

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldThrowErrorWhenDataClientFailsToClose() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createInstance(cluster);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    doThrow(RuntimeException.class).when(bigtableDataClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.readTable(TABLE_ID));
  }

  @Test
  public void testReadTableShouldWorkWhenBigtableDoesNotThrowAnyError() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);
    testManager.createInstance(cluster);

    when(bigtableDataClient.readRows(any())).thenReturn(readRows);
    when(readRows.iterator()).thenReturn(rows);
    when(rows.hasNext()).thenReturn(true, false);
    when(rows.next()).thenReturn(row);

    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);

    testManager.readTable(TABLE_ID);

    verify(bigtableDataClient).readRows(any());
  }

  @Test
  public void testCleanupAllCallsDeleteInstance() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).deleteInstance(anyString());

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesInstanceAdminClient() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).close();

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllClosesTableAdminClient() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).deleteInstance(any());

    assertThrows(RuntimeException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceFailsToDelete() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).deleteInstance(anyString());

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenInstanceAdminClientFailsToClose() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);

    doThrow(RuntimeException.class).when(bigtableInstanceAdminClient).close();

    assertThrows(BigtableResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldWorkWhenBigtableDoesNotThrowAnyError() {
    // use mocked instance admin client object
    when(bigtableResourceManagerClientFactory.bigtableInstanceAdminClient())
        .thenReturn(bigtableInstanceAdminClient);
    // use mocked table admin client object
    when(bigtableResourceManagerClientFactory.bigtableTableAdminClient())
        .thenReturn(bigtableTableAdminClient);
    // use mocked data client object
    when(bigtableResourceManagerClientFactory.bigtableDataClient()).thenReturn(bigtableDataClient);

    testManager.createTable(TABLE_ID, ImmutableList.of("cf1"));
    when(bigtableDataClient.readRows(any())).thenReturn(readRows);
    when(bigtableTableAdminClient.exists(anyString())).thenReturn(true);
    testManager.readTable(TABLE_ID);

    testManager.cleanupAll();

    verify(bigtableDataClient).close();
    verify(bigtableInstanceAdminClient).deleteInstance(anyString());
  }
}

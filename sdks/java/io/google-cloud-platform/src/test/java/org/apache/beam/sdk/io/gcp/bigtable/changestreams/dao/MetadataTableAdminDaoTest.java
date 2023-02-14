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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao.COLUMN_FAMILIES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.admin.v2.GcRule;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.admin.v2.models.Table;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MetadataTableAdminDaoTest {

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  private static BigtableTableAdminClient tableAdminClient;
  private MetadataTableAdminDao metadataTableAdminDao;

  private final String tableId = "my-table";

  @BeforeClass
  public static void beforeClass() throws IOException {
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(BIGTABLE_EMULATOR_RULE.getPort())
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .build();
    tableAdminClient = BigtableTableAdminClient.create(adminSettings);
  }

  @Before
  public void before() {
    if (tableAdminClient.exists(tableId)) {
      tableAdminClient.deleteTable(tableId);
    }
    // We are unable, for now, to test anything with instanceAdminClient because it is marked as
    // final, and we cannot mock final classes.
    metadataTableAdminDao =
        new MetadataTableAdminDao(tableAdminClient, null, "my-change-stream", tableId);
  }

  @Test
  public void testCreateTableDoesNotExist() {
    assertTrue(metadataTableAdminDao.createMetadataTable());
    com.google.bigtable.admin.v2.ColumnFamily gcRule =
        com.google.bigtable.admin.v2.ColumnFamily.newBuilder()
            .setGcRule(GcRule.newBuilder().setMaxNumVersions(1).build())
            .build();
    List<ColumnFamily> expectedColumnFamilies = new ArrayList<>();
    for (String columnFamilyName : COLUMN_FAMILIES) {
      expectedColumnFamilies.add(ColumnFamily.fromProto(columnFamilyName, gcRule));
    }
    Table table = tableAdminClient.getTable(tableId);
    assertThat(
        table.getColumnFamilies(), Matchers.containsInAnyOrder(expectedColumnFamilies.toArray()));
  }

  @Test
  public void testCreateTableAlreadyExists() {
    assertTrue(metadataTableAdminDao.createMetadataTable());
    // Verify column families are correct.
    Table table = tableAdminClient.getTable(tableId);
    assertEquals(COLUMN_FAMILIES.size(), table.getColumnFamilies().size());
    assertThat(
        table.getColumnFamilies().stream().map(ColumnFamily::getId).collect(Collectors.toList()),
        Matchers.containsInAnyOrder(COLUMN_FAMILIES.toArray()));

    assertFalse(metadataTableAdminDao.createMetadataTable());
    // Verify the expected column families are still there.
    table = tableAdminClient.getTable(tableId);
    assertEquals(COLUMN_FAMILIES.size(), table.getColumnFamilies().size());
    assertThat(
        table.getColumnFamilies().stream().map(ColumnFamily::getId).collect(Collectors.toList()),
        Matchers.containsInAnyOrder(COLUMN_FAMILIES.toArray()));
  }

  @Test
  public void testNewColumnFamiliesAreAddedInExistingTable() {
    CreateTableRequest createTableRequest = CreateTableRequest.of(tableId);
    tableAdminClient.createTable(createTableRequest);
    Table table = tableAdminClient.getTable(tableId);
    assertEquals(0, table.getColumnFamilies().size());

    assertFalse(metadataTableAdminDao.createMetadataTable());
    table = tableAdminClient.getTable(tableId);
    assertEquals(COLUMN_FAMILIES.size(), table.getColumnFamilies().size());
    assertThat(
        table.getColumnFamilies().stream().map(ColumnFamily::getId).collect(Collectors.toList()),
        Matchers.containsInAnyOrder(COLUMN_FAMILIES.toArray()));
  }
}

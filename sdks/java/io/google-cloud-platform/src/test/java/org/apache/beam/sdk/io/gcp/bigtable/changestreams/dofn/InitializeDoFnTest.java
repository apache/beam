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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitializeDoFnTest {
  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR_RULE = BigtableEmulatorRule.create();

  @Mock private DaoFactory daoFactory;
  @Mock private transient MetadataTableAdminDao metadataTableAdminDao;
  private transient MetadataTableDao metadataTableDao;
  @Mock private DoFn.OutputReceiver<Instant> outputReceiver;
  private final String tableId = "table";

  private static BigtableDataClient dataClient;
  private static BigtableTableAdminClient adminClient;

  @BeforeClass
  public static void beforeClass() throws IOException {
    BigtableTableAdminSettings adminSettings =
        BigtableTableAdminSettings.newBuilderForEmulator(BIGTABLE_EMULATOR_RULE.getPort())
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .build();
    adminClient = BigtableTableAdminClient.create(adminSettings);
    BigtableDataSettings dataSettingsBuilder =
        BigtableDataSettings.newBuilderForEmulator(BIGTABLE_EMULATOR_RULE.getPort())
            .setProjectId("fake-project")
            .setInstanceId("fake-instance")
            .build();
    dataClient = BigtableDataClient.create(dataSettingsBuilder);
  }

  @Before
  public void setUp() throws IOException {
    String changeStreamName = "changeStreamName";
    metadataTableAdminDao =
        spy(new MetadataTableAdminDao(adminClient, null, changeStreamName, tableId));
    doReturn(true).when(metadataTableAdminDao).isAppProfileSingleClusterAndTransactional(any());
    when(daoFactory.getMetadataTableAdminDao()).thenReturn(metadataTableAdminDao);
    metadataTableDao =
        new MetadataTableDao(
            dataClient, tableId, metadataTableAdminDao.getChangeStreamNamePrefix());
    when(daoFactory.getMetadataTableDao()).thenReturn(metadataTableDao);
    when(daoFactory.getChangeStreamName()).thenReturn(changeStreamName);
  }

  @Test
  public void testInitializeDefault() throws IOException {
    Instant startTime = Instant.now();
    InitializeDoFn initializeDoFn = new InitializeDoFn(daoFactory, "app-profile", startTime);
    initializeDoFn.processElement(outputReceiver);
    verify(outputReceiver, times(1)).output(startTime);
    assertTrue(adminClient.exists(tableId));
    Row row =
        dataClient.readRow(
            tableId,
            metadataTableAdminDao
                .getChangeStreamNamePrefix()
                .concat(MetadataTableAdminDao.DETECT_NEW_PARTITION_SUFFIX));
    assertNotNull(row);
    assertEquals(
        1,
        row.getCells(MetadataTableAdminDao.CF_VERSION, MetadataTableAdminDao.QUALIFIER_DEFAULT)
            .size());
    assertEquals(
        MetadataTableAdminDao.CURRENT_METADATA_TABLE_VERSION,
        (int)
            Longs.fromByteArray(
                row.getCells(
                        MetadataTableAdminDao.CF_VERSION, MetadataTableAdminDao.QUALIFIER_DEFAULT)
                    .get(0)
                    .getValue()
                    .toByteArray()));
  }
}

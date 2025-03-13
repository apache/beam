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
package org.apache.beam.sdk.io.hbase;

import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colFamily2;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colQualifier;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.colQualifier2;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.rowKey;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.rowKey2;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.timeT;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.value;
import static org.apache.beam.sdk.io.hbase.utils.TestConstants.value2;

import java.io.IOException;
import org.apache.beam.sdk.io.hbase.utils.TestHBaseUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Unit tests for Hbase row mutation IO. */
@RunWith(JUnit4.class)
public class HbaseIOWriteRowMutationsTest {
  private static final Logger LOG = LoggerFactory.getLogger(HbaseIOWriteRowMutationsTest.class);

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  private static HBaseTestingUtility htu;
  private static final Configuration conf = HBaseConfiguration.create();

  public HbaseIOWriteRowMutationsTest() {}

  @BeforeClass
  public static void setUpCluster() throws Exception {
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    // Try to bind the hostname to localhost to solve an issue when it is not configured or
    // no DNS resolution available.
    conf.setStrings("hbase.master.hostname", "localhost");
    conf.setStrings("hbase.regionserver.hostname", "localhost");

    // Create an HBase test cluster with one table.
    htu = new HBaseTestingUtility();
    // We don't use the full htu.startMiniCluster() to avoid starting unneeded HDFS/MR daemons
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 4);
    hbm.waitForActiveAndReadyMaster();
    LOG.info("Hbase test cluster started.");
  }

  @AfterClass
  public static void tearDownCluster() throws Exception {
    if (htu != null) {
      htu.shutdownMiniHBaseCluster();
      htu.shutdownMiniZKCluster();
      htu.cleanupTestDir();
      htu = null;
    }
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    // Provide custom encoder to non-serializable RowMutations class.
    pipeline
        .getCoderRegistry()
        .registerCoderForClass(RowMutations.class, HBaseRowMutationsCoder.of());
  }

  @Test
  public void testWritesPuts() throws Exception {

    Table table = TestHBaseUtils.createTable(htu);

    // Write two cells in one row mutations object
    RowMutations rowMutationsOnTwoColumnFamilies = new RowMutations(rowKey);
    rowMutationsOnTwoColumnFamilies.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    rowMutationsOnTwoColumnFamilies.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily2, colQualifier2, value2, timeT));

    // Two mutations on same cell, later one should overwrite earlier one
    RowMutations overwritingRowMutations = new RowMutations(rowKey2);
    overwritingRowMutations.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey2, colFamily, colQualifier, value, timeT));
    overwritingRowMutations.add(
        // Overwrites previous mutation
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey2, colFamily, colQualifier, value2, timeT));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(
                KV.of(rowKey, rowMutationsOnTwoColumnFamilies),
                KV.of(rowKey2, overwritingRowMutations)))
        .apply(
            "Write to hbase",
            HBaseIO.writeRowMutations()
                .withConfiguration(htu.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertEquals(2, TestHBaseUtils.getRowResult(table, rowKey).size());
    Assert.assertArrayEquals(value, TestHBaseUtils.getCell(table, rowKey, colFamily, colQualifier));
    Assert.assertArrayEquals(
        value2, TestHBaseUtils.getCell(table, rowKey, colFamily2, colQualifier2));

    Assert.assertEquals(1, TestHBaseUtils.getRowResult(table, rowKey2).size());
    Assert.assertArrayEquals(
        value2, TestHBaseUtils.getCell(table, rowKey2, colFamily, colQualifier));
  }

  @Test
  public void testWritesDeletes() throws Exception {
    Table table = TestHBaseUtils.createTable(htu);

    // Expect deletes to result in empty row.
    RowMutations deleteCellMutation = new RowMutations(rowKey);
    deleteCellMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    deleteCellMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createDelete(rowKey, colFamily, colQualifier, timeT));
    // Expect delete family to delete entire row.
    RowMutations deleteColFamilyMutation = new RowMutations(rowKey2);
    deleteColFamilyMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey2, colFamily, colQualifier, value, timeT));
    deleteColFamilyMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey2, colFamily, colQualifier2, value2, timeT));
    deleteColFamilyMutation.add(
        TestHBaseUtils.HBaseMutationBuilder.createDeleteFamily(rowKey2, colFamily, Long.MAX_VALUE));

    pipeline
        .apply(
            "Create row mutations",
            Create.of(KV.of(rowKey, deleteCellMutation), KV.of(rowKey2, deleteColFamilyMutation)))
        .apply(
            "Write to hbase",
            HBaseIO.writeRowMutations()
                .withConfiguration(htu.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertTrue(TestHBaseUtils.getRowResult(table, rowKey).isEmpty());
    Assert.assertTrue(TestHBaseUtils.getRowResult(table, rowKey2).isEmpty());
  }

  @Test
  public void testWritesDeletesThenPutsInOrderByTimestamp() throws Exception {
    Table table = TestHBaseUtils.createTable(htu);

    // RowMutations entry ordering does not guarantee mutation ordering, as Hbase operations
    // are ordered by timestamp. See https://issues.apache.org/jira/browse/HBASE-2256
    RowMutations putDeletePut = new RowMutations(rowKey);
    putDeletePut.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value, timeT));
    putDeletePut.add(
        TestHBaseUtils.HBaseMutationBuilder.createDeleteFamily(rowKey, colFamily, timeT + 1));
    putDeletePut.add(
        TestHBaseUtils.HBaseMutationBuilder.createPut(
            rowKey, colFamily, colQualifier, value2, timeT + 2));

    pipeline
        .apply("Create row mutations", Create.of(KV.of(rowKey, putDeletePut)))
        .apply(
            "Write to hbase",
            HBaseIO.writeRowMutations()
                .withConfiguration(htu.getConfiguration())
                .withTableId(table.getName().getNameAsString()));

    pipeline.run().waitUntilFinish();

    Assert.assertArrayEquals(
        value2, TestHBaseUtils.getCell(table, rowKey, colFamily, colQualifier));
  }
}

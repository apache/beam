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

import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hbase.HBaseIO.HBaseSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test HBaseIO. */
@RunWith(JUnit4.class)
public class HBaseIOTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryHBaseTable tmpTable = new TemporaryHBaseTable();

  private static HBaseTestingUtility htu;
  private static HBaseAdmin admin;

  private static final Configuration conf = HBaseConfiguration.create();
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("info");
  private static final byte[] COLUMN_NAME = Bytes.toBytes("name");
  private static final byte[] COLUMN_EMAIL = Bytes.toBytes("email");

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    // Try to bind the hostname to localhost to solve an issue when it is not configured or
    // no DNS resolution available.
    conf.setStrings("hbase.master.hostname", "localhost");
    conf.setStrings("hbase.regionserver.hostname", "localhost");
    htu = new HBaseTestingUtility(conf);

    // We don't use the full htu.startMiniCluster() to avoid starting unneeded HDFS/MR daemons
    htu.startMiniZKCluster();
    MiniHBaseCluster hbm = htu.startMiniHBaseCluster(1, 4);
    hbm.waitForActiveAndReadyMaster();

    admin = htu.getHBaseAdmin();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (admin != null) {
      admin.close();
      admin = null;
    }
    if (htu != null) {
      htu.shutdownMiniHBaseCluster();
      htu.shutdownMiniZKCluster();
      htu.cleanupTestDir();
      htu = null;
    }
  }

  @Test
  public void testReadBuildsCorrectly() {
    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId("table");
    assertEquals("table", read.getTableId());
    assertNotNull("configuration", read.getConfiguration());
  }

  @Test
  public void testReadBuildsCorrectlyInDifferentOrder() {
    HBaseIO.Read read = HBaseIO.read().withTableId("table").withConfiguration(conf);
    assertEquals("table", read.getTableId());
    assertNotNull("configuration", read.getConfiguration());
  }

  @Test
  public void testWriteBuildsCorrectly() {
    HBaseIO.Write write = HBaseIO.write().withConfiguration(conf).withTableId("table");
    assertEquals("table", write.getTableId());
    assertNotNull("configuration", write.getConfiguration());
  }

  @Test
  public void testWriteBuildsCorrectlyInDifferentOrder() {
    HBaseIO.Write write = HBaseIO.write().withTableId("table").withConfiguration(conf);
    assertEquals("table", write.getTableId());
    assertNotNull("configuration", write.getConfiguration());
  }

  @Test
  public void testWriteValidationFailsMissingTable() {
    HBaseIO.Write write = HBaseIO.write().withConfiguration(conf);
    thrown.expect(IllegalArgumentException.class);
    write.expand(null /* input */);
  }

  @Test
  public void testWriteValidationFailsMissingConfiguration() {
    HBaseIO.Write write = HBaseIO.write().withTableId("table");
    thrown.expect(IllegalArgumentException.class);
    write.expand(null /* input */);
  }

  /** Tests that when reading from a non-existent table, the read fails. */
  @Test
  public void testReadingFailsTableDoesNotExist() {
    final String table = tmpTable.getName();
    // Exception will be thrown by read.expand() when read is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));
    runReadTest(
        HBaseIO.read().withConfiguration(conf).withTableId(table), false, new ArrayList<>());
    runReadTest(HBaseIO.read().withConfiguration(conf).withTableId(table), true, new ArrayList<>());
  }

  /** Tests that when reading from an empty table, the read succeeds. */
  @Test
  public void testReadingEmptyTable() throws Exception {
    final String table = tmpTable.getName();
    createTable(table);

    runReadTest(
        HBaseIO.read().withConfiguration(conf).withTableId(table), false, new ArrayList<>());
    runReadTest(HBaseIO.read().withConfiguration(conf).withTableId(table), true, new ArrayList<>());
  }

  @Test
  public void testReading() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    createAndWriteData(table, numRows);

    runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table), false, numRows);
  }

  @Test
  public void testReadingSDF() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    createAndWriteData(table, numRows);

    runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table), true, numRows);
  }

  /** Tests reading all rows from a split table. */
  @Test
  public void testReadingWithSplits() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1500;
    final int numRegions = 4;
    final long bytesPerRow = 100L;
    createAndWriteData(table, numRows);

    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId(table);
    HBaseSource source = new HBaseSource(read, null /* estimatedSizeBytes */);
    List<? extends BoundedSource<Result>> splits =
        source.split(numRows * bytesPerRow / numRegions, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(4));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /** Tests that a {@link HBaseSource} can be read twice, verifying its immutability. */
  @Test
  public void testReadingSourceTwice() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 10;
    createAndWriteData(table, numRows);

    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId(table);
    HBaseSource source = new HBaseSource(read, null /* estimatedSizeBytes */);
    assertThat(SourceTestUtils.readFromSource(source, null), hasSize(numRows));
    // second read.
    assertThat(SourceTestUtils.readFromSource(source, null), hasSize(numRows));
  }

  /** Tests reading all rows using a filter. */
  @Test
  public void testReadingWithFilter() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    createAndWriteData(table, numRows);

    String regex = ".*17.*";
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withFilter(filter), false, 20);
  }

  @Test
  public void testReadingWithFilterSDF() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    createAndWriteData(table, numRows);

    String regex = ".*17.*";
    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withFilter(filter), true, 20);
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted range
   * [] and that some properties hold across them.
   */
  @Test
  public void testReadingKeyRangePrefix() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    final ByteKey startKey = ByteKey.copyFrom("2".getBytes(StandardCharsets.UTF_8));
    createAndWriteData(table, numRows);

    // Test prefix: [beginning, startKey).
    final ByteKeyRange prefixRange = ByteKeyRange.ALL_KEYS.withEndKey(startKey);
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withKeyRange(prefixRange),
        false,
        126);
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted range
   * [] and that some properties hold across them.
   */
  @Test
  public void testReadingKeyRangeSuffix() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    final ByteKey startKey = ByteKey.copyFrom("2".getBytes(StandardCharsets.UTF_8));
    createAndWriteData(table, numRows);

    // Test suffix: [startKey, end).
    final ByteKeyRange suffixRange = ByteKeyRange.ALL_KEYS.withStartKey(startKey);
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withKeyRange(suffixRange),
        false,
        875);
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted range
   * [] and that some properties hold across them.
   */
  @Test
  public void testReadingKeyRangeMiddle() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    final byte[] startRow = "2".getBytes(StandardCharsets.UTF_8);
    final byte[] stopRow = "9".getBytes(StandardCharsets.UTF_8);
    createAndWriteData(table, numRows);

    // Test restricted range: [startKey, endKey).
    // This one tests the second signature of .withKeyRange
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withKeyRange(startRow, stopRow),
        false,
        441);
  }

  @Test
  public void testReadingKeyRangeMiddleSDF() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 1001;
    final byte[] startRow = "2".getBytes(StandardCharsets.UTF_8);
    final byte[] stopRow = "9".getBytes(StandardCharsets.UTF_8);
    createAndWriteData(table, numRows);

    // Test restricted range: [startKey, endKey).
    // This one tests the second signature of .withKeyRange
    runReadTestLength(
        HBaseIO.read().withConfiguration(conf).withTableId(table).withKeyRange(startRow, stopRow),
        true,
        441);
  }

  /** Tests dynamic work rebalancing exhaustively. */
  @Test
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 7;
    createAndWriteData(table, numRows);

    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId(table);
    HBaseSource source =
        new HBaseSource(read, null /* estimatedSizeBytes */)
            .withStartKey(ByteKey.of(48))
            .withEndKey(ByteKey.of(58));

    assertSplitAtFractionExhaustive(source, null);
  }

  /** Unit tests of splitAtFraction. */
  @Test
  public void testReadingSplitAtFraction() throws Exception {
    final String table = tmpTable.getName();
    final int numRows = 10;
    createAndWriteData(table, numRows);

    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId(table);
    HBaseSource source = new HBaseSource(read, null /* estimatedSizeBytes */);

    // The value k is based on the partitioning schema for the data, in this test case,
    // the partitioning is HEX-based, so we start from 1/16m and the value k will be
    // around 1/256, so the tests are done in approximately k ~= 0.003922 steps
    double k = 0.003922;

    assertSplitAtFractionFails(source, 0, k, null /* options */);
    assertSplitAtFractionFails(source, 0, 1.0, null /* options */);
    // With 1 items read, all split requests past k will succeed.
    assertSplitAtFractionSucceedsAndConsistent(source, 1, k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.666, null /* options */);
    // With 3 items read, all split requests past 3k will succeed.
    assertSplitAtFractionFails(source, 3, 2 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 3 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 4 * k, null /* options */);
    // With 6 items read, all split requests past 6k will succeed.
    assertSplitAtFractionFails(source, 6, 5 * k, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 6, 0.7, null /* options */);
  }

  @Test
  public void testReadingDisplayData() {
    HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId("fooTable");
    DisplayData displayData = DisplayData.from(read);
    assertThat(displayData, hasDisplayItem("tableId", "fooTable"));
    assertThat(displayData, hasDisplayItem("configuration"));
  }

  /** Tests that a record gets written to the service and messages are logged. */
  @Test
  public void testWriting() throws Exception {
    final String table = tmpTable.getName();
    final String key = "key";
    final String value = "value";
    final int numMutations = 100;

    createTable(table);

    p.apply("multiple rows", Create.of(makeMutations(key, value, numMutations)))
        .apply("write", HBaseIO.write().withConfiguration(conf).withTableId(table));
    p.run().waitUntilFinish();

    List<Result> results = readTable(table, new Scan());
    assertEquals(numMutations, results.size());
  }

  /** Tests that when writing to a non-existent table, the write fails. */
  @Test
  public void testWritingFailsTableDoesNotExist() {
    final String table = tmpTable.getName();

    // Exception will be thrown by write.expand() when writeToDynamic is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));
    p.apply(Create.empty(HBaseMutationCoder.of()))
        .apply("write", HBaseIO.write().withConfiguration(conf).withTableId(table));
  }

  /** Tests that when writing an element fails, the write fails. */
  @Test
  public void testWritingFailsBadElement() throws Exception {
    final String table = tmpTable.getName();
    final String key = "KEY";
    createTable(table);

    p.apply(Create.of(makeBadMutation(key)))
        .apply(HBaseIO.write().withConfiguration(conf).withTableId(table));

    thrown.expect(Pipeline.PipelineExecutionException.class);
    thrown.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
    thrown.expectMessage("No columns to insert");
    p.run().waitUntilFinish();
  }

  @Test
  public void testWritingDisplayData() {
    final String table = tmpTable.getName();
    HBaseIO.Write write = HBaseIO.write().withTableId(table).withConfiguration(conf);
    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("tableId", table));
  }

  // HBase helper methods
  private static void createTable(String tableId) throws Exception {
    byte[][] splitKeys = {
      "4".getBytes(StandardCharsets.UTF_8),
      "8".getBytes(StandardCharsets.UTF_8),
      "C".getBytes(StandardCharsets.UTF_8)
    };
    createTable(tableId, COLUMN_FAMILY, splitKeys);
  }

  private static void createTable(String tableId, byte[] columnFamily, byte[][] splitKeys)
      throws Exception {
    TableName tableName = TableName.valueOf(tableId);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor colDef = new HColumnDescriptor(columnFamily);
    desc.addFamily(colDef);
    admin.createTable(desc, splitKeys);
  }

  /** Helper function to create a table and return the rows that it created. */
  private static void writeData(String tableId, int numRows) throws Exception {
    Connection connection = admin.getConnection();
    TableName tableName = TableName.valueOf(tableId);
    BufferedMutator mutator = connection.getBufferedMutator(tableName);
    List<Mutation> mutations = makeTableData(numRows);
    mutator.mutate(mutations);
    mutator.flush();
    mutator.close();
  }

  private static void createAndWriteData(final String tableId, final int numRows) throws Exception {
    createTable(tableId);
    writeData(tableId, numRows);
  }

  private static List<Mutation> makeTableData(int numRows) {
    List<Mutation> mutations = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      // We pad values in hex order 0,1, ... ,F,0, ...
      String prefix = String.format("%X", i % 16);
      // This 21 is to have a key longer than an input
      byte[] rowKey = Bytes.toBytes(StringUtils.leftPad("_" + String.valueOf(i), 21, prefix));
      byte[] value = Bytes.toBytes(String.valueOf(i));
      byte[] valueEmail = Bytes.toBytes(String.valueOf(i) + "@email.com");
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_NAME, value));
      mutations.add(new Put(rowKey).addColumn(COLUMN_FAMILY, COLUMN_EMAIL, valueEmail));
    }
    return mutations;
  }

  private static ResultScanner scanTable(String tableId, Scan scan) throws Exception {
    Connection connection = ConnectionFactory.createConnection(conf);
    TableName tableName = TableName.valueOf(tableId);
    Table table = connection.getTable(tableName);
    return table.getScanner(scan);
  }

  private static List<Result> readTable(String tableId, Scan scan) throws Exception {
    ResultScanner scanner = scanTable(tableId, scan);
    List<Result> results = new ArrayList<>();
    for (Result result : scanner) {
      results.add(result);
    }
    scanner.close();
    return results;
  }

  // Beam helper methods
  /** Helper function to make a single row mutation to be written. */
  private static Iterable<Mutation> makeMutations(String key, String value, int numMutations) {
    List<Mutation> mutations = new ArrayList<>();
    for (int i = 0; i < numMutations; i++) {
      mutations.add(makeMutation(key + i, value));
    }
    return mutations;
  }

  private static Mutation makeMutation(String key, String value) {
    return new Put(key.getBytes(StandardCharsets.UTF_8))
        .addColumn(COLUMN_FAMILY, COLUMN_NAME, Bytes.toBytes(value))
        .addColumn(COLUMN_FAMILY, COLUMN_EMAIL, Bytes.toBytes(value + "@email.com"));
  }

  private static Mutation makeBadMutation(String key) {
    return new Put(key.getBytes(StandardCharsets.UTF_8));
  }

  private void runReadTest(HBaseIO.Read read, boolean useSdf, List<Result> expected) {
    PCollection<Result> rows = applyRead(read, useSdf);
    PAssert.that(rows).containsInAnyOrder(expected);
    p.run().waitUntilFinish();
  }

  private void runReadTestLength(HBaseIO.Read read, boolean useSdf, long numElements) {
    PCollection<Result> rows = applyRead(read, useSdf);
    final String transformId = read.getTableId() + "_" + read.getKeyRange();
    PAssert.thatSingleton(rows.apply("Count" + transformId, Count.globally()))
        .isEqualTo(numElements);
    p.run().waitUntilFinish();
  }

  private PCollection<Result> applyRead(HBaseIO.Read read, boolean useSdf) {
    final String transformId = read.getTableId() + "_" + read.getKeyRange();
    return useSdf
        ? p.apply("Create" + transformId, Create.of(read))
            .apply("ReadAll" + transformId, HBaseIO.readAll())
        : p.apply("Read" + transformId, read);
  }

  private static class TemporaryHBaseTable extends ExternalResource {
    private String name;

    @Override
    protected void before() {
      name = "table_" + UUID.randomUUID();
    }

    String getName() {
      return name;
    }
  }
}

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
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.hbase.HBaseIO.HBaseSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test HBaseIO.
 */
@RunWith(JUnit4.class)
public class HBaseIOTest {
    @Rule public final transient TestPipeline p = TestPipeline.create();
    @Rule public ExpectedException thrown = ExpectedException.none();

    private static HBaseTestingUtility htu;
    private static HBaseAdmin admin;

    private static Configuration conf = HBaseConfiguration.create();
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
        htu.startMiniCluster(1, 4);
        admin = htu.getHBaseAdmin();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (htu != null) {
            htu.shutdownMiniCluster();
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
        write.validate(null /* input */);
    }

    @Test
    public void testWriteValidationFailsMissingConfiguration() {
        HBaseIO.Write write = HBaseIO.write().withTableId("table");
        thrown.expect(IllegalArgumentException.class);
        write.validate(null /* input */);
    }

    /** Tests that when reading from a non-existent table, the read fails. */
    @Test
    public void testReadingFailsTableDoesNotExist() throws Exception {
        final String table = "TEST-TABLE-INVALID";
        // Exception will be thrown by read.validate() when read is applied.
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(String.format("Table %s does not exist", table));
        runReadTest(HBaseIO.read().withConfiguration(conf).withTableId(table),
                new ArrayList<Result>());
    }

    /** Tests that when reading from an empty table, the read succeeds. */
    @Test
    public void testReadingEmptyTable() throws Exception {
        final String table = "TEST-EMPTY-TABLE";
        createTable(table);
        runReadTest(HBaseIO.read().withConfiguration(conf).withTableId(table),
                new ArrayList<Result>());
    }

    @Test
    public void testReading() throws Exception {
        final String table = "TEST-MANY-ROWS-TABLE";
        final int numRows = 1001;
        createTable(table);
        writeData(table, numRows);
        runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table), 1001);
    }

    /** Tests reading all rows from a split table. */
    @Test
    public void testReadingWithSplits() throws Exception {
        final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
        final int numRows = 1500;
        final int numRegions = 4;
        final long bytesPerRow = 100L;

        // Set up test table data and sample row keys for size estimation and splitting.
        createTable(table);
        writeData(table, numRows);

        HBaseIO.Read read = HBaseIO.read().withConfiguration(conf).withTableId(table);
        HBaseSource source = new HBaseSource(read, null /* estimatedSizeBytes */);
        List<? extends BoundedSource<Result>> splits =
                source.split(numRows * bytesPerRow / numRegions,
                        null /* options */);

        // Test num splits and split equality.
        assertThat(splits, hasSize(4));
        assertSourcesEqualReferenceSource(source, splits, null /* options */);
    }


    /** Tests reading all rows using a filter. */
    @Test
    public void testReadingWithFilter() throws Exception {
        final String table = "TEST-FILTER-TABLE";
        final int numRows = 1001;

        createTable(table);
        writeData(table, numRows);

        String regex = ".*17.*";
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(regex));
        HBaseIO.Read read =
                HBaseIO.read().withConfiguration(conf).withTableId(table).withFilter(filter);
        runReadTestLength(read, 20);
    }

    /**
     * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted
     * range [] and that some properties hold across them.
     */
    @Test
    public void testReadingWithKeyRange() throws Exception {
        final String table = "TEST-KEY-RANGE-TABLE";
        final int numRows = 1001;
        final byte[] startRow = "2".getBytes();
        final byte[] stopRow = "9".getBytes();
        final ByteKey startKey = ByteKey.copyFrom(startRow);

        createTable(table);
        writeData(table, numRows);

        // Test prefix: [beginning, startKey).
        final ByteKeyRange prefixRange = ByteKeyRange.ALL_KEYS.withEndKey(startKey);
        runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table)
                .withKeyRange(prefixRange), 126);

        // Test suffix: [startKey, end).
        final ByteKeyRange suffixRange = ByteKeyRange.ALL_KEYS.withStartKey(startKey);
        runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table)
                .withKeyRange(suffixRange), 875);

        // Test restricted range: [startKey, endKey).
        // This one tests the second signature of .withKeyRange
        runReadTestLength(HBaseIO.read().withConfiguration(conf).withTableId(table)
                .withKeyRange(startRow, stopRow), 441);
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
        final String table = "table";
        final String key = "key";
        final String value = "value";

        createTable(table);

        p.apply("single row", Create.of(makeWrite(key, value)).withCoder(HBaseIO.WRITE_CODER))
                .apply("write", HBaseIO.write().withConfiguration(conf).withTableId(table));
        p.run().waitUntilFinish();

        List<Result> results = readTable(table, new Scan());
        assertEquals(1, results.size());
    }

    /** Tests that when writing to a non-existent table, the write fails. */
    @Test
    public void testWritingFailsTableDoesNotExist() throws Exception {
        final String table = "TEST-TABLE";

        PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
                p.apply(Create.empty(HBaseIO.WRITE_CODER));

        // Exception will be thrown by write.validate() when write is applied.
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage(String.format("Table %s does not exist", table));

        emptyInput.apply("write", HBaseIO.write().withConfiguration(conf).withTableId(table));
    }

    /** Tests that when writing an element fails, the write fails. */
    @Test
    public void testWritingFailsBadElement() throws Exception {
        final String table = "TEST-TABLE";
        final String key = "KEY";
        createTable(table);

        p.apply(Create.of(makeBadWrite(key)).withCoder(HBaseIO.WRITE_CODER))
                .apply(HBaseIO.write().withConfiguration(conf).withTableId(table));

        thrown.expect(Pipeline.PipelineExecutionException.class);
        thrown.expectCause(Matchers.<Throwable>instanceOf(IllegalArgumentException.class));
        thrown.expectMessage("No columns to insert");
        p.run().waitUntilFinish();
    }

    @Test
    public void testWritingDisplayData() {
        HBaseIO.Write write = HBaseIO.write().withTableId("fooTable").withConfiguration(conf);
        DisplayData displayData = DisplayData.from(write);
        assertThat(displayData, hasDisplayItem("tableId", "fooTable"));
    }

    // HBase helper methods
    private static void createTable(String tableId) throws Exception {
        byte[][] splitKeys = {"4".getBytes(), "8".getBytes(), "C".getBytes()};
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

    /**
     * Helper function to create a table and return the rows that it created.
     */
    private static void writeData(String tableId, int numRows) throws Exception {
        Connection connection = admin.getConnection();
        TableName tableName = TableName.valueOf(tableId);
        BufferedMutator mutator = connection.getBufferedMutator(tableName);
        List<Mutation> mutations = makeTableData(numRows);
        mutator.mutate(mutations);
        mutator.flush();
        mutator.close();
    }

    private static List<Mutation> makeTableData(int numRows) {
        List<Mutation> mutations = new ArrayList<>(numRows);
        for (int i = 0; i < numRows; ++i) {
            // We pad values in hex order 0,1, ... ,F,0, ...
            String prefix = String.format("%X", i % 16);
            // This 21 is to have a key longer than an input
            byte[] rowKey = Bytes.toBytes(
                    StringUtils.leftPad("_" + String.valueOf(i), 21, prefix));
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
    private static KV<ByteString, Iterable<Mutation>> makeWrite(String key, String value) {
        ByteString rowKey = ByteString.copyFromUtf8(key);
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(makeMutation(key, value));
        return KV.of(rowKey, (Iterable<Mutation>) mutations);
    }


    private static Mutation makeMutation(String key, String value) {
        ByteString rowKey = ByteString.copyFromUtf8(key);
        return new Put(rowKey.toByteArray())
                    .addColumn(COLUMN_FAMILY, COLUMN_NAME, Bytes.toBytes(value))
                    .addColumn(COLUMN_FAMILY, COLUMN_EMAIL, Bytes.toBytes(value + "@email.com"));
    }

    private static KV<ByteString, Iterable<Mutation>> makeBadWrite(String key) {
        Put put = new Put(key.getBytes());
        List<Mutation> mutations = new ArrayList<>();
        mutations.add(put);
        return KV.of(ByteString.copyFromUtf8(key), (Iterable<Mutation>) mutations);
    }

    private void runReadTest(HBaseIO.Read read, List<Result> expected) {
        final String transformId = read.getTableId() + "_" + read.getKeyRange();
        PCollection<Result> rows = p.apply("Read" + transformId, read);
        PAssert.that(rows).containsInAnyOrder(expected);
        p.run().waitUntilFinish();
    }

    private void runReadTestLength(HBaseIO.Read read, long numElements) {
        final String transformId = read.getTableId() + "_" + read.getKeyRange();
        PCollection<Result> rows = p.apply("Read" + transformId, read);
        PAssert.thatSingleton(rows.apply("Count" + transformId,
                Count.<Result>globally())).isEqualTo(numElements);
        p.run().waitUntilFinish();
    }
}

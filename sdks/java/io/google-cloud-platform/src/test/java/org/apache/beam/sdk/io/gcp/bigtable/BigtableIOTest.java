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
package org.apache.beam.sdk.io.gcp.bigtable;

import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verifyNotNull;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

import com.google.bigtable.v1.Cell;
import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.bigtable.v1.Row;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

/**
 * Unit tests for {@link BigtableIO}.
 */
@RunWith(JUnit4.class)
public class BigtableIOTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigtableIO.class);

  /**
   * These tests requires a static instance of the {@link FakeBigtableService} because the writers
   * go through a serialization step when executing the test and would not affect passed-in objects
   * otherwise.
   */
  private static FakeBigtableService service;
  private static final BigtableOptions BIGTABLE_OPTIONS =
      new BigtableOptions.Builder()
          .setProjectId("project")
          .setClusterId("cluster")
          .setZoneId("zone")
          .build();
  private static BigtableIO.Read defaultRead =
      BigtableIO.read().withBigtableOptions(BIGTABLE_OPTIONS);
  private static BigtableIO.Write defaultWrite =
      BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS);
  private Coder<KV<ByteString, Iterable<Mutation>>> bigtableCoder;
  private static final TypeDescriptor<KV<ByteString, Iterable<Mutation>>> BIGTABLE_WRITE_TYPE =
      new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {};

  @Before
  public void setup() throws Exception {
    service = new FakeBigtableService();
    defaultRead = defaultRead.withBigtableService(service);
    defaultWrite = defaultWrite.withBigtableService(service);
    bigtableCoder = TestPipeline.create().getCoderRegistry().getCoder(BIGTABLE_WRITE_TYPE);
  }

  @Test
  public void testReadBuildsCorrectly() {
    BigtableIO.Read read =
        BigtableIO.read().withBigtableOptions(BIGTABLE_OPTIONS).withTableId("table");
    assertEquals("project", read.getBigtableOptions().getProjectId());
    assertEquals("cluster", read.getBigtableOptions().getClusterId());
    assertEquals("zone", read.getBigtableOptions().getZoneId());
    assertEquals("table", read.getTableId());
  }

  @Test
  public void testReadBuildsCorrectlyInDifferentOrder() {
    BigtableIO.Read read =
        BigtableIO.read().withTableId("table").withBigtableOptions(BIGTABLE_OPTIONS);
    assertEquals("project", read.getBigtableOptions().getProjectId());
    assertEquals("cluster", read.getBigtableOptions().getClusterId());
    assertEquals("zone", read.getBigtableOptions().getZoneId());
    assertEquals("table", read.getTableId());
  }

  @Test
  public void testWriteBuildsCorrectly() {
    BigtableIO.Write write =
        BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS).withTableId("table");
    assertEquals("table", write.getTableId());
    assertEquals("project", write.getBigtableOptions().getProjectId());
    assertEquals("zone", write.getBigtableOptions().getZoneId());
    assertEquals("cluster", write.getBigtableOptions().getClusterId());
  }

  @Test
  public void testWriteBuildsCorrectlyInDifferentOrder() {
    BigtableIO.Write write =
        BigtableIO.write().withTableId("table").withBigtableOptions(BIGTABLE_OPTIONS);
    assertEquals("cluster", write.getBigtableOptions().getClusterId());
    assertEquals("project", write.getBigtableOptions().getProjectId());
    assertEquals("zone", write.getBigtableOptions().getZoneId());
    assertEquals("table", write.getTableId());
  }

  @Test
  public void testWriteValidationFailsMissingTable() {
    BigtableIO.Write write = BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS);

    thrown.expect(IllegalArgumentException.class);

    write.validate(null);
  }

  @Test
  public void testWriteValidationFailsMissingOptions() {
    BigtableIO.Write write = BigtableIO.write().withTableId("table");

    thrown.expect(IllegalArgumentException.class);

    write.validate(null);
  }

  /** Helper function to make a single row mutation to be written. */
  private static KV<ByteString, Iterable<Mutation>> makeWrite(String key, String value) {
    ByteString rowKey = ByteString.copyFromUtf8(key);
    Iterable<Mutation> mutations =
        ImmutableList.of(
            Mutation.newBuilder()
                .setSetCell(SetCell.newBuilder().setValue(ByteString.copyFromUtf8(value)))
                .build());
    return KV.of(rowKey, mutations);
  }

  /** Helper function to make a single bad row mutation (no set cell). */
  private static KV<ByteString, Iterable<Mutation>> makeBadWrite(String key) {
    Iterable<Mutation> mutations = ImmutableList.of(Mutation.newBuilder().build());
    return KV.of(ByteString.copyFromUtf8(key), mutations);
  }

  /** Tests that when reading from a non-existent table, the read fails. */
  @Test
  public void testReadingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withBigtableService(service);

    // Exception will be thrown by read.validate() when read is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    TestPipeline.create().apply(read);
  }

  /** Tests that when reading from an empty table, the read succeeds. */
  @Test
  public void testReadingEmptyTable() throws Exception {
    final String table = "TEST-EMPTY-TABLE";
    service.createTable(table);

    TestPipeline p = TestPipeline.create();
    PCollection<Row> rows = p.apply(defaultRead.withTableId(table));
    PAssert.that(rows).empty();

    p.run();
    logged.verifyInfo(String.format("Closing reader after reading 0 records."));
  }

  /** Tests reading all rows from a table. */
  @Test
  public void testReading() throws Exception {
    final String table = "TEST-MANY-ROWS-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);

    TestPipeline p = TestPipeline.create();
    PCollection<Row> rows = p.apply(defaultRead.withTableId(table));
    PAssert.that(rows).containsInAnyOrder(testRows);

    p.run();
    logged.verifyInfo(String.format("Closing reader after reading %d records.", numRows));
  }

  /** A {@link Predicate} that a {@link Row Row's} key matches the given regex. */
  private static class KeyMatchesRegex implements Predicate<ByteString> {
    private final String regex;

    public KeyMatchesRegex(String regex) {
      this.regex = regex;
    }

    @Override
    public boolean apply(@Nullable ByteString input) {
      verifyNotNull(input, "input");
      return input.toStringUtf8().matches(regex);
    }
  }

  /** Tests reading all rows using a filter. */
  @Test
  public void testReadingWithFilter() throws Exception {
    final String table = "TEST-FILTER-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    String regex = ".*17.*";
    final KeyMatchesRegex keyPredicate = new KeyMatchesRegex(regex);
    Iterable<Row> filteredRows =
        Iterables.filter(
            testRows,
            new Predicate<Row>() {
              @Override
              public boolean apply(@Nullable Row input) {
                verifyNotNull(input, "input");
                return keyPredicate.apply(input.getKey());
              }
            });

    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(regex)).build();

    TestPipeline p = TestPipeline.create();
    PCollection<Row> rows = p.apply(defaultRead.withTableId(table).withRowFilter(filter));
    PAssert.that(rows).containsInAnyOrder(filteredRows);

    p.run();
  }

  /**
   * Tests dynamic work rebalancing exhaustively.
   *
   * <p>Because this test runs so slowly, it is disabled by default. Re-run when changing the
   * {@link BigtableIO.Read} implementation.
   */
  @Ignore("Slow. Rerun when changing the implementation.")
  @Test
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final String table = "TEST-FEW-ROWS-SPLIT-EXHAUSTIVE-TABLE";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(service, table, null, service.getTableRange(table), null);
    assertSplitAtFractionExhaustive(source, null);
  }

  /**
   * Unit tests of splitAtFraction.
   */
  @Test
  public void testReadingSplitAtFraction() throws Exception {
    final String table = "TEST-SPLIT-AT-FRACTION";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(service, table, null, service.getTableRange(table), null);
    // With 0 items read, all split requests will fail.
    assertSplitAtFractionFails(source, 0, 0.1, null /* options */);
    assertSplitAtFractionFails(source, 0, 1.0, null /* options */);
    // With 1 items read, all split requests past 1/10th will succeed.
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.333, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.666, null /* options */);
    // With 3 items read, all split requests past 3/10ths will succeed.
    assertSplitAtFractionFails(source, 3, 0.2, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 0.571, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 3, 0.9, null /* options */);
    // With 6 items read, all split requests past 6/10ths will succeed.
    assertSplitAtFractionFails(source, 6, 0.5, null /* options */);
    assertSplitAtFractionSucceedsAndConsistent(source, 6, 0.7, null /* options */);
  }

  /** Tests reading all rows from a split table. */
  @Test
  public void testReadingWithSplits() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 1500;
    final int numSamples = 10;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(service, table, null /*filter*/, ByteKeyRange.ALL_KEYS, null /*size*/);
    List<BigtableSource> splits =
        source.splitIntoBundles(numRows * bytesPerRow / numSamples, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /** Tests reading all rows from a sub-split table. */
  @Test
  public void testReadingWithSubSplits() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 1000;
    final int numSamples = 10;
    final int numSplits = 20;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(service, table, null /*filter*/, ByteKeyRange.ALL_KEYS, null /*size*/);
    List<BigtableSource> splits = source.splitIntoBundles(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /** Tests reading all rows from a sub-split table. */
  @Test
  public void testReadingWithFilterAndSubSplits() throws Exception {
    final String table = "TEST-FILTER-SUB-SPLITS";
    final int numRows = 1700;
    final int numSamples = 10;
    final int numSplits = 20;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Generate source and split it.
    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(".*17.*")).build();
    BigtableSource source =
        new BigtableSource(service, table, filter, ByteKeyRange.ALL_KEYS, null /*size*/);
    List<BigtableSource> splits = source.splitIntoBundles(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  @Test
  public void testReadingDisplayData() {
    RowFilter rowFilter = RowFilter.newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8("foo.*"))
        .build();

    BigtableIO.Read read = BigtableIO.read()
        .withBigtableOptions(BIGTABLE_OPTIONS)
        .withTableId("fooTable")
        .withRowFilter(rowFilter);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("tableId", "fooTable"));
    assertThat(displayData, hasDisplayItem("rowFilter", rowFilter.toString()));

    // BigtableIO adds user-agent to options; assert only on key and not value.
    assertThat(displayData, hasDisplayItem("bigtableOptions"));
  }

  /** Tests that a record gets written to the service and messages are logged. */
  @Test
  public void testWriting() throws Exception {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    TestPipeline p = TestPipeline.create();
    p.apply("single row", Create.of(makeWrite(key, value)).withCoder(bigtableCoder))
        .apply("write", defaultWrite.withTableId(table));
    p.run();

    logged.verifyInfo("Wrote 1 records");

    assertEquals(1, service.tables.size());
    assertNotNull(service.getTable(table));
    Map<ByteString, ByteString> rows = service.getTable(table);
    assertEquals(1, rows.size());
    assertEquals(ByteString.copyFromUtf8(value), rows.get(ByteString.copyFromUtf8(key)));
  }

  /** Tests that when writing to a non-existent table, the write fails. */
  @Test
  public void testWritingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
        TestPipeline.create().apply(Create.<KV<ByteString, Iterable<Mutation>>>of());

    // Exception will be thrown by write.validate() when write is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    emptyInput.apply("write", defaultWrite.withTableId(table));
  }

  /** Tests that when writing an element fails, the write fails. */
  @Test
  public void testWritingFailsBadElement() throws Exception {
    final String table = "TEST-TABLE";
    final String key = "KEY";
    service.createTable(table);

    TestPipeline p = TestPipeline.create();
    p.apply(Create.of(makeBadWrite(key)).withCoder(bigtableCoder))
        .apply(defaultWrite.withTableId(table));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(Matchers.<Throwable>instanceOf(IOException.class));
    thrown.expectMessage("At least 1 errors occurred writing to Bigtable. First 1 errors:");
    thrown.expectMessage("Error mutating row " + key + " with mutations []: cell value missing");
    p.run();
  }

  @Test
  public void testWritingDisplayData() {
    BigtableIO.Write write = BigtableIO.write()
        .withTableId("fooTable")
        .withBigtableOptions(BIGTABLE_OPTIONS);

    DisplayData displayData = DisplayData.from(write);
    assertThat(displayData, hasDisplayItem("tableId", "fooTable"));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////
  private static final String COLUMN_FAMILY_NAME = "family";
  private static final ByteString COLUMN_NAME = ByteString.copyFromUtf8("column");
  private static final Column TEST_COLUMN = Column.newBuilder().setQualifier(COLUMN_NAME).build();
  private static final Family TEST_FAMILY = Family.newBuilder().setName(COLUMN_FAMILY_NAME).build();

  /** Helper function that builds a {@link Row} in a test table that could be returned by read. */
  private static Row makeRow(ByteString key, ByteString value) {
    // Build the currentRow and return true.
    Column.Builder newColumn = TEST_COLUMN.toBuilder().addCells(Cell.newBuilder().setValue(value));
    return Row.newBuilder()
        .setKey(key)
        .addFamilies(TEST_FAMILY.toBuilder().addColumns(newColumn))
        .build();
  }

  /** Helper function to create a table and return the rows that it created. */
  private static List<Row> makeTableData(String tableId, int numRows) {
    service.createTable(tableId);
    Map<ByteString, ByteString> testData = service.getTable(tableId);

    List<Row> testRows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", i));
      ByteString value = ByteString.copyFromUtf8(String.format("value%09d", i));
      testData.put(key, value);
      testRows.add(makeRow(key, value));
    }

    return testRows;
  }


  /**
   * A {@link BigtableService} implementation that stores tables and their contents in memory.
   */
  private static class FakeBigtableService implements BigtableService {
    private final Map<String, SortedMap<ByteString, ByteString>> tables = new HashMap<>();
    private final Map<String, List<SampleRowKeysResponse>> sampleRowKeys = new HashMap<>();

    @Nullable
    public SortedMap<ByteString, ByteString> getTable(String tableId) {
      return tables.get(tableId);
    }

    public ByteKeyRange getTableRange(String tableId) {
      verifyTableExists(tableId);
      SortedMap<ByteString, ByteString> data = tables.get(tableId);
      return ByteKeyRange.of(ByteKey.of(data.firstKey()), ByteKey.of(data.lastKey()));
    }

    public void createTable(String tableId) {
      tables.put(tableId, new TreeMap<ByteString, ByteString>(new ByteStringComparator()));
    }

    @Override
    public boolean tableExists(String tableId) {
      return tables.containsKey(tableId);
    }

    public void verifyTableExists(String tableId) {
      checkArgument(tableExists(tableId), "Table %s does not exist", tableId);
    }

    @Override
    public FakeBigtableReader createReader(BigtableSource source) {
      return new FakeBigtableReader(source);
    }

    @Override
    public FakeBigtableWriter openForWriting(String tableId) {
      return new FakeBigtableWriter(tableId);
    }

    @Override
    public List<SampleRowKeysResponse> getSampleRowKeys(BigtableSource source) {
      List<SampleRowKeysResponse> samples = sampleRowKeys.get(source.getTableId());
      checkArgument(samples != null, "No samples found for table %s", source.getTableId());
      return samples;
    }

    /** Sets up the sample row keys for the specified table. */
    void setupSampleRowKeys(String tableId, int numSamples, long bytesPerRow) {
      verifyTableExists(tableId);
      checkArgument(numSamples > 0, "Number of samples must be positive: %s", numSamples);
      checkArgument(bytesPerRow > 0, "Bytes/Row must be positive: %s", bytesPerRow);

      ImmutableList.Builder<SampleRowKeysResponse> ret = ImmutableList.builder();
      SortedMap<ByteString, ByteString> rows = getTable(tableId);
      int currentSample = 1;
      int rowsSoFar = 0;
      for (Map.Entry<ByteString, ByteString> entry : rows.entrySet()) {
        if (((double) rowsSoFar) / rows.size() >= ((double) currentSample) / numSamples) {
          // add the sample with the total number of bytes in the table before this key.
          ret.add(
              SampleRowKeysResponse.newBuilder()
                  .setRowKey(entry.getKey())
                  .setOffsetBytes(rowsSoFar * bytesPerRow)
                  .build());
          // Move on to next sample
          currentSample++;
        }
        ++rowsSoFar;
      }

      // Add the last sample indicating the end of the table, with all rows before it.
      ret.add(SampleRowKeysResponse.newBuilder().setOffsetBytes(rows.size() * bytesPerRow).build());
      sampleRowKeys.put(tableId, ret.build());
    }
  }

  /**
   * A {@link BigtableService.Reader} implementation that reads from the static instance of
   * {@link FakeBigtableService} stored in {@link #service}.
   *
   * <p>This reader does not support {@link RowFilter} objects.
   */
  private static class FakeBigtableReader implements BigtableService.Reader {
    private final BigtableSource source;
    private Iterator<Map.Entry<ByteString, ByteString>> rows;
    private Row currentRow;
    private Predicate<ByteString> filter;

    public FakeBigtableReader(BigtableSource source) {
      this.source = source;
      if (source.getRowFilter() == null) {
        filter = Predicates.alwaysTrue();
      } else {
        ByteString keyRegex = source.getRowFilter().getRowKeyRegexFilter();
        checkArgument(!keyRegex.isEmpty(), "Only RowKeyRegexFilter is supported");
        filter = new KeyMatchesRegex(keyRegex.toStringUtf8());
      }
      service.verifyTableExists(source.getTableId());
    }

    @Override
    public boolean start() {
      rows = service.tables.get(source.getTableId()).entrySet().iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      // Loop until we find a row in range, or reach the end of the iterator.
      Map.Entry<ByteString, ByteString> entry = null;
      while (rows.hasNext()) {
        entry = rows.next();
        if (!filter.apply(entry.getKey())
            || !source.getRange().containsKey(ByteKey.of(entry.getKey()))) {
          // Does not match row filter or does not match source range. Skip.
          entry = null;
          continue;
        }
        // Found a row inside this source's key range, stop.
        break;
      }

      // Return false if no more rows.
      if (entry == null) {
        currentRow = null;
        return false;
      }

      // Set the current row and return true.
      currentRow = makeRow(entry.getKey(), entry.getValue());
      return true;
    }

    @Override
    public Row getCurrentRow() {
      if (currentRow == null) {
        throw new NoSuchElementException();
      }
      return currentRow;
    }

    @Override
    public void close() {
      rows = null;
      currentRow = null;
    }
  }

  /**
   * A {@link BigtableService.Writer} implementation that writes to the static instance of
   * {@link FakeBigtableService} stored in {@link #service}.
   *
   * <p>This writer only supports {@link Mutation Mutations} that consist only of {@link SetCell}
   * entries. The column family in the {@link SetCell} is ignored; only the value is used.
   *
   * <p>When no {@link SetCell} is provided, the write will fail and this will be exposed via an
   * exception on the returned {@link ListenableFuture}.
   */
  private static class FakeBigtableWriter implements BigtableService.Writer {
    private final String tableId;

    public FakeBigtableWriter(String tableId) {
      this.tableId = tableId;
    }

    @Override
    public ListenableFuture<Empty> writeRecord(KV<ByteString, Iterable<Mutation>> record) {
      service.verifyTableExists(tableId);
      Map<ByteString, ByteString> table = service.getTable(tableId);
      ByteString key = record.getKey();
      for (Mutation m : record.getValue()) {
        SetCell cell = m.getSetCell();
        if (cell.getValue().isEmpty()) {
          return Futures.immediateFailedCheckedFuture(new IOException("cell value missing"));
        }
        table.put(key, cell.getValue());
      }
      return Futures.immediateFuture(Empty.getDefaultInstance());
    }

    @Override
    public void close() {}
  }

  /** A serializable comparator for ByteString. Used to make row samples. */
  private static final class ByteStringComparator implements Comparator<ByteString>, Serializable {
    @Override
    public int compare(ByteString o1, ByteString o2) {
      return ByteKey.of(o1).compareTo(ByteKey.of(o2));
    }
  }
}

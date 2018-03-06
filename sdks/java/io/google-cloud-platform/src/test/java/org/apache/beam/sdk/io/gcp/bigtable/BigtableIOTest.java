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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Verify.verifyNotNull;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasLabel;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.bigtable.v2.SampleRowKeysResponse;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import com.google.cloud.bigtable.config.CredentialOptions.CredentialType;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.gcp.auth.TestCredential;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link BigtableIO}.
 */
@RunWith(JUnit4.class)
public class BigtableIOTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigtableIO.class);

  static final ValueProvider<String> NOT_ACCESSIBLE_VALUE = new ValueProvider<String>() {
    @Override
    public String get() {
      throw new IllegalStateException("Value is not accessible");
    }

    @Override
    public boolean isAccessible() {
      return false;
    }
  };

  private static BigtableConfig config;
  private static FakeBigtableService service;
  private static final BigtableOptions BIGTABLE_OPTIONS =
      new BigtableOptions.Builder()
          .setProjectId("options_project")
          .setInstanceId("options_instance")
          .build();
  private static BigtableIO.Read defaultRead =
      BigtableIO.read().withInstanceId("instance").withProjectId("project");
  private static BigtableIO.Write defaultWrite =
      BigtableIO.write().withInstanceId("instance").withProjectId("project");
  private Coder<KV<ByteString, Iterable<Mutation>>> bigtableCoder;
  private static final TypeDescriptor<KV<ByteString, Iterable<Mutation>>> BIGTABLE_WRITE_TYPE =
      new TypeDescriptor<KV<ByteString, Iterable<Mutation>>>() {};

  private static final SerializableFunction<BigtableOptions.Builder, BigtableOptions.Builder>
      PORT_CONFIGURATOR = input -> input.setPort(1234);

  @Before
  public void setup() throws Exception {
    service = new FakeBigtableService();
    defaultRead = defaultRead.withBigtableService(service);
    defaultWrite = defaultWrite.withBigtableService(service);
    bigtableCoder = p.getCoderRegistry().getCoder(BIGTABLE_WRITE_TYPE);

    config = BigtableConfig.builder()
      .setValidate(true)
      .setBigtableService(service)
      .build();
  }

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  @Test
  public void testReadBuildsCorrectly() {
    BigtableIO.Read read =
        BigtableIO.read().withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project")
            .withBigtableOptionsConfigurator(PORT_CONFIGURATOR);
    assertEquals("options_project", read.getBigtableOptions().getProjectId());
    assertEquals("options_instance", read.getBigtableOptions().getInstanceId());
    assertEquals("instance", read.getBigtableConfig().getInstanceId().get());
    assertEquals("project", read.getBigtableConfig().getProjectId().get());
    assertEquals("table", read.getTableId());
    assertEquals(PORT_CONFIGURATOR, read.getBigtableConfig().getBigtableOptionsConfigurator());
  }

  @Test
  public void testReadValidationFailsMissingTable() {
    BigtableIO.Read read = BigtableIO.read().withBigtableOptions(BIGTABLE_OPTIONS);

    thrown.expect(IllegalArgumentException.class);
    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingInstanceId() {
    BigtableIO.Read read = BigtableIO.read().withTableId("table")
        .withProjectId("project")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingProjectId() {
    BigtableIO.Read read = BigtableIO.read().withTableId("table")
        .withInstanceId("instance")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.Read read = BigtableIO.read()
        .withTableId("table")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testWriteBuildsCorrectly() {
    BigtableIO.Write write =
        BigtableIO.write().withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project");
    assertEquals("table", write.getBigtableConfig().getTableId().get());
    assertEquals("options_project", write.getBigtableOptions().getProjectId());
    assertEquals("options_instance", write.getBigtableOptions().getInstanceId());
    assertEquals("instance", write.getBigtableConfig().getInstanceId().get());
    assertEquals("project", write.getBigtableConfig().getProjectId().get());
  }

  @Test
  public void testWriteValidationFailsMissingInstanceId() {
    BigtableIO.Write write = BigtableIO.write().withTableId("table")
        .withProjectId("project")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingProjectId() {
    BigtableIO.Write write = BigtableIO.write().withTableId("table")
        .withInstanceId("instance")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.Write write = BigtableIO.write()
        .withTableId("table")
        .withBigtableOptions(new BigtableOptions.Builder().build());

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingOptionsAndInstanceAndProject() {
    BigtableIO.Write write = BigtableIO.write().withTableId("table");

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
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

  /** Tests that credentials are used from PipelineOptions if not supplied by BigtableOptions. */
  @Test
  public void testUsePipelineOptionsCredentialsIfNotSpecifiedInBigtableOptions() throws Exception {
    BigtableOptions options = BIGTABLE_OPTIONS.toBuilder()
        .setCredentialOptions(CredentialOptions.defaultCredentials())
        .build();
    GcpOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);
    pipelineOptions.setGcpCredential(new TestCredential());
    BigtableService readService = BigtableIO.read()
        .withBigtableOptions(options)
        .withTableId("TEST-TABLE")
        .getBigtableConfig()
        .getBigtableService(pipelineOptions);
    BigtableService writeService = BigtableIO.write()
        .withBigtableOptions(options)
        .withTableId("TEST-TABLE")
        .getBigtableConfig()
        .getBigtableService(pipelineOptions);
    assertEquals(CredentialType.SuppliedCredentials,
        readService.getBigtableOptions().getCredentialOptions().getCredentialType());
    assertEquals(CredentialType.SuppliedCredentials,
        writeService.getBigtableOptions().getCredentialOptions().getCredentialType());
  }

  /** Tests that credentials are not used from PipelineOptions if supplied by BigtableOptions. */
  @Test
  public void testDontUsePipelineOptionsCredentialsIfSpecifiedInBigtableOptions() throws Exception {
    BigtableOptions options = BIGTABLE_OPTIONS.toBuilder()
        .setCredentialOptions(CredentialOptions.nullCredential())
        .build();
    GcpOptions pipelineOptions = PipelineOptionsFactory.as(GcpOptions.class);
    pipelineOptions.setGcpCredential(new TestCredential());
    BigtableService readService = BigtableIO.read()
        .withBigtableOptions(options)
        .withTableId("TEST-TABLE")
        .getBigtableConfig()
        .getBigtableService(pipelineOptions);
    BigtableService writeService = BigtableIO.write()
        .withBigtableOptions(options)
        .withTableId("TEST-TABLE")
        .getBigtableConfig()
        .getBigtableService(pipelineOptions);
    assertEquals(CredentialType.None,
        readService.getBigtableOptions().getCredentialOptions().getCredentialType());
    assertEquals(CredentialType.None,
        writeService.getBigtableOptions().getCredentialOptions().getCredentialType());
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

    p.apply(read);
    p.run();
  }

  /** Tests that when reading from an empty table, the read succeeds. */
  @Test
  public void testReadingEmptyTable() throws Exception {
    final String table = "TEST-EMPTY-TABLE";
    service.createTable(table);
    service.setupSampleRowKeys(table, 1, 1L);

    runReadTest(defaultRead.withTableId(table), new ArrayList<>());
    logged.verifyInfo("Closing reader after reading 0 records.");
  }

  /** Tests reading all rows from a table. */
  @Test
  public void testReading() throws Exception {
    final String table = "TEST-MANY-ROWS-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);

    service.setupSampleRowKeys(table, 3, 1000L);
    runReadTest(defaultRead.withTableId(table), testRows);
    logged.verifyInfo(String.format("Closing reader after reading %d records.", numRows / 3));
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

  private static List<Row> filterToRange(List<Row> rows, final ByteKeyRange range) {
    return filterToRanges(rows, ImmutableList.of(range));
  }

  private static List<Row> filterToRanges(List<Row> rows, final List<ByteKeyRange> ranges) {
    return Lists.newArrayList(
        rows.stream()
            .filter(
                input -> {
                  verifyNotNull(input, "input");
                  for (ByteKeyRange range : ranges) {
                    if (range.containsKey(makeByteKey(input.getKey()))) {
                      return true;
                    }
                  }
                  return false;
                })
            .collect(Collectors.toList()));
  }

  private void runReadTest(BigtableIO.Read read, List<Row> expected) {
    PCollection<Row> rows = p.apply(read.getTableId() + "_" + read.getKeyRanges(), read);
    PAssert.that(rows).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted
   * range [] and that some properties hold across them.
   */
  @Test
  public void testReadingWithKeyRange() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey = ByteKey.copyFrom("key000000100".getBytes());
    ByteKey endKey = ByteKey.copyFrom("key000000300".getBytes());

    service.setupSampleRowKeys(table, numRows / 10, "key000000100".length());
    // Test prefix: [beginning, startKey).
    final ByteKeyRange prefixRange = ByteKeyRange.ALL_KEYS.withEndKey(startKey);
    List<Row> prefixRows = filterToRange(testRows, prefixRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(prefixRange), prefixRows);

    // Test suffix: [startKey, end).
    final ByteKeyRange suffixRange = ByteKeyRange.ALL_KEYS.withStartKey(startKey);
    List<Row> suffixRows = filterToRange(testRows, suffixRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(suffixRange), suffixRows);

    // Test restricted range: [startKey, endKey).
    final ByteKeyRange middleRange = ByteKeyRange.of(startKey, endKey);
    List<Row> middleRows = filterToRange(testRows, middleRange);
    runReadTest(defaultRead.withTableId(table).withKeyRange(middleRange), middleRows);

    //////// Size and content sanity checks //////////

    // Prefix, suffix, middle should be non-trivial (non-zero,non-all).
    assertThat(prefixRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
    assertThat(suffixRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
    assertThat(middleRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));

    // Prefix + suffix should be exactly all rows.
    List<Row> union = Lists.newArrayList(prefixRows);
    union.addAll(suffixRows);
    assertThat("prefix + suffix = total", union, containsInAnyOrder(testRows.toArray(new Row[]{})));

    // Suffix should contain the middle.
    assertThat(suffixRows, hasItems(middleRows.toArray(new Row[]{})));
  }

  /**
   * Tests reading three key ranges with one read.
   */
  @Test
  public void testReadingWithKeyRanges() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 11;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey1 = ByteKey.copyFrom("key000000001".getBytes());
    ByteKey endKey1 = ByteKey.copyFrom("key000000003".getBytes());
    ByteKey startKey2 = ByteKey.copyFrom("key000000004".getBytes());
    ByteKey endKey2 = ByteKey.copyFrom("key000000007".getBytes());
    ByteKey startKey3 = ByteKey.copyFrom("key000000008".getBytes());
    ByteKey endKey3 = ByteKey.copyFrom("key000000009".getBytes());

    service.setupSampleRowKeys(table, numRows / 10, "key000000001".length());

    final ByteKeyRange range1 = ByteKeyRange.of(startKey1, endKey1);
    final ByteKeyRange range2 = ByteKeyRange.of(startKey2, endKey2);
    final ByteKeyRange range3 = ByteKeyRange.of(startKey3, endKey3);
    List<ByteKeyRange> ranges = ImmutableList.of(range1, range2, range3);
    List<Row> rangeRows = filterToRanges(testRows, ranges);
    runReadTest(defaultRead.withTableId(table).withKeyRanges(ranges), rangeRows);

    // range rows should be non-trivial (non-zero,non-all).
    assertThat(rangeRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
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
        testRows
            .stream()
            .filter(
                input -> {
                  verifyNotNull(input, "input");
                  return keyPredicate.apply(input.getKey());
                })
            .collect(Collectors.toList());

    RowFilter filter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8(regex)).build();
    service.setupSampleRowKeys(table, 5, 10L);

    runReadTest(
        defaultRead.withTableId(table).withRowFilter(filter), Lists.newArrayList(filteredRows));
  }

  /**
   * Tests dynamic work rebalancing exhaustively.
   */
  @Test
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final String table = "TEST-FEW-ROWS-SPLIT-EXHAUSTIVE-TABLE";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)), null,
          Arrays.asList(service.getTableRange(table)), null);
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
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
          null, Arrays.asList(service.getTableRange(table)), null);
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
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
            null /*filter*/,
            Arrays.asList(ByteKeyRange.ALL_KEYS),
            null /*size*/);
    List<BigtableSource> splits =
        source.split(numRows * bytesPerRow / numSamples, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /** Tests reading all rows from a split table with several key ranges. */
  @Test
  public void testReadingWithSplitsWithSeveralKeyRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE-MULTIPLE-RANGES";
    final int numRows = 1500;
    final int numSamples = 10;
    // Two more splits are generated because of the split keys at 500 and 1000.
    // E.g. the split [450, 600) becomes [450, 500) and [500, 600).
    final int numSplits = 12;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    ByteKey splitKey1 = ByteKey.copyFrom("key000000500".getBytes());
    ByteKey splitKey2 = ByteKey.copyFrom("key000001000".getBytes());

    ByteKeyRange tableRange = service.getTableRange(table);
    List<ByteKeyRange> keyRanges = Arrays.asList(
        tableRange.withEndKey(splitKey1),
        tableRange.withStartKey(splitKey1).withEndKey(splitKey2),
        tableRange.withStartKey(splitKey2));
    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
            null /*filter*/,
            keyRanges,
            null /*size*/);
    BigtableSource referenceSource =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
            null /*filter*/,
            ImmutableList.of(service.getTableRange(table)),
            null /*size*/);
    List<BigtableSource> splits = // 10,000
        source.split(numRows * bytesPerRow / numSamples, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(referenceSource, splits, null /* options */);
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
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
        null /*filter*/,
        Arrays.asList(ByteKeyRange.ALL_KEYS),
        null /*size*/);
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /** Tests reading all rows from a sub-split table with several key ranges. */
  @Test
  public void testReadingWithSubSplitsWithSeveralKeyRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE-MULTIPLE-RANGES";
    final int numRows = 1000;
    final int numSamples = 10;
    final int numSplits = 20;
    // We expect 24 splits instead of 20 due to the multiple ranges. For a key of 330 separating
    // the multiple ranges, first the [300, 330) range is subsplit into two (since numSplits is
    // twice numSamples), so we get [300, 315) and [315, 330). Then, the [330, 400) range is also
    // split into two, resulting in [330, 365) and [365, 400). These ranges would instead be
    // [300, 350) and [350, 400) if this source was one range. Thus, each extra range adds two
    // resulting splits.
    final int expectedNumSplits = 24;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    ByteKey splitKey1 = ByteKey.copyFrom("key000000330".getBytes());
    ByteKey splitKey2 = ByteKey.copyFrom("key000000730".getBytes());

    ByteKeyRange tableRange = service.getTableRange(table);
    List<ByteKeyRange> keyRanges = Arrays.asList(
        tableRange.withEndKey(splitKey1),
        tableRange.withStartKey(splitKey1).withEndKey(splitKey2),
        tableRange.withStartKey(splitKey2));
    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
            null /*filter*/,
            keyRanges,
            null /*size*/);
    BigtableSource referenceSource =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
            null /*filter*/,
            ImmutableList.of(service.getTableRange(table)),
            null /*size*/);
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(expectedNumSplits));
    assertSourcesEqualReferenceSource(referenceSource, splits, null /* options */);
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
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
          filter, Arrays.asList(ByteKeyRange.ALL_KEYS), null /*size*/);
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  @Test
  public void testReadingDisplayData() {
    RowFilter rowFilter = RowFilter.newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8("foo.*"))
        .build();

    ByteKeyRange keyRange = ByteKeyRange.ALL_KEYS.withEndKey(ByteKey.of(0xab, 0xcd));
    BigtableIO.Read read = BigtableIO.read()
        .withBigtableOptions(BIGTABLE_OPTIONS)
        .withTableId("fooTable")
        .withRowFilter(rowFilter)
        .withKeyRange(keyRange);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem(allOf(
        hasKey("tableId"),
        hasLabel("Bigtable Table Id"),
        hasValue("fooTable"))));

    assertThat(displayData, hasDisplayItem("rowFilter", rowFilter.toString()));

    assertThat(displayData, hasDisplayItem("keyRange 0", keyRange.toString()));

    // BigtableIO adds user-agent to options; assert only on key and not value.
    assertThat(displayData, hasDisplayItem("bigtableOptions"));
  }

  @Test
  public void testReadingPrimitiveDisplayData() throws IOException, InterruptedException {
    final String table = "fooTable";
    service.createTable(table);

    RowFilter rowFilter = RowFilter.newBuilder()
        .setRowKeyRegexFilter(ByteString.copyFromUtf8("foo.*"))
        .build();

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    BigtableIO.Read read = BigtableIO.read()
        .withBigtableOptions(BIGTABLE_OPTIONS)
        .withTableId(table)
        .withRowFilter(rowFilter)
        .withBigtableService(service);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("BigtableIO.Read should include the table id in its primitive display data",
        displayData, Matchers.hasItem(hasDisplayItem("tableId")));
    assertThat("BigtableIO.Read should include the row filter, if it exists, in its primitive "
        + "display data", displayData, Matchers.hasItem(hasDisplayItem("rowFilter")));
  }

  @Test
  public void testReadWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Read read = BigtableIO.read()
        .withBigtableOptions(BIGTABLE_OPTIONS)
        .withTableId(table)
        .withBigtableService(service)
        .withoutValidation();

    // validate() will throw if withoutValidation() isn't working
    read.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testWriteWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Write write = BigtableIO.write()
        .withBigtableOptions(BIGTABLE_OPTIONS)
        .withTableId(table)
        .withBigtableService(service)
        .withoutValidation();

    // validate() will throw if withoutValidation() isn't working
    write.validate(TestPipeline.testingPipelineOptions());
  }

  /** Tests that a record gets written to the service and messages are logged. */
  @Test
  public void testWriting() throws Exception {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    p.apply("single row", Create.of(makeWrite(key, value)).withCoder(bigtableCoder))
        .apply("write", defaultWrite.withTableId(table));
    p.run();

    logged.verifyDebug("Wrote 1 records");

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
        p.apply(
            Create.empty(
                KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)))));

    // Exception will be thrown by write.validate() when writeToDynamic is applied.
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Table %s does not exist", table));

    emptyInput.apply("write", defaultWrite.withTableId(table));
    p.run();
  }

  /** Tests that when writing to a non-existent table, the write fails. */
  @Test
  public void testTableCheckIgnoredWhenCanNotAccessConfig() throws Exception {
    PCollection<KV<ByteString, Iterable<Mutation>>> emptyInput =
      p.apply(
        Create.empty(
          KvCoder.of(ByteStringCoder.of(), IterableCoder.of(ProtoCoder.of(Mutation.class)))));

    emptyInput.apply("write", defaultWrite.withTableId(NOT_ACCESSIBLE_VALUE));
    p.run();
  }

  /** Tests that when writing an element fails, the write fails. */
  @Test
  public void testWritingFailsBadElement() throws Exception {
    final String table = "TEST-TABLE";
    final String key = "KEY";
    service.createTable(table);

    p.apply(Create.of(makeBadWrite(key)).withCoder(bigtableCoder))
        .apply(defaultWrite.withTableId(table));

    thrown.expect(PipelineExecutionException.class);
    thrown.expectCause(Matchers.instanceOf(IOException.class));
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

  @Test
  public void testGetSplitPointsConsumed() throws Exception {
    final String table = "TEST-TABLE";
    final int numRows = 100;
    int splitPointsConsumed = 0;

    makeTableData(table, numRows);

    BigtableSource source =
        new BigtableSource(config.withTableId(ValueProvider.StaticValueProvider.of(table)),
          null, Arrays.asList(ByteKeyRange.ALL_KEYS), null);

    BoundedReader<Row> reader = source.createReader(TestPipeline.testingPipelineOptions());

    reader.start();
    // Started, 0 split points consumed
    assertEquals("splitPointsConsumed starting",
        splitPointsConsumed, reader.getSplitPointsConsumed());

    // Split points consumed increases for each row read
    while (reader.advance()) {
      assertEquals("splitPointsConsumed advancing",
          ++splitPointsConsumed, reader.getSplitPointsConsumed());
    }

    // Reader marked as done, 100 split points consumed
    assertEquals("splitPointsConsumed done", numRows, reader.getSplitPointsConsumed());

    reader.close();
  }

  @Test
  public void testReadWithBigTableOptionsSetsRetryOptions() {
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    RetryOptions.Builder retryOptionsBuilder = new RetryOptions.Builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder.setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Read read =
        BigtableIO.read().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = read.getBigtableOptions();
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(options.getRetryOptions(),
        Matchers.equalTo(retryOptionsBuilder.build()));
  }

  @Test
  public void testWriteWithBigTableOptionsSetsBulkOptionsAndRetryOptions() {
    final int maxInflightRpcs = 1;
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    BulkOptions.Builder bulkOptionsBuilder = new BulkOptions.Builder();
    bulkOptionsBuilder.setMaxInflightRpcs(maxInflightRpcs);

    RetryOptions.Builder retryOptionsBuilder = new RetryOptions.Builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder.setBulkOptions(bulkOptionsBuilder.build())
        .setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Write write =
        BigtableIO.write().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = write.getBigtableOptions();
    assertEquals(true, options.getBulkOptions().useBulkApi());
    assertEquals(maxInflightRpcs, options.getBulkOptions().getMaxInflightRpcs());
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(options.getBulkOptions(),
        Matchers.equalTo(bulkOptionsBuilder
            .setUseBulkApi(true)
            .build()));
    assertThat(options.getRetryOptions(),
        Matchers.equalTo(retryOptionsBuilder.build()));
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

    @Override
    public BigtableOptions getBigtableOptions() {
      return null;
    }

    @Nullable
    public SortedMap<ByteString, ByteString> getTable(String tableId) {
      return tables.get(tableId);
    }

    public ByteKeyRange getTableRange(String tableId) {
      verifyTableExists(tableId);
      SortedMap<ByteString, ByteString> data = tables.get(tableId);
      return ByteKeyRange.of(makeByteKey(data.firstKey()), makeByteKey(data.lastKey()));
    }

    public void createTable(String tableId) {
      tables.put(tableId, new TreeMap<>(new ByteStringComparator()));
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
      List<SampleRowKeysResponse> samples = sampleRowKeys.get(source.getTableId().get());
      checkNotNull(samples, "No samples found for table %s", source.getTableId().get());
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
      service.verifyTableExists(source.getTableId().get());
    }

    @Override
    public boolean start() {
      rows = service.tables.get(source.getTableId().get()).entrySet().iterator();
      return advance();
    }

    @Override
    public boolean advance() {
      // Loop until we find a row in range, or reach the end of the iterator.
      Map.Entry<ByteString, ByteString> entry = null;
      while (rows.hasNext()) {
        entry = rows.next();
        if (!filter.apply(entry.getKey())
            || !rangesContainsKey(source.getRanges(), makeByteKey(entry.getKey()))) {
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

    private boolean rangesContainsKey(List<ByteKeyRange> ranges, ByteKey key) {
      for (ByteKeyRange range : ranges) {
        if (range.containsKey(key)) {
          return true;
        }
      }
      return false;
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
   * exception on the returned {@link CompletionStage}.
   */
  private static class FakeBigtableWriter implements BigtableService.Writer {
    private final String tableId;

    public FakeBigtableWriter(String tableId) {
      this.tableId = tableId;
    }

    @Override
    public CompletionStage<MutateRowResponse> writeRecord(
        KV<ByteString, Iterable<Mutation>> record) {
      service.verifyTableExists(tableId);
      Map<ByteString, ByteString> table = service.getTable(tableId);
      ByteString key = record.getKey();
      for (Mutation m : record.getValue()) {
        SetCell cell = m.getSetCell();
        if (cell.getValue().isEmpty()) {
          CompletableFuture<MutateRowResponse> result = new CompletableFuture<>();
          result.completeExceptionally(new IOException("cell value missing"));
          return result;
        }
        table.put(key, cell.getValue());
      }
      return CompletableFuture.completedFuture(MutateRowResponse.getDefaultInstance());
    }

    @Override
    public void flush() {}

    @Override
    public void close() {}
  }

  /** A serializable comparator for ByteString. Used to make row samples. */
  private static final class ByteStringComparator implements Comparator<ByteString>, Serializable {
    @Override
    public int compare(ByteString o1, ByteString o2) {
      return makeByteKey(o1).compareTo(makeByteKey(o2));
    }
  }
}

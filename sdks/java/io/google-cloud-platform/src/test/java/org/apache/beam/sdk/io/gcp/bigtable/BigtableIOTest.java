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

import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSourcesEqualReferenceSource;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasLabel;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Verify.verifyNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.api.gax.rpc.ApiException;
import com.google.auto.value.AutoValue;
import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.MutateRowResponse;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.Row;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.BulkOptions;
import com.google.cloud.bigtable.config.RetryOptions;
import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.protobuf.ByteStringCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO.BigtableSource;
import org.apache.beam.sdk.io.range.ByteKey;
import org.apache.beam.sdk.io.range.ByteKeyRange;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipeline.PipelineRunMissingException;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicate;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Predicates;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link BigtableIO}. */
@RunWith(JUnit4.class)
public class BigtableIOTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ExpectedLogs logged = ExpectedLogs.none(BigtableIO.class);

  /** Read Options for testing. */
  public interface ReadOptions extends GcpOptions {
    @Description("The project that contains the table to export.")
    ValueProvider<String> getBigtableProject();

    @SuppressWarnings("unused")
    void setBigtableProject(ValueProvider<String> projectId);

    @Description("The Bigtable instance id that contains the table to export.")
    ValueProvider<String> getBigtableInstanceId();

    @SuppressWarnings("unused")
    void setBigtableInstanceId(ValueProvider<String> instanceId);

    @Description("The Bigtable table id to export.")
    ValueProvider<String> getBigtableTableId();

    @SuppressWarnings("unused")
    void setBigtableTableId(ValueProvider<String> tableId);
  }

  static final ValueProvider<String> NOT_ACCESSIBLE_VALUE =
      new ValueProvider<String>() {
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
      BigtableOptions.builder()
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

  private static final ValueProvider<List<ByteKeyRange>> ALL_KEY_RANGE =
      StaticValueProvider.of(Collections.singletonList(ByteKeyRange.ALL_KEYS));

  private FakeServiceFactory factory;

  private static BigtableServiceFactory.ConfigId configId;

  @Before
  public void setup() throws Exception {
    service = new FakeBigtableService();

    factory = new FakeServiceFactory(service);

    configId = factory.newId();

    defaultRead = defaultRead.withServiceFactory(factory);

    defaultWrite = defaultWrite.withServiceFactory(factory);

    bigtableCoder = p.getCoderRegistry().getCoder(BIGTABLE_WRITE_TYPE);

    config = BigtableConfig.builder().setValidate(true).build();
  }

  private static ByteKey makeByteKey(ByteString key) {
    return ByteKey.copyFrom(key.asReadOnlyByteBuffer());
  }

  @Test
  public void testReadBuildsCorrectly() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project")
            .withAppProfileId("app-profile")
            .withBigtableOptionsConfigurator(PORT_CONFIGURATOR);
    assertEquals("options_project", read.getBigtableOptions().getProjectId());
    assertEquals("options_instance", read.getBigtableOptions().getInstanceId());
    assertEquals("instance", read.getBigtableConfig().getInstanceId().get());
    assertEquals("project", read.getBigtableConfig().getProjectId().get());
    assertEquals("app-profile", read.getBigtableConfig().getAppProfileId().get());
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
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withProjectId("project")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingProjectId() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withInstanceId("instance")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.Read read =
        BigtableIO.read()
            .withTableId("table")
            .withBigtableOptions(BigtableOptions.builder().build());

    thrown.expect(IllegalArgumentException.class);

    read.expand(null);
  }

  @Test
  public void testReadWithRuntimeParametersValidationFailed() {
    ReadOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(ReadOptions.class);

    BigtableIO.Read read =
        BigtableIO.read()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("tableId was not supplied");

    p.apply(read);
  }

  @Test
  public void testReadWithRuntimeParametersValidationDisabled() {
    ReadOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(ReadOptions.class);

    BigtableIO.Read read =
        BigtableIO.read()
            .withoutValidation()
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());

    // Not running a pipeline therefore this is expected.
    thrown.expect(PipelineRunMissingException.class);

    p.apply(read);
  }

  @Test
  public void testReadWithReaderStartFailed() throws IOException {
    FailureBigtableService failureService =
        new FailureBigtableService(FailureOptions.builder().setFailAtStart(true).build());

    BigtableConfig failureConfig = BigtableConfig.builder().setValidate(true).build();
    final String table = "TEST-TABLE";
    final int numRows = 100;
    makeTableData(failureService, table, numRows);
    FakeServiceFactory failureFactory = new FakeServiceFactory(failureService);

    BigtableSource source =
        new BigtableSource(
            failureFactory,
            BigtableServiceFactory.ConfigId.create(),
            failureConfig,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null);
    BoundedReader<Row> reader = source.createReader(TestPipeline.testingPipelineOptions());

    thrown.expect(IOException.class);
    thrown.expectMessage("Fake IOException at start()");

    reader.start();
  }

  @Test
  public void testReadWithReaderAdvanceFailed() throws IOException {
    FailureBigtableService failureService =
        new FailureBigtableService(FailureOptions.builder().setFailAtAdvance(true).build());

    FakeServiceFactory failureFactory = new FakeServiceFactory(failureService);

    BigtableConfig failureConfig = BigtableConfig.builder().setValidate(true).build();
    final String table = "TEST-TABLE";
    final int numRows = 100;
    makeTableData(failureService, table, numRows);
    BigtableSource source =
        new BigtableSource(
            failureFactory,
            failureFactory.newId(),
            failureConfig,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null);
    BoundedReader<Row> reader = source.createReader(TestPipeline.testingPipelineOptions());

    thrown.expect(IOException.class);
    thrown.expectMessage("Fake IOException at advance()");

    reader.start();
    reader.advance();
  }

  @Test
  public void testWriteBuildsCorrectly() {
    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("table")
            .withInstanceId("instance")
            .withProjectId("project")
            .withAppProfileId("app-profile");
    assertEquals("table", write.getBigtableWriteOptions().getTableId().get());
    assertEquals("options_project", write.getBigtableOptions().getProjectId());
    assertEquals("options_instance", write.getBigtableOptions().getInstanceId());
    assertEquals("instance", write.getBigtableConfig().getInstanceId().get());
    assertEquals("project", write.getBigtableConfig().getProjectId().get());
    assertEquals("app-profile", write.getBigtableConfig().getAppProfileId().get());
  }

  @Test
  public void testWriteValidationFailsMissingInstanceId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withProjectId("project")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingProjectId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withInstanceId("instance")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingInstanceIdAndProjectId() {
    BigtableIO.WriteWithResults write =
        BigtableIO.write()
            .withTableId("table")
            .withBigtableOptions(BigtableOptions.builder().build())
            .withWriteResults();

    thrown.expect(IllegalArgumentException.class);

    write.expand(null);
  }

  @Test
  public void testWriteValidationFailsMissingOptionsAndInstanceAndProject() {
    BigtableIO.WriteWithResults write = BigtableIO.write().withTableId("table").withWriteResults();

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

  /** Tests that when reading from a non-existent table, the read fails. */
  @Test
  public void testReadingFailsTableDoesNotExist() throws Exception {
    final String table = "TEST-TABLE";

    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withServiceFactory(factory);

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
    PCollection<Row> rows =
        p.apply(read.getTableId() + "_" + read.getBigtableReadOptions().getKeyRanges(), read);
    PAssert.that(rows).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Tests reading all rows using key ranges. Tests a prefix [), a suffix (], and a restricted range
   * [] and that some properties hold across them.
   */
  @Test
  public void testReadingWithKeyRange() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey = ByteKey.copyFrom("key000000100".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey = ByteKey.copyFrom("key000000300".getBytes(StandardCharsets.UTF_8));

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
    assertThat(
        "prefix + suffix = total", union, containsInAnyOrder(testRows.toArray(new Row[] {})));

    // Suffix should contain the middle.
    assertThat(suffixRows, hasItems(middleRows.toArray(new Row[] {})));
  }

  /** Tests reading key ranges specified through a ValueProvider. */
  @Test
  public void testReadingWithRuntimeParameterizedKeyRange() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey = ByteKey.copyFrom("key000000100".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey = ByteKey.copyFrom("key000000300".getBytes(StandardCharsets.UTF_8));

    service.setupSampleRowKeys(table, numRows / 10, "key000000100".length());

    final ByteKeyRange middleRange = ByteKeyRange.of(startKey, endKey);
    List<Row> middleRows = filterToRange(testRows, middleRange);
    runReadTest(
        defaultRead
            .withTableId(table)
            .withKeyRanges(StaticValueProvider.of(Collections.singletonList(middleRange))),
        middleRows);

    assertThat(middleRows, allOf(hasSize(lessThan(numRows)), hasSize(greaterThan(0))));
  }

  /** Tests reading three key ranges with one read. */
  @Test
  public void testReadingWithKeyRanges() throws Exception {
    final String table = "TEST-KEY-RANGE-TABLE";
    final int numRows = 11;
    List<Row> testRows = makeTableData(table, numRows);
    ByteKey startKey1 = ByteKey.copyFrom("key000000001".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey1 = ByteKey.copyFrom("key000000003".getBytes(StandardCharsets.UTF_8));
    ByteKey startKey2 = ByteKey.copyFrom("key000000004".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey2 = ByteKey.copyFrom("key000000007".getBytes(StandardCharsets.UTF_8));
    ByteKey startKey3 = ByteKey.copyFrom("key000000008".getBytes(StandardCharsets.UTF_8));
    ByteKey endKey3 = ByteKey.copyFrom("key000000009".getBytes(StandardCharsets.UTF_8));

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
  public void testReadingWithFilter() {
    final String table = "TEST-FILTER-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    String regex = ".*17.*";
    final KeyMatchesRegex keyPredicate = new KeyMatchesRegex(regex);
    Iterable<Row> filteredRows =
        testRows.stream()
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

  /** Tests reading rows using a filter provided through ValueProvider. */
  @Test
  public void testReadingWithRuntimeParameterizedFilter() throws Exception {
    final String table = "TEST-FILTER-TABLE";
    final int numRows = 1001;
    List<Row> testRows = makeTableData(table, numRows);
    String regex = ".*17.*";
    final KeyMatchesRegex keyPredicate = new KeyMatchesRegex(regex);
    Iterable<Row> filteredRows =
        testRows.stream()
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
        defaultRead.withTableId(table).withRowFilter(StaticValueProvider.of(filter)),
        Lists.newArrayList(filteredRows));
  }
  /** Tests dynamic work rebalancing exhaustively. */
  @Test
  public void testReadingSplitAtFractionExhaustive() throws Exception {
    final String table = "TEST-FEW-ROWS-SPLIT-EXHAUSTIVE-TABLE";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(Collections.singletonList(service.getTableRange(table))))
                .build(),
            null);
    assertSplitAtFractionExhaustive(source, null);
  }

  /** Unit tests of splitAtFraction. */
  @Test
  public void testReadingSplitAtFraction() throws Exception {
    final String table = "TEST-SPLIT-AT-FRACTION";
    final int numRows = 10;
    final int numSamples = 1;
    final long bytesPerRow = 1L;
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(Collections.singletonList(service.getTableRange(table))))
                .build(),
            null);
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
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null /*size*/);
    List<BigtableSource> splits =
        source.split(numRows * bytesPerRow / numSamples, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  /**
   * Regression test for <a href="https://github.com/apache/beam/issues/28793">[Bug]: BigtableSource
   * "Desired bundle size 0 bytes must be greater than 0" #28793</a>.
   */
  @Test
  public void testSplittingWithDesiredBundleSizeZero() throws Exception {
    final String table = "TEST-SPLIT-DESIRED-BUNDLE-SIZE-ZERO-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 1L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null /*size*/);
    List<BigtableSource> splits = source.split(0, null /* options */);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSamples));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  @Test
  public void testReadingWithSplitFailed() throws Exception {
    FailureBigtableService failureService =
        new FailureBigtableService(FailureOptions.builder().setFailAtSplit(true).build());

    BigtableConfig failureConfig = BigtableConfig.builder().setValidate(true).build();

    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 1500;
    final int numSamples = 10;
    final long bytesPerRow = 100L;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(failureService, table, numRows);
    failureService.setupSampleRowKeys(table, numSamples, bytesPerRow);

    FakeServiceFactory failureFactory = new FakeServiceFactory(failureService);

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            failureFactory,
            failureFactory.newId(),
            failureConfig,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null /*size*/);

    thrown.expect(RuntimeException.class);
    thrown.expectMessage("Fake Exception in getSampleRowKeys()");

    source.split(numRows * bytesPerRow / numSamples, null /* options */);
  }

  private void assertAllSourcesHaveSingleAdjacentRanges(List<BigtableSource> sources) {
    if (sources.size() > 0) {
      assertThat(sources.get(0).getRanges(), hasSize(1));
      for (int i = 1; i < sources.size(); i++) {
        assertThat(sources.get(i).getRanges(), hasSize(1));
        ByteKey lastEndKey = sources.get(i - 1).getRanges().get(0).getEndKey();
        ByteKey currentStartKey = sources.get(i).getRanges().get(0).getStartKey();
        assertEquals(lastEndKey, currentStartKey);
      }
    }
  }

  private void assertAllSourcesHaveSingleRanges(List<BigtableSource> sources) {
    for (BigtableSource source : sources) {
      assertThat(source.getRanges(), hasSize(1));
    }
  }

  private ByteKey createByteKey(int key) {
    return ByteKey.copyFrom(String.format("key%09d", key).getBytes(StandardCharsets.UTF_8));
  }

  /** Tests reduce splits with few non adjacent ranges. */
  @Test
  public void testReduceSplitsWithSomeNonAdjacentRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Construct few non contiguous key ranges [..1][1..2][3..4][4..5][6..7][8..9]
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(1), createByteKey(2)),
            ByteKeyRange.of(createByteKey(3), createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    // Expected ranges after split and reduction by maxSplitCount is [..2][3..5][6..7][8..9]
    List<ByteKeyRange> expectedKeyRangesAfterReducedSplits =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(2)),
            ByteKeyRange.of(createByteKey(3), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null /*size*/);

    List<BigtableSource> splits = new ArrayList<>();
    for (ByteKeyRange range : keyRanges) {
      splits.add(source.withSingleRange(range));
    }

    List<BigtableSource> reducedSplits = source.reduceSplits(splits, null, maxSplit);

    List<ByteKeyRange> actualRangesAfterSplit = new ArrayList<>();

    for (BigtableSource splitSource : reducedSplits) {
      actualRangesAfterSplit.addAll(splitSource.getRanges());
    }

    assertAllSourcesHaveSingleRanges(reducedSplits);

    assertThat(
        actualRangesAfterSplit,
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            expectedKeyRangesAfterReducedSplits.toArray()));
  }

  /** Tests reduce split with all non adjacent ranges. */
  @Test
  public void testReduceSplitsWithAllNonAdjacentRange() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Construct non contiguous key ranges [..1][2..3][4..5][6..7][8..9]
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(2), createByteKey(3)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)));

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null /*size*/);

    List<BigtableSource> splits = new ArrayList<>();
    for (ByteKeyRange range : keyRanges) {
      splits.add(source.withSingleRange(range));
    }

    List<BigtableSource> reducedSplits = source.reduceSplits(splits, null, maxSplit);

    List<ByteKeyRange> actualRangesAfterSplit = new ArrayList<>();

    for (BigtableSource splitSource : reducedSplits) {
      actualRangesAfterSplit.addAll(splitSource.getRanges());
    }

    assertAllSourcesHaveSingleRanges(reducedSplits);

    // The expected split source ranges are exactly same as original
    assertThat(
        actualRangesAfterSplit,
        IsIterableContainingInAnyOrder.containsInAnyOrder(keyRanges.toArray()));
  }

  /** Tests reduce Splits with all adjacent ranges. */
  @Test
  public void tesReduceSplitsWithAdjacentRanges() throws Exception {
    final String table = "TEST-MANY-ROWS-SPLITS-TABLE";
    final int numRows = 10;
    final int numSamples = 10;
    final long bytesPerRow = 100L;
    final int maxSplit = 3;

    // Set up test table data and sample row keys for size estimation and splitting.
    makeTableData(table, numRows);
    service.setupSampleRowKeys(table, numSamples, bytesPerRow);

    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null /*size*/);
    List<BigtableSource> splits = new ArrayList<>();
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(1)),
            ByteKeyRange.of(createByteKey(1), createByteKey(2)),
            ByteKeyRange.of(createByteKey(2), createByteKey(3)),
            ByteKeyRange.of(createByteKey(3), createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(5)),
            ByteKeyRange.of(createByteKey(5), createByteKey(6)),
            ByteKeyRange.of(createByteKey(6), createByteKey(7)),
            ByteKeyRange.of(createByteKey(7), createByteKey(8)),
            ByteKeyRange.of(createByteKey(8), createByteKey(9)),
            ByteKeyRange.of(createByteKey(9), ByteKey.EMPTY));
    for (ByteKeyRange range : keyRanges) {
      splits.add(source.withSingleRange(range));
    }

    // Splits Source have ranges [..1][1..2][2..3][3..4][4..5][5..6][6..7][7..8][8..9][9..]
    // expected reduced Split source ranges are [..4][4..8][8..]
    List<ByteKeyRange> expectedKeyRangesAfterReducedSplits =
        Arrays.asList(
            ByteKeyRange.of(ByteKey.EMPTY, createByteKey(4)),
            ByteKeyRange.of(createByteKey(4), createByteKey(8)),
            ByteKeyRange.of(createByteKey(8), ByteKey.EMPTY));

    List<BigtableSource> reducedSplits = source.reduceSplits(splits, null, maxSplit);

    List<ByteKeyRange> actualRangesAfterSplit = new ArrayList<>();

    for (BigtableSource splitSource : reducedSplits) {
      actualRangesAfterSplit.addAll(splitSource.getRanges());
    }

    assertThat(
        actualRangesAfterSplit,
        IsIterableContainingInAnyOrder.containsInAnyOrder(
            expectedKeyRangesAfterReducedSplits.toArray()));
    assertAllSourcesHaveSingleAdjacentRanges(reducedSplits);
    assertSourcesEqualReferenceSource(source, reducedSplits, null /* options */);
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

    ByteKey splitKey1 = ByteKey.copyFrom("key000000500".getBytes(StandardCharsets.UTF_8));
    ByteKey splitKey2 = ByteKey.copyFrom("key000001000".getBytes(StandardCharsets.UTF_8));

    ByteKeyRange tableRange = service.getTableRange(table);
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            tableRange.withEndKey(splitKey1),
            tableRange.withStartKey(splitKey1).withEndKey(splitKey2),
            tableRange.withStartKey(splitKey2));
    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null /*size*/);
    BigtableSource referenceSource =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(Collections.singletonList(service.getTableRange(table))))
                .build(),
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
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
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

    ByteKey splitKey1 = ByteKey.copyFrom("key000000330".getBytes(StandardCharsets.UTF_8));
    ByteKey splitKey2 = ByteKey.copyFrom("key000000730".getBytes(StandardCharsets.UTF_8));

    ByteKeyRange tableRange = service.getTableRange(table);
    List<ByteKeyRange> keyRanges =
        Arrays.asList(
            tableRange.withEndKey(splitKey1),
            tableRange.withStartKey(splitKey1).withEndKey(splitKey2),
            tableRange.withStartKey(splitKey2));
    // Generate source and split it.
    BigtableSource source =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(StaticValueProvider.of(keyRanges))
                .build(),
            null /*size*/);
    BigtableSource referenceSource =
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(
                    StaticValueProvider.of(ImmutableList.of(service.getTableRange(table))))
                .build(),
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
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setRowFilter(StaticValueProvider.of(filter))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null /*size*/);
    List<BigtableSource> splits = source.split(numRows * bytesPerRow / numSplits, null);

    // Test num splits and split equality.
    assertThat(splits, hasSize(numSplits));
    assertSourcesEqualReferenceSource(source, splits, null /* options */);
  }

  @Test
  public void testReadingDisplayData() {
    RowFilter rowFilter =
        RowFilter.newBuilder().setRowKeyRegexFilter(ByteString.copyFromUtf8("foo.*")).build();

    ByteKeyRange keyRange = ByteKeyRange.ALL_KEYS.withEndKey(ByteKey.of(0xab, 0xcd));
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId("fooTable")
            .withRowFilter(rowFilter)
            .withKeyRange(keyRange);

    DisplayData displayData = DisplayData.from(read);

    assertThat(
        displayData,
        hasDisplayItem(
            allOf(hasKey("tableId"), hasLabel("Bigtable Table Id"), hasValue("fooTable"))));

    assertThat(displayData, hasDisplayItem("rowFilter", rowFilter.toString()));

    assertThat(
        displayData, hasDisplayItem("keyRanges", "[ByteKeyRange{startKey=[], endKey=[abcd]}]"));

    // BigtableIO adds user-agent to options; assert only on key and not value.
    assertThat(displayData, hasDisplayItem("bigtableOptions"));
  }

  @Test
  public void testReadingDisplayDataFromRuntimeParameters() {
    ReadOptions options = PipelineOptionsFactory.fromArgs().withValidation().as(ReadOptions.class);
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withProjectId(options.getBigtableProject())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId());
    DisplayData displayData = DisplayData.from(read);
    assertThat(
        displayData,
        hasDisplayItem(
            allOf(
                hasKey("projectId"),
                hasLabel("Bigtable Project Id"),
                hasValue("RuntimeValueProvider{propertyName=bigtableProject, default=null}"))));
    assertThat(
        displayData,
        hasDisplayItem(
            allOf(
                hasKey("instanceId"),
                hasLabel("Bigtable Instance Id"),
                hasValue("RuntimeValueProvider{propertyName=bigtableInstanceId, default=null}"))));
    assertThat(
        displayData,
        hasDisplayItem(
            allOf(
                hasKey("tableId"),
                hasLabel("Bigtable Table Id"),
                hasValue("RuntimeValueProvider{propertyName=bigtableTableId, default=null}"))));
  }

  @Test
  public void testReadWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Read read =
        BigtableIO.read()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
            .withoutValidation();

    // validate() will throw if withoutValidation() isn't working
    read.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testWriteWithoutValidate() {
    final String table = "fooTable";
    BigtableIO.Write write =
        BigtableIO.write()
            .withBigtableOptions(BIGTABLE_OPTIONS)
            .withTableId(table)
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

  /** Tests that at least one result is emitted per element written in the global window. */
  @Test
  public void testWritingEmitsResultsWhenDoneInGlobalWindow() {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    PCollection<BigtableWriteResult> results =
        p.apply("single row", Create.of(makeWrite(key, value)).withCoder(bigtableCoder))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());
    PAssert.that(results)
        .inWindow(GlobalWindow.INSTANCE)
        .containsInAnyOrder(BigtableWriteResult.create(1));

    p.run();
  }

  /**
   * Tests that the outputs of the Bigtable writer are correctly windowed, and can be used in a
   * Wait.on transform as the trigger.
   */
  @Test
  public void testWritingAndWaitingOnResults() {
    final String table = "table";
    final String key = "key";
    final String value = "value";

    service.createTable(table);

    Instant elementTimestamp = Instant.parse("2019-06-10T00:00:00");
    Duration windowDuration = Duration.standardMinutes(1);

    TestStream<KV<ByteString, Iterable<Mutation>>> writeInputs =
        TestStream.create(bigtableCoder)
            .advanceWatermarkTo(elementTimestamp)
            .addElements(makeWrite(key, value))
            .advanceWatermarkToInfinity();

    TestStream<String> testInputs =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(elementTimestamp)
            .addElements("done")
            .advanceWatermarkToInfinity();

    PCollection<BigtableWriteResult> writes =
        p.apply("rows", writeInputs)
            .apply(
                "window rows",
                Window.<KV<ByteString, Iterable<Mutation>>>into(FixedWindows.of(windowDuration))
                    .withAllowedLateness(Duration.ZERO))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());

    PCollection<String> inputs =
        p.apply("inputs", testInputs)
            .apply("window inputs", Window.into(FixedWindows.of(windowDuration)))
            .apply("wait", Wait.on(writes));

    BoundedWindow expectedWindow = new IntervalWindow(elementTimestamp, windowDuration);

    PAssert.that(inputs).inWindow(expectedWindow).containsInAnyOrder("done");

    p.run();
  }

  /**
   * A DoFn used to generate N outputs, where N is the input. Used to generate bundles of >= 1
   * element.
   */
  private static class WriteGeneratorDoFn extends DoFn<Long, KV<ByteString, Iterable<Mutation>>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      for (int i = 0; i < ctx.element(); i++) {
        ctx.output(makeWrite("key", "value"));
      }
    }
  }

  /** Tests that at least one result is emitted per element written in each window. */
  @Test
  public void testWritingEmitsResultsWhenDoneInFixedWindow() throws Exception {
    final String table = "table";

    service.createTable(table);

    Instant elementTimestamp = Instant.parse("2019-06-10T00:00:00");
    Duration windowDuration = Duration.standardMinutes(1);

    TestStream<Long> input =
        TestStream.create(VarLongCoder.of())
            .advanceWatermarkTo(elementTimestamp)
            .addElements(1L)
            .advanceWatermarkTo(elementTimestamp.plus(windowDuration))
            .addElements(2L)
            .advanceWatermarkToInfinity();

    BoundedWindow expectedFirstWindow = new IntervalWindow(elementTimestamp, windowDuration);
    BoundedWindow expectedSecondWindow =
        new IntervalWindow(elementTimestamp.plus(windowDuration), windowDuration);

    PCollection<BigtableWriteResult> results =
        p.apply("rows", input)
            .apply("window", Window.into(FixedWindows.of(windowDuration)))
            .apply("expand", ParDo.of(new WriteGeneratorDoFn()))
            .apply("write", defaultWrite.withTableId(table).withWriteResults());
    PAssert.that(results)
        .inWindow(expectedFirstWindow)
        .containsInAnyOrder(BigtableWriteResult.create(1));
    PAssert.that(results)
        .inWindow(expectedSecondWindow)
        .containsInAnyOrder(BigtableWriteResult.create(2));

    p.run();
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

  /** Tests that the error happens when submitting the write request is handled. */
  @Test
  public void testWritingFailsAtWriteRecord() throws IOException {
    FailureBigtableService failureService =
        new FailureBigtableService(FailureOptions.builder().setFailAtWriteRecord(true).build());

    FakeServiceFactory failureFactory = new FakeServiceFactory(failureService);

    BigtableIO.Write failureWrite =
        BigtableIO.write().withInstanceId("instance").withProjectId("project");

    final String table = "table";
    final String key = "key";
    final String value = "value";

    failureService.createTable(table);

    p.apply("single row", Create.of(makeWrite(key, value)).withCoder(bigtableCoder))
        .apply("write", failureWrite.withTableId(table).withServiceFactory(failureFactory));

    // Exception will be thrown by writer.writeRecord() when BigtableWriterFn is applied.
    thrown.expect(IOException.class);
    thrown.expectMessage("Fake IOException in writeRecord()");

    try {
      p.run();
    } catch (PipelineExecutionException e) {
      // throwing inner exception helps assert that first exception is thrown from writeRecord()
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
    }
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
    BigtableIO.Write write =
        BigtableIO.write().withTableId("fooTable").withBigtableOptions(BIGTABLE_OPTIONS);

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
        new BigtableSource(
            factory,
            configId,
            config,
            BigtableReadOptions.builder()
                .setTableId(StaticValueProvider.of(table))
                .setKeyRanges(ALL_KEY_RANGE)
                .build(),
            null);

    BoundedReader<Row> reader = source.createReader(TestPipeline.testingPipelineOptions());

    reader.start();
    // Started, 0 split points consumed
    assertEquals(
        "splitPointsConsumed starting", splitPointsConsumed, reader.getSplitPointsConsumed());

    // Split points consumed increases for each row read
    while (reader.advance()) {
      assertEquals(
          "splitPointsConsumed advancing", ++splitPointsConsumed, reader.getSplitPointsConsumed());
    }

    // Reader marked as done, 100 split points consumed
    assertEquals("splitPointsConsumed done", numRows, reader.getSplitPointsConsumed());

    reader.close();
  }

  @Test
  public void testReadWithBigTableOptionsSetsRetryOptions() {
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    RetryOptions.Builder retryOptionsBuilder = RetryOptions.builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder.setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Read read = BigtableIO.read().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = read.getBigtableOptions();
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(options.getRetryOptions(), Matchers.equalTo(retryOptionsBuilder.build()));
  }

  @Test
  public void testWriteWithBigTableOptionsSetsBulkOptionsAndRetryOptions() {
    final int maxInflightRpcs = 1;
    final int initialBackoffMillis = -1;

    BigtableOptions.Builder optionsBuilder = BIGTABLE_OPTIONS.toBuilder();

    BulkOptions.Builder bulkOptionsBuilder = BulkOptions.builder();
    bulkOptionsBuilder.setMaxInflightRpcs(maxInflightRpcs);

    RetryOptions.Builder retryOptionsBuilder = RetryOptions.builder();
    retryOptionsBuilder.setInitialBackoffMillis(initialBackoffMillis);

    optionsBuilder
        .setBulkOptions(bulkOptionsBuilder.build())
        .setRetryOptions(retryOptionsBuilder.build());

    BigtableIO.Write write = BigtableIO.write().withBigtableOptions(optionsBuilder.build());

    BigtableOptions options = write.getBigtableOptions();
    assertTrue(options.getBulkOptions().useBulkApi());
    assertEquals(maxInflightRpcs, options.getBulkOptions().getMaxInflightRpcs());
    assertEquals(initialBackoffMillis, options.getRetryOptions().getInitialBackoffMillis());

    assertThat(
        options.getBulkOptions(), Matchers.equalTo(bulkOptionsBuilder.setUseBulkApi(true).build()));
    assertThat(options.getRetryOptions(), Matchers.equalTo(retryOptionsBuilder.build()));
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

  /** Helper function to create a table and return the rows that it created with custom service. */
  private static List<Row> makeTableData(
      FakeBigtableService fakeService, String tableId, int numRows) {
    fakeService.createTable(tableId);
    Map<ByteString, ByteString> testData = fakeService.getTable(tableId);

    List<Row> testRows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; ++i) {
      ByteString key = ByteString.copyFromUtf8(String.format("key%09d", i));
      ByteString value = ByteString.copyFromUtf8(String.format("value%09d", i));
      testData.put(key, value);
      testRows.add(makeRow(key, value));
    }

    return testRows;
  }

  /** Helper function to create a table and return the rows that it created. */
  private static List<Row> makeTableData(String tableId, int numRows) {
    return makeTableData(service, tableId, numRows);
  }

  /** A {@link BigtableService} implementation that stores tables and their contents in memory. */
  private static class FakeBigtableService implements BigtableService {
    private final Map<String, SortedMap<ByteString, ByteString>> tables = new HashMap<>();
    private final Map<String, List<KeyOffset>> sampleRowKeys = new HashMap<>();

    public @Nullable SortedMap<ByteString, ByteString> getTable(String tableId) {
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
    public FakeBigtableWriter openForWriting(BigtableWriteOptions writeOptions) {
      return new FakeBigtableWriter(writeOptions.getTableId().get());
    }

    @Override
    public List<KeyOffset> getSampleRowKeys(BigtableSource source) {
      List<KeyOffset> samples = sampleRowKeys.get(source.getTableId().get());
      checkNotNull(samples, "No samples found for table %s", source.getTableId().get());
      return samples;
    }

    @Override
    public void close() {}

    /** Sets up the sample row keys for the specified table. */
    void setupSampleRowKeys(String tableId, int numSamples, long bytesPerRow) {
      verifyTableExists(tableId);
      checkArgument(numSamples > 0, "Number of samples must be positive: %s", numSamples);
      checkArgument(bytesPerRow > 0, "Bytes/Row must be positive: %s", bytesPerRow);

      ImmutableList.Builder<KeyOffset> ret = ImmutableList.builder();
      SortedMap<ByteString, ByteString> rows = getTable(tableId);
      int currentSample = 1;
      int rowsSoFar = 0;
      for (Map.Entry<ByteString, ByteString> entry : rows.entrySet()) {
        if (((double) rowsSoFar) / rows.size() >= ((double) currentSample) / numSamples) {
          // add the sample with the total number of bytes in the table before this key.
          ret.add(KeyOffset.create(entry.getKey(), rowsSoFar * bytesPerRow));
          // Move on to next sample
          currentSample++;
        }
        ++rowsSoFar;
      }

      // Add the last sample indicating the end of the table, with all rows before it
      ret.add(KeyOffset.create(ByteString.EMPTY, rows.size() * bytesPerRow));

      sampleRowKeys.put(tableId, ret.build());
    }
  }

  /** A {@link BigtableService} implementation that throw exceptions at given stage. */
  private static class FailureBigtableService extends FakeBigtableService {
    public FailureBigtableService(FailureOptions options) {
      failureOptions = options;
    }

    @Override
    public FakeBigtableReader createReader(BigtableSource source) {
      return new FailureBigtableReader(source, this, failureOptions);
    }

    @Override
    public FailureBigtableWriter openForWriting(BigtableWriteOptions writeOptions) {
      return new FailureBigtableWriter(writeOptions.getTableId().get(), this, failureOptions);
    }

    @Override
    public List<KeyOffset> getSampleRowKeys(BigtableSource source) {
      if (failureOptions.getFailAtSplit()) {
        throw new RuntimeException("Fake Exception in getSampleRowKeys()");
      }
      return super.getSampleRowKeys(source);
    }

    @Override
    public void close() {}

    private final FailureOptions failureOptions;
  }

  /**
   * A {@link BigtableService.Reader} implementation that reads from the static instance of {@link
   * FakeBigtableService} stored in {@link #service}.
   *
   * <p>This reader does not support {@link RowFilter} objects.
   */
  private static class FakeBigtableReader implements BigtableService.Reader {
    private final BigtableSource source;
    private final FakeBigtableService service;
    private Iterator<Map.Entry<ByteString, ByteString>> rows;
    private Row currentRow;
    private final Predicate<ByteString> filter;

    public FakeBigtableReader(BigtableSource source, FakeBigtableService service) {
      this.source = source;
      this.service = service;
      if (source.getRowFilter() == null) {
        filter = Predicates.alwaysTrue();
      } else {
        ByteString keyRegex = source.getRowFilter().getRowKeyRegexFilter();
        checkArgument(!keyRegex.isEmpty(), "Only RowKeyRegexFilter is supported");
        filter = new KeyMatchesRegex(keyRegex.toStringUtf8());
      }
      service.verifyTableExists(source.getTableId().get());
    }

    public FakeBigtableReader(BigtableSource source) {
      this(source, BigtableIOTest.service);
    }

    @Override
    public boolean start() throws IOException {
      rows = service.tables.get(source.getTableId().get()).entrySet().iterator();
      return advance();
    }

    @Override
    public boolean advance() throws IOException {
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
    public void close() {}
  }

  /** A {@link FakeBigtableReader} implementation that throw exceptions at given stage. */
  private static class FailureBigtableReader extends FakeBigtableReader {
    public FailureBigtableReader(
        BigtableSource source, FakeBigtableService service, FailureOptions options) {
      super(source, service);
      failureOptions = options;
      numAdvance = 0;
    }

    @Override
    public boolean start() throws IOException {
      if (failureOptions.getFailAtStart()) {
        throw new IOException("Fake IOException at start()");
      }
      return super.start();
    }

    @Override
    public boolean advance() throws IOException {
      if (failureOptions.getFailAtAdvance() && numAdvance > 0) {
        // because start() will call numAdvance once, only throw if called before
        throw new IOException("Fake IOException at advance()");
      }
      ++numAdvance;
      return super.advance();
    }

    private long numAdvance;
    private final FailureOptions failureOptions;
  }

  /**
   * A {@link BigtableService.Writer} implementation that writes to the static instance of {@link
   * FakeBigtableService} stored in {@link #service}.
   *
   * <p>This writer only supports {@link Mutation Mutations} that consist only of {@link SetCell}
   * entries. The column family in the {@link SetCell} is ignored; only the value is used.
   *
   * <p>When no {@link SetCell} is provided, the write will fail and this will be exposed via an
   * exception on the returned {@link CompletionStage}.
   */
  private static class FakeBigtableWriter implements BigtableService.Writer {
    private final String tableId;
    private final FakeBigtableService service;

    public FakeBigtableWriter(String tableId, FakeBigtableService service) {
      this.tableId = tableId;
      this.service = service;
    }

    public FakeBigtableWriter(String tableId) {
      this(tableId, BigtableIOTest.service);
    }

    @Override
    public CompletableFuture<MutateRowResponse> writeRecord(
        KV<ByteString, Iterable<Mutation>> record) throws IOException {
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
    public void writeSingleRecord(KV<ByteString, Iterable<Mutation>> record) {}

    @Override
    public void close() {}
  }

  /** A {@link FakeBigtableWriter} implementation that throw exceptions at given stage. */
  private static class FailureBigtableWriter extends FakeBigtableWriter {
    public FailureBigtableWriter(
        String tableId, FailureBigtableService service, FailureOptions options) {
      super(tableId, service);
      this.failureOptions = options;
    }

    @Override
    public CompletableFuture<MutateRowResponse> writeRecord(
        KV<ByteString, Iterable<Mutation>> record) throws IOException {
      if (failureOptions.getFailAtWriteRecord()) {
        throw new IOException("Fake IOException in writeRecord()");
      }
      return super.writeRecord(record);
    }

    @Override
    public void writeSingleRecord(KV<ByteString, Iterable<Mutation>> record) throws ApiException {
      if (failureOptions.getFailAtWriteRecord()) {
        throw new RuntimeException("Fake RuntimeException in writeRecord()");
      }
    }

    private final FailureOptions failureOptions;
  }

  /** A serializable comparator for ByteString. Used to make row samples. */
  private static final class ByteStringComparator implements Comparator<ByteString>, Serializable {
    @Override
    public int compare(ByteString o1, ByteString o2) {
      return makeByteKey(o1).compareTo(makeByteKey(o2));
    }
  }

  /** Error injection options for FakeBigtableService and FakeBigtableReader. */
  @AutoValue
  abstract static class FailureOptions implements Serializable {
    abstract Boolean getFailAtStart();

    abstract Boolean getFailAtAdvance();

    abstract Boolean getFailAtSplit();

    abstract Boolean getFailAtWriteRecord();

    static Builder builder() {
      return new AutoValue_BigtableIOTest_FailureOptions.Builder()
          .setFailAtStart(false)
          .setFailAtAdvance(false)
          .setFailAtSplit(false)
          .setFailAtWriteRecord(false);
    }

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFailAtStart(Boolean failAtStart);

      abstract Builder setFailAtAdvance(Boolean failAtAdvance);

      abstract Builder setFailAtSplit(Boolean failAtSplit);

      abstract Builder setFailAtWriteRecord(Boolean failAtWriteRecord);

      abstract FailureOptions build();
    }
  }

  static class FakeServiceFactory extends BigtableServiceFactory {
    private FakeBigtableService service;

    FakeServiceFactory(FakeBigtableService service) {
      this.service = service;
    }

    @Override
    BigtableServiceEntry getServiceForReading(
        ConfigId configId,
        BigtableConfig config,
        BigtableReadOptions opts,
        PipelineOptions pipelineOptions) {
      return BigtableServiceEntry.create(configId, service);
    }

    @Override
    BigtableServiceEntry getServiceForWriting(
        ConfigId configId,
        BigtableConfig config,
        BigtableWriteOptions opts,
        PipelineOptions pipelineOptions) {
      return BigtableServiceEntry.create(configId, service);
    }

    @Override
    boolean checkTableExists(BigtableConfig config, PipelineOptions pipelineOptions, String tableId)
        throws IOException {
      return service.tableExists(tableId);
    }

    @Override
    synchronized ConfigId newId() {
      return ConfigId.create();
    }
  }

  /////////////////////////// ReadChangeStream ///////////////////////////

  @Test
  public void testReadChangeStreamBuildsCorrectly() {
    Instant startTime = Instant.now();
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table")
            .withAppProfileId("app-profile")
            .withChangeStreamName("change-stream-name")
            .withMetadataTableProjectId("metadata-project")
            .withMetadataTableInstanceId("metadata-instance")
            .withMetadataTableTableId("metadata-table")
            .withMetadataTableAppProfileId("metadata-app-profile")
            .withStartTime(startTime)
            .withBacklogReplicationAdjustment(Duration.standardMinutes(1))
            .withReadChangeStreamTimeout(Duration.standardMinutes(1))
            .withCreateOrUpdateMetadataTable(false)
            .withExistingPipelineOptions(BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS);
    assertEquals("project", readChangeStream.getBigtableConfig().getProjectId().get());
    assertEquals("instance", readChangeStream.getBigtableConfig().getInstanceId().get());
    assertEquals("app-profile", readChangeStream.getBigtableConfig().getAppProfileId().get());
    assertEquals("table", readChangeStream.getTableId());
    assertEquals(
        "metadata-project", readChangeStream.getMetadataTableBigtableConfig().getProjectId().get());
    assertEquals(
        "metadata-instance",
        readChangeStream.getMetadataTableBigtableConfig().getInstanceId().get());
    assertEquals(
        "metadata-app-profile",
        readChangeStream.getMetadataTableBigtableConfig().getAppProfileId().get());
    assertEquals("metadata-table", readChangeStream.getMetadataTableId());
    assertEquals("change-stream-name", readChangeStream.getChangeStreamName());
    assertEquals(startTime, readChangeStream.getStartTime());
    assertEquals(Duration.standardMinutes(1), readChangeStream.getBacklogReplicationAdjustment());
    assertEquals(Duration.standardMinutes(1), readChangeStream.getReadChangeStreamTimeout());
    assertEquals(false, readChangeStream.getCreateOrUpdateMetadataTable());
    assertEquals(
        BigtableIO.ExistingPipelineOptions.FAIL_IF_EXISTS,
        readChangeStream.getExistingPipelineOptions());
  }

  @Test
  public void testReadChangeStreamFailsValidation() {
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table");
    // Validating table fails because table does not exist.
    thrown.expect(IllegalArgumentException.class);
    readChangeStream.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testReadChangeStreamPassWithoutValidation() {
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table")
            .withoutValidation();
    // No error is thrown because we skip validation
    readChangeStream.validate(TestPipeline.testingPipelineOptions());
  }

  @Test
  public void testReadChangeStreamValidationFailsDuringApply() {
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table");
    // Validating table fails because resources cannot be found
    thrown.expect(RuntimeException.class);

    p.apply(readChangeStream);
  }

  @Test
  public void testReadChangeStreamPassWithoutValidationDuringApply() {
    BigtableIO.ReadChangeStream readChangeStream =
        BigtableIO.readChangeStream()
            .withProjectId("project")
            .withInstanceId("instance")
            .withTableId("table")
            .withoutValidation();
    // No RunTime exception as seen in previous test with validation. Only error that the pipeline
    // is not ran.
    thrown.expect(PipelineRunMissingException.class);
    p.apply(readChangeStream);
  }
}

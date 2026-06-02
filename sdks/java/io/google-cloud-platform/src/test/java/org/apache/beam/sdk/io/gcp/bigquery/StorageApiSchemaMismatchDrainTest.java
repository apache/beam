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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices;
import org.apache.beam.sdk.io.gcp.testing.FakeDatasetService;
import org.apache.beam.sdk.io.gcp.testing.FakeJobService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.Statement;

/**
 * Tests the drain path taken when a consistent schema update never arrives.
 *
 * <p>When {@code withAutoSchemaUpdateConsistent} is enabled, rows whose schema does not match the
 * destination table are buffered in state and retried rather than dropped. If the table schema is
 * never widened, those buffered rows have to be released when the window expires -- otherwise a
 * draining pipeline would either hang or silently discard them. That release happens in the
 * {@code @OnWindowExpiration} handlers of {@link BufferMismatchedRows} (at-least-once) and {@link
 * StorageApiWritesShardedRecords} (exactly-once), which are the two paths exercised here.
 *
 * <p>These tests never update the table schema, so every input row must end up in the failed-rows
 * collection exactly once.
 */
@RunWith(Parameterized.class)
@SuppressWarnings({"nullness"})
public class StorageApiSchemaMismatchDrainTest implements Serializable {

  private static final String DRAIN_MESSAGE =
      "Timed out waiting for table schema update in OnWindowExpiration";

  private static final int NUM_ROWS = 5;

  @Parameters(name = "useAtLeastOnce={0}")
  public static Iterable<Object[]> data() {
    return ImmutableList.of(new Object[] {false}, new Object[] {true});
  }

  @Parameter(0)
  public boolean useAtLeastOnce;

  private transient PipelineOptions options;
  private transient TemporaryFolder testFolder = new TemporaryFolder();
  private transient TestPipeline p;

  @Rule public transient Timeout globalTimeout = Timeout.seconds(300);

  @Rule
  public final transient TestRule folderThenPipeline =
      new TestRule() {
        @Override
        public Statement apply(final Statement base, final Description description) {
          Statement withPipeline =
              new Statement() {
                @Override
                public void evaluate() throws Throwable {
                  options = TestPipeline.testingPipelineOptions();
                  BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
                  bqOptions.setProject("project-id");
                  bqOptions.setTempLocation(testFolder.getRoot().getAbsolutePath());
                  bqOptions.setUseStorageWriteApi(true);
                  if (useAtLeastOnce) {
                    bqOptions.setUseStorageWriteApiAtLeastOnce(true);
                  }
                  bqOptions.setStorageWriteApiTriggeringFrequencySec(1);
                  options.as(StreamingOptions.class).setStreaming(true);
                  p = TestPipeline.fromOptions(options);
                  p.apply(base, description).evaluate();
                }
              };
          return testFolder.apply(withPipeline, description);
        }
      };

  private FakeDatasetService fakeDatasetService = new FakeDatasetService();
  private FakeJobService fakeJobService = new FakeJobService();
  private FakeBigQueryServices fakeBqServices =
      new FakeBigQueryServices()
          .withDatasetService(fakeDatasetService)
          .withJobService(fakeJobService);

  @Before
  public void setUp() throws ExecutionException, IOException, InterruptedException {
    FakeDatasetService.setUp();
    BigQueryIO.clearStaticCaches();
    fakeDatasetService.createDataset("project-id", "dataset-id", "", "", null);
  }

  /** A row whose {@code extra} field is absent from the destination table schema. */
  private static TableRow rowWithUnknownField(long i) {
    return new TableRow()
        .set("number", Long.toString(i))
        .set("name", "name" + i)
        .set("extra", "extra" + i);
  }

  private TableReference createNarrowTable() throws IOException, InterruptedException {
    TableReference tableRef = BigQueryHelpers.parseTableSpec("project-id:dataset-id.table");
    TableSchema narrowSchema =
        new TableSchema()
            .setFields(
                ImmutableList.of(
                    new TableFieldSchema().setName("number").setType("INTEGER"),
                    new TableFieldSchema().setName("name").setType("STRING")));
    fakeDatasetService.createTable(new Table().setTableReference(tableRef).setSchema(narrowSchema));
    return tableRef;
  }

  /**
   * Builds a pipeline whose destination table schema is never widened, so the rows stay mismatched
   * until the window expires.
   *
   * @param strictTimeout the per-row retry deadline given to {@code withAutoSchemaUpdateConsistent}
   */
  private PCollection<BigQueryStorageApiInsertError> buildPipeline(
      TableReference tableRef, Duration strictTimeout) {
    p.enableAbandonedNodeEnforcement(false);

    BigQueryOptions bqOptions = p.getOptions().as(BigQueryOptions.class);
    // Keep GroupIntoBatches from buffering rows away from the schema-mismatch machinery.
    bqOptions.setStorageApiAppendThresholdBytes(1);
    bqOptions.setNumStorageWriteApiStreams(1);
    bqOptions.setStorageApiMismatchLocalRetryTimeMilliSec(0);
    // Retry quickly, and bound the drain so the test does not sit in the backoff loop. The drain
    // budget is shared across keys within a window, so this bounds the whole drain, not each key.
    bqOptions.setStorageApiMismatchRetryTimeMilliSec(500);
    bqOptions.setStorageApiMismatchDrainRetryTimeMilliSec(1000);

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    for (long i = 0; i < NUM_ROWS; i++) {
      testStream = testStream.addElements(i);
    }
    // Let the retry timer fire at least once before the window expires.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(5));

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(
                useAtLeastOnce ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .ignoreUnknownValues()
            .withAutoSchemaUpdateConsistent(true, strictTimeout)
            .withTestServices(fakeBqServices)
            .withoutValidation();

    return p.apply(testStream.advanceWatermarkToInfinity())
        .apply(
            "getRow",
            MapElements.into(TypeDescriptor.of(TableRow.class))
                .via(
                    (SerializableFunction<Long, TableRow>)
                        StorageApiSchemaMismatchDrainTest::rowWithUnknownField))
        .setCoder(TableRowJsonCoder.of())
        .apply(write)
        .getFailedStorageApiInserts();
  }

  private static PCollection<String> failedRowNames(
      PCollection<BigQueryStorageApiInsertError> failedRows) {
    return failedRows.apply(
        "names",
        MapElements.into(TypeDescriptors.strings())
            .via(
                (SerializableFunction<BigQueryStorageApiInsertError, String>)
                    e -> (String) e.getRow().get("name")));
  }

  private static List<String> expectedNames() {
    return LongStream.range(0, NUM_ROWS).mapToObj(i -> "name" + i).collect(Collectors.toList());
  }

  /**
   * Asserts that every expected row name reaches the dead-letter output exactly once.
   *
   * <p>{@code containsInAnyOrder} is an exact multiset match and so would already fail on a
   * duplicate, but only with a whole-collection mismatch dump that does not say which row was
   * doubled. The drain loop re-reads the same buffered rows on every round, and rows that expire
   * mid-round are emitted from a different place than rows released at the drain deadline, so a
   * double emission is the most likely way for it to regress. This asserts it directly and names
   * the offending rows.
   */
  private static SerializableFunction<Iterable<String>, Void> assertEmittedExactlyOnce() {
    return observed -> {
      Set<String> seen = Sets.newHashSet();
      List<String> duplicated = Lists.newArrayList();
      for (String name : observed) {
        if (!seen.add(name)) {
          duplicated.add(name);
        }
      }
      assertEquals(
          "rows emitted more than once by the drain", ImmutableList.<String>of(), duplicated);
      assertEquals("rows missing from the drain output", Sets.newHashSet(expectedNames()), seen);
      return null;
    };
  }

  /**
   * With a retry deadline far in the future no row expires on its own, so the drain deadline is
   * what releases them and every row must be dead-lettered with the drain message.
   */
  @Test
  public void testWindowExpirationDrainsBufferedRowsWhenSchemaNeverUpdates() throws Exception {
    TableReference tableRef = createNarrowTable();

    PCollection<BigQueryStorageApiInsertError> failedRows =
        buildPipeline(tableRef, Duration.standardHours(1));

    PCollection<String> names = failedRowNames(failedRows);
    PAssert.that("every buffered row is dead-lettered exactly once", names)
        .containsInAnyOrder(expectedNames());
    PAssert.that("the drain must not emit a row twice", names)
        .satisfies(assertEmittedExactlyOnce());

    PCollection<String> messages =
        failedRows.apply(
            "messages",
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (SerializableFunction<BigQueryStorageApiInsertError, String>)
                        BigQueryStorageApiInsertError::getErrorMessage));
    PAssert.that("all failures come from the window-expiration drain", messages)
        .satisfies(
            observed -> {
              for (String message : observed) {
                assertEquals(DRAIN_MESSAGE, message);
              }
              return null;
            });

    p.run();

    // Nothing should have reached the table, since its schema never accepted these rows.
    assertTrue(
        fakeDatasetService
            .getAllRows(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId())
            .isEmpty());
  }

  /**
   * With a retry deadline that lapses while rows are still buffered, rows expire inside the drain
   * loop rather than at its deadline. Whichever branch catches them, no row may be lost: an earlier
   * version of the drain loop derived its pending set and its expired set from two different
   * collections, so a row that expired inside {@code processElement} was filtered out of the
   * pending set without ever being recorded as expired.
   */
  @Test
  public void testExpiredRowsAreDeadLetteredAndNotDropped() throws Exception {
    TableReference tableRef = createNarrowTable();

    PCollection<BigQueryStorageApiInsertError> failedRows =
        buildPipeline(tableRef, Duration.millis(1));

    PCollection<String> names = failedRowNames(failedRows);
    PAssert.that("no row may be lost when its retry deadline lapses", names)
        .containsInAnyOrder(expectedNames());
    PAssert.that("no row may be dead-lettered twice when its retry deadline lapses", names)
        .satisfies(assertEmittedExactlyOnce());

    p.run();
  }

  private static TableSchema widenedSchema() {
    return new TableSchema()
        .setFields(
            ImmutableList.of(
                new TableFieldSchema().setName("number").setType("INTEGER"),
                new TableFieldSchema().setName("name").setType("STRING"),
                new TableFieldSchema().setName("extra").setType("STRING")));
  }

  private static List<TableRow> expectedRowsWithExtra() {
    return LongStream.range(0, NUM_ROWS)
        .mapToObj(StorageApiSchemaMismatchDrainTest::rowWithUnknownField)
        .collect(Collectors.toList());
  }

  /**
   * Verifies that rows buffered due to a schema mismatch are flushed with all unknown fields intact
   * by the {@code @OnTimer} retry handler after the destination table schema is widened externally,
   * even when no subsequent upstream elements arrive.
   */
  @Test
  public void testBufferedRowsRecoverOnTimerWhenSchemaUpdatesWithNoSubsequentElements()
      throws Exception {
    TableReference tableRef = createNarrowTable();
    p.enableAbandonedNodeEnforcement(false);

    BigQueryOptions bqOptions = p.getOptions().as(BigQueryOptions.class);
    bqOptions.setStorageApiAppendThresholdBytes(1);
    bqOptions.setNumStorageWriteApiStreams(1);
    bqOptions.setStorageApiMismatchLocalRetryTimeMilliSec(0);
    bqOptions.setStorageApiMismatchRetryTimeMilliSec(500);

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    for (long i = 0; i < NUM_ROWS; i++) {
      testStream = testStream.addElements(i);
    }
    // Advance past UpdateTableSchemaDoFn's 2s timer so the schema is widened in FakeDatasetService
    // without emitting any new elements.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(5));
    // Advance processing time again so the @OnTimer retry handler observes the widened schema and
    // flushes the buffered rows before window expiration.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(10));

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(
                useAtLeastOnce ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .ignoreUnknownValues()
            .withAutoSchemaUpdateConsistent(true, Duration.standardHours(1))
            .withTestServices(fakeBqServices)
            .withoutValidation();

    PCollection<BigQueryStorageApiInsertError> failedRows =
        p.apply(testStream.advanceWatermarkToInfinity())
            .apply(
                "getRow",
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(
                        (SerializableFunction<Long, TableRow>)
                            StorageApiSchemaMismatchDrainTest::rowWithUnknownField))
            .apply("addKey", WithKeys.of("project-id:dataset-id.table"))
            .apply(
                "updateSchemaOnTimer",
                ParDo.of(
                    new BigQueryIOWriteTest.UpdateTableSchemaDoFn(
                        Duration.standardSeconds(2), widenedSchema(), fakeDatasetService)))
            .setCoder(TableRowJsonCoder.of())
            .apply(write)
            .getFailedStorageApiInserts();

    PAssert.that("no rows should fail when schema updates before timeout", failedRows).empty();

    p.run();

    assertThat(
        fakeDatasetService.getAllRows(
            tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
        containsInAnyOrder(expectedRowsWithExtra().toArray(new TableRow[0])));
  }

  /**
   * Verifies that when {@code @OnTimer} does not fire before the window expires, the
   * {@code @OnWindowExpiration} drain path refreshes the destination table schema and writes all
   * buffered rows (including their newly added columns) to BigQuery rather than dropping or
   * dead-lettering them.
   */
  @Test
  public void testWindowExpirationDrainsAndWritesBufferedRowsWhenSchemaUpdatesBeforeDrain()
      throws Exception {
    TableReference tableRef = createNarrowTable();
    p.enableAbandonedNodeEnforcement(false);

    BigQueryOptions bqOptions = p.getOptions().as(BigQueryOptions.class);
    bqOptions.setStorageApiAppendThresholdBytes(1);
    bqOptions.setNumStorageWriteApiStreams(1);
    bqOptions.setStorageApiMismatchLocalRetryTimeMilliSec(0);
    // Schedule the retry timer far in the future (1 hour) so @OnTimer never fires during the test,
    // forcing recovery to happen exclusively inside @OnWindowExpiration.
    bqOptions.setStorageApiMismatchRetryTimeMilliSec(3_600_000);
    bqOptions.setStorageApiMismatchDrainRetryTimeMilliSec(5_000);

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    for (long i = 0; i < NUM_ROWS; i++) {
      testStream = testStream.addElements(i);
    }
    // Fire UpdateTableSchemaDoFn at t=2s (while retryTimer is still set for t=3600s), then
    // immediately expire the window so @OnWindowExpiration must drain the buffered rows.
    testStream = testStream.advanceProcessingTime(Duration.standardSeconds(3));

    BigQueryIO.Write<TableRow> write =
        BigQueryIO.writeTableRows()
            .to(tableRef)
            .withMethod(
                useAtLeastOnce ? Method.STORAGE_API_AT_LEAST_ONCE : Method.STORAGE_WRITE_API)
            .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
            .ignoreUnknownValues()
            .withAutoSchemaUpdateConsistent(true, Duration.standardHours(1))
            .withTestServices(fakeBqServices)
            .withoutValidation();

    PCollection<BigQueryStorageApiInsertError> failedRows =
        p.apply(testStream.advanceWatermarkToInfinity())
            .apply(
                "getRow",
                MapElements.into(TypeDescriptor.of(TableRow.class))
                    .via(
                        (SerializableFunction<Long, TableRow>)
                            StorageApiSchemaMismatchDrainTest::rowWithUnknownField))
            .apply("addKey", WithKeys.of("project-id:dataset-id.table"))
            .apply(
                "updateSchemaOnTimer",
                ParDo.of(
                    new BigQueryIOWriteTest.UpdateTableSchemaDoFn(
                        Duration.standardSeconds(2), widenedSchema(), fakeDatasetService)))
            .setCoder(TableRowJsonCoder.of())
            .apply(write)
            .getFailedStorageApiInserts();

    PAssert.that("no rows should be dead-lettered when drain succeeds", failedRows).empty();

    p.run();

    assertThat(
        fakeDatasetService.getAllRows(
            tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId()),
        containsInAnyOrder(expectedRowsWithExtra().toArray(new TableRow[0])));
  }

  private static class ConstantTableDynamicDestinations
      extends DynamicDestinations<TableRow, String> {
    private final String tableSpec;

    ConstantTableDynamicDestinations(String tableSpec) {
      this.tableSpec = tableSpec;
    }

    @Override
    public String getDestination(org.apache.beam.sdk.values.ValueInSingleWindow<TableRow> element) {
      return tableSpec;
    }

    @Override
    public TableDestination getTable(String destination) {
      return new TableDestination(BigQueryHelpers.parseTableSpec(destination), null);
    }

    @Override
    public TableSchema getSchema(String destination) {
      return null;
    }
  }

  /**
   * Verifies that {@link SchemaUpdateHoldingFn#onWindowExpiration} bounds its retry loop by {@link
   * BigQueryOptions#getStorageApiMismatchDrainRetryTimeMilliSec()} and routes remaining
   * unconvertible elements to the dead-letter output when the table schema is never widened.
   */
  @Test
  public void testSchemaUpdateHoldingFnWindowExpirationDrainsToDeadLetterOnTimeout()
      throws Exception {
    createNarrowTable();
    p.enableAbandonedNodeEnforcement(false);

    BigQueryOptions bqOptions = p.getOptions().as(BigQueryOptions.class);
    bqOptions.setStorageApiMismatchDrainRetryTimeMilliSec(500);

    StorageApiDynamicDestinations<TableRow, String> dynamicDestinations =
        new StorageApiDynamicDestinationsTableRow<>(
            new ConstantTableDynamicDestinations("project-id:dataset-id.table"),
            BigQueryIO.TableRowFormatFunction.fromSerializableFunction(tr -> tr),
            null,
            false,
            BigQueryIO.Write.CreateDisposition.CREATE_NEVER,
            false,
            false,
            true);

    TupleTag<KV<String, StorageApiWritePayload>> successfulWritesTag =
        new TupleTag<>("successfulWrites");
    TupleTag<BigQueryStorageApiInsertError> failedWritesTag = new TupleTag<>("failedWrites");
    TupleTag<KV<String, TableRow>> waitingTag = new TupleTag<>("waiting");
    TupleTag<KV<String, com.google.cloud.bigquery.storage.v1.TableSchema>> newSchemasTag =
        new TupleTag<>("newSchemas");
    TupleTag<BadRecord> badRecordTag = new TupleTag<>("badRecords");

    ConvertMessagesDoFn<String, TableRow> convertDoFn =
        new ConvertMessagesDoFn<>(
            dynamicDestinations,
            fakeBqServices,
            "StorageApiWrite",
            failedWritesTag,
            successfulWritesTag,
            newSchemasTag,
            waitingTag,
            null,
            BadRecordRouter.THROWING_ROUTER,
            KvCoder.of(StringUtf8Coder.of(), TableRowJsonCoder.of()),
            true);

    TestStream.Builder<Long> testStream =
        TestStream.create(VarLongCoder.of()).advanceWatermarkTo(new Instant(0));
    for (long i = 0; i < NUM_ROWS; i++) {
      testStream = testStream.addElements(i);
    }

    PCollectionTuple result =
        p.apply(testStream.advanceWatermarkToInfinity())
            .apply(
                "toShardedRow",
                MapElements.into(new TypeDescriptor<KV<ShardedKey<String>, TableRow>>() {})
                    .via(
                        (SerializableFunction<Long, KV<ShardedKey<String>, TableRow>>)
                            i ->
                                KV.of(
                                    StorageApiConvertMessages.AssignShardFn.getShardedKey(
                                        "project-id:dataset-id.table", 0, 1),
                                    rowWithUnknownField(i))))
            .setCoder(
                KvCoder.of(
                    ShardedKey.Coder.of(StringUtf8Coder.of()),
                    NullableCoder.of(TableRowJsonCoder.of())))
            .apply(
                "holdAndDrain",
                ParDo.of(new SchemaUpdateHoldingFn<>(TableRowJsonCoder.of(), convertDoFn))
                    .withOutputTags(
                        successfulWritesTag,
                        TupleTagList.of(ImmutableList.of(failedWritesTag, badRecordTag))));

    result
        .get(successfulWritesTag)
        .setCoder(
            KvCoder.of(
                StringUtf8Coder.of(),
                p.getSchemaRegistry().getSchemaCoder(StorageApiWritePayload.class)));
    result.get(failedWritesTag).setCoder(BigQueryStorageApiInsertErrorCoder.of());
    result.get(badRecordTag).setCoder(BadRecord.getCoder(p));

    PCollection<String> failedNames = failedRowNames(result.get(failedWritesTag));
    PAssert.that(
            "SchemaUpdateHoldingFn must dead-letter all buffered rows on drain timeout",
            failedNames)
        .containsInAnyOrder(expectedNames());

    p.run();
  }
}

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
package org.apache.beam.sdk.io.gcp.spanner;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.BatchableMutationFilterFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.GatherSortCreateBatchesFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteToSpannerFn;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests for {@link SpannerIO}.
 *
 * <p>Note that because batching and sorting work on Bundles, and the TestPipeline does not bundle
 * small numbers of elements, the batching and sorting DoFns need to be unit tested outside of the
 * pipeline.
 */
@RunWith(JUnit4.class)
public class SpannerIOWriteTest implements Serializable {
  private static final long CELLS_PER_KEY = 7;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Captor public transient ArgumentCaptor<Iterable<Mutation>> mutationBatchesCaptor;
  @Captor public transient ArgumentCaptor<Iterable<MutationGroup>> mutationGroupListCaptor;
  @Captor public transient ArgumentCaptor<MutationGroup> mutationGroupCaptor;

  private FakeServiceFactory serviceFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    serviceFactory = new FakeServiceFactory();

    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Capture batches sent to writeAtLeastOnce.
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(mutationBatchesCaptor.capture()))
        .thenReturn(null);

    // Simplest schema: a table with int64 key
    preparePkMetadata(tx, Arrays.asList(pkMetadata("tEsT", "key", "ASC")));
    prepareColumnMetadata(tx, Arrays.asList(columnMetadata("tEsT", "key", "INT64", CELLS_PER_KEY)));
  }

  private SpannerSchema getSchema() {
    return SpannerSchema.builder()
        .addColumn("tEsT", "key", "INT64", CELLS_PER_KEY)
        .addKeyPart("tEsT", "key", false)
        .build();
  }

  private static Struct columnMetadata(
      String tableName, String columnName, String type, long cellsMutated) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("spanner_type")
        .to(type)
        .set("cells_mutated")
        .to(cellsMutated)
        .build();
  }

  private static Struct pkMetadata(String tableName, String columnName, String ordering) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("column_ordering")
        .to(ordering)
        .build();
  }

  private void prepareColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("spanner_type", Type.string()),
            Type.StructField.of("cells_mutated", Type.int64()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Statement argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  private void preparePkMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type =
        Type.struct(
            Type.StructField.of("table_name", Type.string()),
            Type.StructField.of("column_name", Type.string()),
            Type.StructField.of("column_ordering", Type.string()));
    when(tx.executeQuery(
            argThat(
                new ArgumentMatcher<Statement>() {

                  @Override
                  public boolean matches(Statement argument) {
                    if (!(argument instanceof Statement)) {
                      return false;
                    }
                    Statement st = (Statement) argument;
                    return st.getSql().contains("information_schema.index_columns");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Write write = SpannerIO.write();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.expand(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.expand(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    write.expand(null);
  }

  @Test
  public void singleMutationPipeline() throws Exception {
    Mutation mutation = m(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory));
    pipeline.run();

    verifyBatches(batch(m(2L)));
  }

  @Test
  public void singleMutationGroupPipeline() throws Exception {
    PCollection<MutationGroup> mutations =
        pipeline.apply(Create.<MutationGroup>of(g(m(1L), m(2L), m(3L))));
    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory)
            .grouped());
    pipeline.run();

    verifyBatches(batch(m(1L), m(2L), m(3L)));
  }

  private void verifyBatches(Iterable<Mutation>... batches) {
    for (Iterable<Mutation> b : batches) {
      verify(serviceFactory.mockDatabaseClient(), times(1)).writeAtLeastOnce(mutationsInNoOrder(b));
    }
  }

  @Test
  public void noBatching() throws Exception {

    // This test uses a different mock/fake because it explicitly does not want to populate the
    // Spanner schema.
    FakeServiceFactory fakeServiceFactory = new FakeServiceFactory();
    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(fakeServiceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Capture batches sent to writeAtLeastOnce.
    when(fakeServiceFactory.mockDatabaseClient().writeAtLeastOnce(mutationBatchesCaptor.capture()))
        .thenReturn(null);

    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(g(m(1L)), g(m(2L))));
    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(fakeServiceFactory)
            .withBatchSizeBytes(1)
            .grouped());
    pipeline.run();

    verify(fakeServiceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(mutationsInNoOrder(batch(m(1L))));
    verify(fakeServiceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(mutationsInNoOrder(batch(m(2L))));
    // If no batching then the DB schema is never read.
    verify(tx, never()).executeQuery(any());
  }

  @Test
  public void streamingWrites() throws Exception {
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(m(1L), m(2L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(m(3L), m(4L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(m(5L), m(6L))
            .advanceWatermarkToInfinity();
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance")
                .withDatabaseId("test-database")
                .withServiceFactory(serviceFactory));
    pipeline.run();

    verifyBatches(batch(m(1L), m(2L)), batch(m(3L), m(4L)), batch(m(5L), m(6L)));
  }

  @Test
  public void streamingWritesWithGrouping() throws Exception {

    // verify that grouping/sorting occurs when set.
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(m(1L), m(5L), m(2L), m(4L), m(3L), m(6L))
            .advanceWatermarkToInfinity();
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance")
                .withDatabaseId("test-database")
                .withServiceFactory(serviceFactory)
                .withGroupingFactor(40)
                .withMaxNumRows(2));
    pipeline.run();

    // Output should be batches of sorted mutations.
    verifyBatches(batch(m(1L), m(2L)), batch(m(3L), m(4L)), batch(m(5L), m(6L)));
  }

  @Test
  public void streamingWritesNoGrouping() throws Exception {

    // verify that grouping/sorting does not occur - batches should be created in received order.
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(m(1L), m(5L), m(2L), m(4L), m(3L), m(6L))
            .advanceWatermarkToInfinity();

    // verify that grouping/sorting does not occur when notset.
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance")
                .withDatabaseId("test-database")
                .withServiceFactory(serviceFactory)
                .withMaxNumRows(2));
    pipeline.run();

    verifyBatches(batch(m(1L), m(5L)), batch(m(2L), m(4L)), batch(m(3L), m(6L)));
  }

  @Test
  public void reportFailures() throws Exception {

    MutationGroup[] mutationGroups = new MutationGroup[10];
    for (int i = 0; i < mutationGroups.length; i++) {
      mutationGroups[i] = g(m((long) i));
    }

    List<MutationGroup> mutationGroupList = Arrays.asList(mutationGroups);

    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenAnswer(
            invocationOnMock -> {
              Preconditions.checkNotNull(invocationOnMock.getArguments()[0]);
              throw SpannerExceptionFactory.newSpannerException(ErrorCode.ALREADY_EXISTS, "oops");
            });

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationGroupList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES)
                    .grouped());
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(mutationGroups.length, Iterables.size(m));
              return null;
            });
    PAssert.that(result.getFailedMutations()).containsInAnyOrder(mutationGroupList);
    pipeline.run().waitUntilFinish();

    // writeAtLeastOnce called once for the batch of mutations
    // (which as they are unbatched = each mutation group) then again for the individual retry.
    verify(serviceFactory.mockDatabaseClient(), times(20)).writeAtLeastOnce(any());
  }

  @Test
  public void deadlineExceededRetries() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(m((long) 1));

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with 2 timeouts and a success.
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 1"))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 2"))
        .thenReturn(Timestamp.now());

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES));

    // all success, so veryify no errors
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(0, Iterables.size(m));
              return null;
            });
    pipeline.run().waitUntilFinish();

    // 2 calls to sleeper
    verify(WriteToSpannerFn.sleeper, times(2)).sleep(anyLong());
    // 3 write attempts for the single mutationGroup.
    verify(serviceFactory.mockDatabaseClient(), times(3)).writeAtLeastOnce(any());
  }

  @Test
  public void deadlineExceededFailsAfterRetries() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(m((long) 1));

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with all timeouts.
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout"));

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withMaxCumulativeBackoff(Duration.standardHours(2))
                    .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES));

    // One error
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(1, Iterables.size(m));
              return null;
            });
    pipeline.run().waitUntilFinish();

    // Due to jitter in backoff algorithm, we cannot test for an exact number of retries,
    // but there will be more than 16 (normally 18).
    int numSleeps = Mockito.mockingDetails(WriteToSpannerFn.sleeper).getInvocations().size();
    assertTrue(String.format("Should be least 16 sleeps, got %d", numSleeps), numSleeps > 16);
    long totalSleep =
        Mockito.mockingDetails(WriteToSpannerFn.sleeper).getInvocations().stream()
            .mapToLong(i -> i.getArgument(0))
            .reduce(0L, Long::sum);

    // Total sleep should be greater then 2x maxCumulativeBackoff: 120m,
    // because the batch is repeated inidividually due REPORT_FAILURES.
    assertTrue(
        String.format("Should be least 7200s of sleep, got %d", totalSleep),
        totalSleep >= Duration.standardHours(2).getMillis());

    // Number of write attempts should be numSleeps + 2 write attempts:
    //      1 batch attempt, numSleeps/2 batch retries,
    // then 1 individual attempt + numSleeps/2 individual retries
    verify(serviceFactory.mockDatabaseClient(), times(numSleeps + 2)).writeAtLeastOnce(any());
  }

  @Test
  public void retryOnSchemaChangeException() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(m((long) 1));

    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with 2 timeouts and a success.
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenReturn(Timestamp.now());

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(FailureMode.FAIL_FAST));

    // all success, so veryify no errors
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(0, Iterables.size(m));
              return null;
            });
    pipeline.run().waitUntilFinish();

    // 0 calls to sleeper
    verify(WriteToSpannerFn.sleeper, times(0)).sleep(anyLong());
    // 3 write attempts for the single mutationGroup.
    verify(serviceFactory.mockDatabaseClient(), times(3)).writeAtLeastOnce(any());
  }

  @Test
  public void retryMaxOnSchemaChangeException() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(m((long) 1));

    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // Respond with Aborted transaction
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString));

    // When spanner aborts transaction for more than 5 time, pipeline execution stops with
    // PipelineExecutionException
    thrown.expect(PipelineExecutionException.class);
    thrown.expectMessage(errString);

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(FailureMode.FAIL_FAST));

    // One error
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(1, Iterables.size(m));
              return null;
            });
    pipeline.run().waitUntilFinish();

    // 0 calls to sleeper
    verify(WriteToSpannerFn.sleeper, times(0)).sleep(anyLong());
    // 5 write attempts for the single mutationGroup.
    verify(serviceFactory.mockDatabaseClient(), times(5)).writeAtLeastOnce(any());
  }

  @Test
  public void retryOnAbortedAndDeadlineExceeded() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(m((long) 1));

    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // Respond with (1) Aborted transaction a couple of times (2) deadline exceeded
    // (3) Aborted transaction 3 times (4)  deadline exceeded and finally return success.
    when(serviceFactory.mockDatabaseClient().writeAtLeastOnce(any()))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 1"))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 2"))
        .thenReturn(Timestamp.now());

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withProjectId("test-project")
                    .withInstanceId("test-instance")
                    .withDatabaseId("test-database")
                    .withServiceFactory(serviceFactory)
                    .withBatchSizeBytes(0)
                    .withFailureMode(FailureMode.FAIL_FAST));

    // Zero error
    PAssert.that(result.getFailedMutations())
        .satisfies(
            m -> {
              assertEquals(0, Iterables.size(m));
              return null;
            });
    pipeline.run().waitUntilFinish();

    // 2 calls to sleeper
    verify(WriteToSpannerFn.sleeper, times(2)).sleep(anyLong());
    // 8 write attempts for the single mutationGroup.
    verify(serviceFactory.mockDatabaseClient(), times(8)).writeAtLeastOnce(any());
  }

  @Test
  public void displayDataWrite() throws Exception {
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(123)
            .withMaxNumMutations(456)
            .withMaxNumRows(789)
            .withGroupingFactor(100);

    DisplayData data = DisplayData.from(write);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("projectId", "test-project"));
    assertThat(data, hasDisplayItem("instanceId", "test-instance"));
    assertThat(data, hasDisplayItem("databaseId", "test-database"));
    assertThat(data, hasDisplayItem("batchSizeBytes", 123));
    assertThat(data, hasDisplayItem("maxNumMutations", 456));
    assertThat(data, hasDisplayItem("maxNumRows", 789));
    assertThat(data, hasDisplayItem("groupingFactor", "100"));

    // check for default grouping value
    write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database");

    data = DisplayData.from(write);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("groupingFactor", "DEFAULT"));
  }

  @Test
  public void displayDataWriteGrouped() throws Exception {
    SpannerIO.WriteGrouped writeGrouped =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(123)
            .withMaxNumMutations(456)
            .withMaxNumRows(789)
            .withGroupingFactor(100)
            .grouped();

    DisplayData data = DisplayData.from(writeGrouped);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("projectId", "test-project"));
    assertThat(data, hasDisplayItem("instanceId", "test-instance"));
    assertThat(data, hasDisplayItem("databaseId", "test-database"));
    assertThat(data, hasDisplayItem("batchSizeBytes", 123));
    assertThat(data, hasDisplayItem("maxNumMutations", 456));
    assertThat(data, hasDisplayItem("maxNumRows", 789));
    assertThat(data, hasDisplayItem("groupingFactor", "100"));

    // check for default grouping value
    writeGrouped =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .grouped();

    data = DisplayData.from(writeGrouped);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("groupingFactor", "DEFAULT"));
  }

  @Test
  public void testBatchableMutationFilterFn_cells() {
    Mutation all = Mutation.delete("test", KeySet.all());
    Mutation prefix = Mutation.delete("test", KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            "test", KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          g(m(1L)),
          g(m(2L), m(3L)),
          g(m(2L), m(3L), m(4L), m(5L)), // not batchable - too big.
          g(del(1L)),
          g(del(5L, 6L)), // not point delete.
          g(all),
          g(prefix),
          g(range)
        };

    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, 10000000, 3 * CELLS_PER_KEY, 1000);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(g(m(1L)), g(m(2L), m(3L)), g(del(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            g(m(2L), m(3L), m(4L), m(5L)), // not batchable - too big.
            g(del(5L, 6L)), // not point delete.
            g(all),
            g(prefix),
            g(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_size() {
    Mutation all = Mutation.delete("test", KeySet.all());
    Mutation prefix = Mutation.delete("test", KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            "test", KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          g(m(1L)),
          g(m(2L), m(3L)),
          g(m(1L), m(3L), m(4L), m(5L)), // not batchable - too big.
          g(del(1L)),
          g(del(5L, 6L)), // not point delete.
          g(all),
          g(prefix),
          g(range)
        };

    long mutationSize = MutationSizeEstimator.sizeOf(m(1L));
    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, mutationSize * 3, 1000, 1000);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(g(m(1L)), g(m(2L), m(3L)), g(del(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            g(m(1L), m(3L), m(4L), m(5L)), // not batchable - too big.
            g(del(5L, 6L)), // not point delete.
            g(all),
            g(prefix),
            g(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_rows() {
    Mutation all = Mutation.delete("test", KeySet.all());
    Mutation prefix = Mutation.delete("test", KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            "test", KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          g(m(1L)),
          g(m(2L), m(3L)),
          g(m(1L), m(3L), m(4L), m(5L)), // not batchable - too many rows.
          g(del(1L)),
          g(del(5L, 6L)), // not point delete.
          g(all),
          g(prefix),
          g(range)
        };

    long mutationSize = MutationSizeEstimator.sizeOf(m(1L));
    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 1000, 1000, 3);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertThat(
        mutationGroupCaptor.getAllValues(),
        containsInAnyOrder(g(m(1L)), g(m(2L), m(3L)), g(del(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            g(m(1L), m(3L), m(4L), m(5L)), // not batchable - too many rows.
            g(del(5L, 6L)), // not point delete.
            g(all),
            g(prefix),
            g(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_batchingDisabled() {
    MutationGroup[] mutationGroups =
        new MutationGroup[] {g(m(1L)), g(m(2L)), g(del(1L)), g(del(5L, 6L))};

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 0, 0, 0);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupCaptor.capture());
    doNothing().when(mockProcessContext).output(any(), mutationGroupListCaptor.capture());

    // Process all elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }

    // Verify captured batchable elements.
    assertTrue(mutationGroupCaptor.getAllValues().isEmpty());

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(unbatchableMutations, containsInAnyOrder(mutationGroups));
  }

  @Test
  public void testGatherSortAndBatchFn() throws Exception {

    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            100, // batch up to 35 mutated cells.
            5, // batch rows
            100, // groupingFactor
            null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    FinishBundleContext mockFinishBundleContext = Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          // Unsorted group of 12 mutations.
          // each mutation is considered 7 cells,
          // should be sorted and output as 2 lists of 5, then 1 list of 2
          // with mutations sorted in order.
          g(m(4L)),
          g(m(1L)),
          g(m(7L)),
          g(m(12L)),
          g(m(10L)),
          g(m(11L)),
          g(m(2L)),
          g(del(8L)),
          g(m(3L)),
          g(m(6L)),
          g(m(9L)),
          g(m(5L))
        };

    // Process all elements as one bundle.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      // outputReceiver should not be called until end of bundle.
      testFn.processElement(mockProcessContext, null);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockProcessContext, never()).output(any());
    verify(mockFinishBundleContext, times(3)).output(any(), any(), any());

    // Verify output are 3 batches of sorted values
    assertThat(
        mutationGroupListCaptor.getAllValues(),
        contains(
            Arrays.asList(g(m(1L)), g(m(2L)), g(m(3L)), g(m(4L)), g(m(5L))),
            Arrays.asList(g(m(6L)), g(m(7L)), g(del(8L)), g(m(9L)), g(m(10L))),
            Arrays.asList(g(m(11L)), g(m(12L)))));
  }

  @Test
  public void testGatherBundleAndSortFn_flushOversizedBundle() throws Exception {

    // Setup class to bundle every 6 rows and create batches of 2.
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            100, // batch up to 14 mutated cells.
            2, // batch rows
            3, // groupingFactor
            null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    FinishBundleContext mockFinishBundleContext = Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());
    OutputReceiver<Iterable<MutationGroup>> mockOutputReceiver = mock(OutputReceiver.class);

    // Capture the outputs.
    doNothing().when(mockOutputReceiver).output(mutationGroupListCaptor.capture());
    // Capture the outputs.
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          // Unsorted group of 12 mutations.
          // each mutation is considered 7 cells,
          // should be sorted and output as 2 lists of 5, then 1 list of 2
          // with mutations sorted in order.
          g(m(4L)),
          g(m(1L)),
          g(m(7L)),
          g(m(9L)),
          g(m(10L)),
          g(m(11L)),
          // end group
          g(m(2L)),
          g(del(8L)), // end batch
          g(m(3L)),
          g(m(6L)), // end batch
          g(m(5L))
          // end bundle, so end group and end batch.
        };

    // Process all elements as one bundle.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext, mockOutputReceiver);
    }
    testFn.finishBundle(mockFinishBundleContext);

    // processElement ouput receiver should have been called 3 times when the 1st group was full.
    verify(mockOutputReceiver, times(3)).output(any());
    // finsihBundleContext output should be called 3 times when the bundle was finished.
    verify(mockFinishBundleContext, times(3)).output(any(), any(), any());

    List<Iterable<MutationGroup>> mgListGroups = mutationGroupListCaptor.getAllValues();

    assertEquals(6, mgListGroups.size());
    // verify contents of 6 sorted groups.
    // first group should be 1,3,4,7,9,11
    assertThat(mgListGroups.get(0), contains(g(m(1L)), g(m(4L))));
    assertThat(mgListGroups.get(1), contains(g(m(7L)), g(m(9L))));
    assertThat(mgListGroups.get(2), contains(g(m(10L)), g(m(11L))));

    // second group at finishBundle should be 2,3,5,6,8
    assertThat(mgListGroups.get(3), contains(g(m(2L)), g(m(3L))));
    assertThat(mgListGroups.get(4), contains(g(m(5L)), g(m(6L))));
    assertThat(mgListGroups.get(5), contains(g(del(8L))));
  }

  @Test
  public void testBatchFn_cells() throws Exception {

    // Setup class to batch every 3 mutations (3xCELLS_PER_KEY cell mutations)
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000000, // batch bytes
            3 * CELLS_PER_KEY, // batch up to 21 mutated cells - 3 mutations.
            100, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  @Test
  public void testBatchFn_size() throws Exception {

    long mutationSize = MutationSizeEstimator.sizeOf(m(1L));

    // Setup class to bundle every 3 mutations by size)
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            mutationSize * 3, // batch bytes = 3 mutations.
            100, // batch cells
            100, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  @Test
  public void testBatchFn_rows() throws Exception {

    // Setup class to bundle every 3 rows
    GatherSortCreateBatchesFn testFn =
        new GatherSortCreateBatchesFn(
            10000, // batch bytes = 3 mutations.
            100, // batch cells
            3, // batch rows
            100, // groupingFactor
            null);

    testAndVerifyBatches(testFn);
  }

  private void testAndVerifyBatches(GatherSortCreateBatchesFn testFn) throws Exception {
    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    FinishBundleContext mockFinishBundleContext = Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the output at finish bundle..
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    List<MutationGroup> mutationGroups =
        Arrays.asList(
            g(m(1L)),
            g(m(4L)),
            g(m(5L), m(6L), m(7L), m(8L), m(9L)),
            g(m(3L)),
            g(m(10L)),
            g(m(11L)),
            g(m(2L)));

    // Process elements.
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext, null);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockFinishBundleContext, times(4)).output(any(), any(), any());

    List<Iterable<MutationGroup>> batches = mutationGroupListCaptor.getAllValues();
    assertEquals(4, batches.size());

    // verify contents of 4 batches.
    assertThat(batches.get(0), contains(g(m(1L)), g(m(2L)), g(m(3L))));
    assertThat(batches.get(1), contains(g(m(4L)))); // small batch : next mutation group is too big.
    assertThat(batches.get(2), contains(g(m(5L), m(6L), m(7L), m(8L), m(9L))));
    assertThat(batches.get(3), contains(g(m(10L)), g(m(11L))));
  }

  @Test
  public void testRefCountedSpannerAccessorOnlyOnce() {
    SpannerConfig config1 =
        SpannerConfig.create()
            .toBuilder()
            .setServiceFactory(serviceFactory)
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();

    SpannerIO.WriteToSpannerFn test1Fn =
        new SpannerIO.WriteToSpannerFn(config1, FailureMode.REPORT_FAILURES, null /* failedTag */);
    SpannerIO.WriteToSpannerFn test2Fn =
        new SpannerIO.WriteToSpannerFn(config1, FailureMode.REPORT_FAILURES, null /* failedTag */);
    SpannerIO.WriteToSpannerFn test3Fn =
        new SpannerIO.WriteToSpannerFn(config1, FailureMode.REPORT_FAILURES, null /* failedTag */);

    test1Fn.setup();
    test2Fn.setup();
    test3Fn.setup();

    test2Fn.teardown();
    test3Fn.teardown();
    test1Fn.teardown();

    // getDatabaseClient and close() only called once.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(DatabaseId.of("project", "test1", "test1"));
    verify(serviceFactory.mockSpanner(), times(1)).close();
  }

  @Test
  public void testRefCountedSpannerAccessorDifferentDbsOnlyOnce() {
    SpannerConfig config1 =
        SpannerConfig.create()
            .toBuilder()
            .setServiceFactory(serviceFactory)
            .setMaxCumulativeBackoff(StaticValueProvider.of(Duration.standardSeconds(10)))
            .setProjectId(StaticValueProvider.of("project"))
            .setInstanceId(StaticValueProvider.of("test1"))
            .setDatabaseId(StaticValueProvider.of("test1"))
            .build();
    SpannerConfig config2 =
        config1
            .toBuilder()
            .setInstanceId(StaticValueProvider.of("test2"))
            .setDatabaseId(StaticValueProvider.of("test2"))
            .build();

    SpannerIO.WriteToSpannerFn test1Fn =
        new SpannerIO.WriteToSpannerFn(config1, FailureMode.REPORT_FAILURES, null /* failedTag */);
    SpannerIO.WriteToSpannerFn test2Fn =
        new SpannerIO.WriteToSpannerFn(config1, FailureMode.REPORT_FAILURES, null /* failedTag */);

    SpannerIO.WriteToSpannerFn test3Fn =
        new SpannerIO.WriteToSpannerFn(config2, FailureMode.REPORT_FAILURES, null /* failedTag */);
    SpannerIO.WriteToSpannerFn test4Fn =
        new SpannerIO.WriteToSpannerFn(config2, FailureMode.REPORT_FAILURES, null /* failedTag */);

    test1Fn.setup();
    test2Fn.setup();
    test3Fn.setup();
    test4Fn.setup();

    test2Fn.teardown();
    test3Fn.teardown();
    test4Fn.teardown();
    test1Fn.teardown();

    // getDatabaseClient called once each for the separate instances.
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(eq(DatabaseId.of("project", "test1", "test1")));
    verify(serviceFactory.mockSpanner(), times(1))
        .getDatabaseClient(eq(DatabaseId.of("project", "test2", "test2")));
    verify(serviceFactory.mockSpanner(), times(2)).close();
  }

  private static MutationGroup g(Mutation m, Mutation... other) {
    return MutationGroup.create(m, other);
  }

  private static Mutation m(Long key) {
    return Mutation.newInsertOrUpdateBuilder("test").set("key").to(key).build();
  }

  private static Iterable<Mutation> batch(Mutation... m) {
    return Arrays.asList(m);
  }

  private static Mutation del(Long... keys) {

    KeySet.Builder builder = KeySet.newBuilder();
    for (Long key : keys) {
      builder.addKey(Key.of(key));
    }
    return Mutation.delete("test", builder.build());
  }

  private static Mutation delRange(Long start, Long end) {
    return Mutation.delete("test", KeySet.range(KeyRange.closedClosed(Key.of(start), Key.of(end))));
  }

  private static Iterable<Mutation> mutationsInNoOrder(Iterable<Mutation> expected) {
    final ImmutableSet<Mutation> mutations = ImmutableSet.copyOf(expected);
    return argThat(
        new ArgumentMatcher<Iterable<Mutation>>() {

          @Override
          public boolean matches(Iterable<Mutation> argument) {
            if (!(argument instanceof Iterable)) {
              return false;
            }
            ImmutableSet<Mutation> actual = ImmutableSet.copyOf((Iterable) argument);
            return actual.equals(mutations);
          }

          @Override
          public String toString() {
            return "Iterable must match " + mutations;
          }
        });
  }

  private Iterable<Mutation> iterableOfSize(final int size) {
    return argThat(
        new ArgumentMatcher<Iterable<Mutation>>() {

          @Override
          public boolean matches(Iterable<Mutation> argument) {
            return argument instanceof Iterable && Iterables.size((Iterable<?>) argument) == size;
          }

          @Override
          public String toString() {
            return "The size of the iterable must equal " + size;
          }
        });
  }
}

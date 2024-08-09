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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.BatchReadOnlyTransaction;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.FakeBatchTransactionId;
import com.google.cloud.spanner.FakePartitionFactory;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options.ReadAndQueryOption;
import com.google.cloud.spanner.Options.ReadQueryUpdateTransactionOption;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.protobuf.ByteString;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link SpannerIO}. */
@RunWith(JUnit4.class)
public class SpannerIOReadTest implements Serializable {

  private static final TimestampBound TIMESTAMP_BOUND =
      TimestampBound.ofReadTimestamp(Timestamp.ofTimeMicroseconds(12345));
  public static final String PROJECT_ID = "1234";
  public static final String INSTANCE_ID = "123";
  public static final String DATABASE_ID = "aaa";
  public static final String TABLE_ID = "users";
  public static final String QUERY_NAME = "My-query";
  public static final String QUERY_STATEMENT = "SELECT * FROM users";

  @Rule
  public final transient TestPipeline pipeline =
      TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;
  private BatchReadOnlyTransaction mockBatchTx;
  private Partition fakePartition;
  private SpannerConfig spannerConfig;

  private static final Type FAKE_TYPE =
      Type.struct(
          Type.StructField.of("id", Type.int64()), Type.StructField.of("name", Type.string()));

  private static final List<Struct> FAKE_ROWS =
      Arrays.asList(
          Struct.newBuilder().set("id").to(Value.int64(1)).set("name").to("Alice").build(),
          Struct.newBuilder().set("id").to(Value.int64(2)).set("name").to("Bob").build(),
          Struct.newBuilder().set("id").to(Value.int64(3)).set("name").to("Carl").build(),
          Struct.newBuilder().set("id").to(Value.int64(4)).set("name").to("Dan").build(),
          Struct.newBuilder().set("id").to(Value.int64(5)).set("name").to("Evan").build(),
          Struct.newBuilder().set("id").to(Value.int64(6)).set("name").to("Floyd").build());

  @Before
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
    mockBatchTx = Mockito.mock(BatchReadOnlyTransaction.class);
    fakePartition = FakePartitionFactory.createFakeQueryPartition(ByteString.copyFromUtf8("one"));
    spannerConfig =
        SpannerConfig.create()
            .withProjectId(PROJECT_ID)
            .withInstanceId(INSTANCE_ID)
            .withDatabaseId(DATABASE_ID)
            .withServiceFactory(serviceFactory);

    // Setup the common mocks.
    when(mockBatchTx.getBatchTransactionId())
        .thenReturn(new FakeBatchTransactionId("runQueryTest"));
    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(TIMESTAMP_BOUND))
        .thenReturn(mockBatchTx);
    when(serviceFactory.mockBatchClient().batchReadOnlyTransaction(any(BatchTransactionId.class)))
        .thenReturn(mockBatchTx);

    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    MetricsEnvironment.setCurrentContainer(container);
  }

  @Test
  public void runBatchQueryTestWithSpannerConfig() {
    runBatchQueryTest(
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runWithQueryAndWithTableAtTheSameTimeFails() {
    SpannerIO.Read read =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "Both query and table cannot be specified at the same time for SpannerIO.read().");
    runBatchQueryTest(read);
  }

  @Test
  public void runBatchQueryTestWithUnspecifiedProject() {
    // Default spannerConfig has project ID specified - use an unspecified project.
    runBatchQueryTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runBatchQueryTestWithNullProject() {
    runBatchQueryTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withProjectId((String) null)
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runBatchQueryTestWithPriority() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND)
            .withHighPriority();
    runBatchQueryTest(readTransform);
    assertEquals(RpcPriority.HIGH, readTransform.getSpannerConfig().getRpcPriority().get());
  }

  @Test
  public void runBatchQueryTestWithDataBoost() {
    SpannerConfig spannerConfig1 = spannerConfig.withDataBoostEnabled(StaticValueProvider.of(true));
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig1)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND);
    runBatchQueryTest(readTransform);
  }

  private void runBatchQueryTest(SpannerIO.Read readTransform) {
    PCollection<Struct> results = pipeline.apply("read q", readTransform);

    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class),
            eq(Statement.of(QUERY_STATEMENT)),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyQueryRequestMetricWasSet(readTransform.getSpannerConfig(), QUERY_NAME, "ok", 4);
  }

  @Test
  public void runBatchQueryTestWithFailures() {
    pipeline.apply(
        "read q",
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));

    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class),
            eq(Statement.of(QUERY_STATEMENT)),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.PERMISSION_DENIED, "Simulated Failure"));

    assertThrows(
        "PERMISSION_DENIED: Simulated Failure", PipelineExecutionException.class, pipeline::run);
    // Query request should succeed at lease once (for partition query) and one execute may succeed.
    assertTrue(getQueryRequestMetric(spannerConfig, QUERY_NAME, "ok") >= 1);
    verifyQueryRequestMetricWasSet(spannerConfig, QUERY_NAME, "permission_denied", 1);
  }

  @Test
  public void runNaiveQueryTestWithProjectId() {
    runNaiveQueryTest(
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveQueryTestWithUnspecifiedProject() {
    // Default spannerConfig has project ID specified - use an unspecified project.
    runNaiveQueryTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveQueryTestWithNullProject() {
    runNaiveQueryTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withProjectId((String) null)
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveQueryTestWithPriority() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND)
            .withHighPriority();
    runNaiveQueryTest(readTransform);
    assertEquals(RpcPriority.HIGH, readTransform.getSpannerConfig().getRpcPriority().get());
  }

  private void runNaiveQueryTest(SpannerIO.Read readTransform) {
    readTransform = readTransform.withBatching(false);
    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.executeQuery(
            eq(Statement.of(QUERY_STATEMENT)), any(ReadQueryUpdateTransactionOption.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyQueryRequestMetricWasSet(readTransform.getSpannerConfig(), QUERY_NAME, "ok", 1);
  }

  @Test
  public void runNaiveQueryTestWithAnonymousQuery() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withTimestampBound(TIMESTAMP_BOUND)
            .withHighPriority()
            .withBatching(false);
    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.executeQuery(
            eq(Statement.of(QUERY_STATEMENT)), any(ReadQueryUpdateTransactionOption.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    String queryName = String.format("UNNAMED_QUERY#%08x", QUERY_STATEMENT.hashCode());
    verifyQueryRequestMetricWasSet(spannerConfig, queryName, "ok", 1);
  }

  @Test
  public void runNaiveQueryTestWithFailures() {

    pipeline.apply(
        "read q",
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withQuery(QUERY_STATEMENT)
            .withQueryName(QUERY_NAME)
            .withTimestampBound(TIMESTAMP_BOUND)
            .withBatching(false));
    when(mockBatchTx.executeQuery(
            eq(Statement.of(QUERY_STATEMENT)), any(ReadQueryUpdateTransactionOption.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.PERMISSION_DENIED, "Simulated Failure"));
    assertThrows(
        "PERMISSION_DENIED: Simulated Failure", PipelineExecutionException.class, pipeline::run);
    verifyQueryRequestMetricWasSet(spannerConfig, QUERY_NAME, "permission_denied", 1);
  }

  @Test
  public void runBatchReadTestWithProjectId() {
    runBatchReadTest(
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runBatchReadTestWithUnspecifiedProject() {
    // Default spannerConfig has project ID specified - use an unspecified project.
    runBatchReadTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runBatchReadTestWithNullProject() {
    runBatchReadTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withProjectId((String) null)
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runBatchReadTestWithPriority() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND)
            .withHighPriority();
    runBatchReadTest(readTransform);
    assertEquals(RpcPriority.HIGH, readTransform.getSpannerConfig().getRpcPriority().get());
  }

  @Test
  public void runBatchReadTestWithDataBoost() {
    SpannerConfig spannerConfig1 = spannerConfig.withDataBoostEnabled(StaticValueProvider.of(true));

    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig1)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND);
    runBatchReadTest(readTransform);
  }

  private void runBatchReadTest(SpannerIO.Read readTransform) {

    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(readTransform.getSpannerConfig(), TABLE_ID, "ok", 4);
  }

  @Test
  public void runBatchReadTestWithFailures() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND);

    pipeline.apply("read q", readTransform);

    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition));
    when(mockBatchTx.execute(any(Partition.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.PERMISSION_DENIED, "Simulated Failure"));

    assertThrows(
        "PERMISSION_DENIED: Simulated Failure", PipelineExecutionException.class, pipeline::run);

    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "ok", 1);
    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "permission_denied", 1);
  }

  @Test
  public void runNaiveReadTestWithProjectId() {
    runNaiveReadTest(
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveReadTestWithUnspecifiedProject() {
    // Default spannerConfig has project ID specified - use an unspecified project.
    runNaiveReadTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveReadTestWithNullProject() {
    runNaiveReadTest(
        SpannerIO.read()
            .withSpannerConfig(
                SpannerConfig.create()
                    .withProjectId((String) null)
                    .withInstanceId(INSTANCE_ID)
                    .withDatabaseId(DATABASE_ID)
                    .withServiceFactory(serviceFactory))
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND));
  }

  @Test
  public void runNaiveReadTestWithPriority() {
    SpannerIO.Read readTransform =
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND)
            .withHighPriority();
    runNaiveReadTest(readTransform);
    assertEquals(RpcPriority.HIGH, readTransform.getSpannerConfig().getRpcPriority().get());
  }

  private void runNaiveReadTest(SpannerIO.Read readTransform) {
    readTransform = readTransform.withBatching(false);

    PCollection<Struct> results = pipeline.apply("read q", readTransform);
    when(mockBatchTx.read(
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(readTransform.getSpannerConfig(), TABLE_ID, "ok", 1);
  }

  @Test
  public void runNaiveReadTestWithFailures() {

    pipeline.apply(
        "read q",
        SpannerIO.read()
            .withSpannerConfig(spannerConfig)
            .withTable(TABLE_ID)
            .withColumns("id", "name")
            .withTimestampBound(TIMESTAMP_BOUND)
            .withBatching(false));

    when(mockBatchTx.read(
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.PERMISSION_DENIED, "Simulated Failure"));

    assertThrows(
        "PERMISSION_DENIED: Simulated Failure", PipelineExecutionException.class, pipeline::run);
    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "permission_denied", 1);
  }

  @Test
  public void runBatchReadUsingIndex() {
    PCollection<Struct> one =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withTimestamp(Timestamp.now())
                .withSpannerConfig(spannerConfig)
                .withTable(TABLE_ID)
                .withColumns("id", "name")
                .withIndex("theindex")
                .withTimestampBound(TIMESTAMP_BOUND));

    when(mockBatchTx.partitionReadUsingIndex(
            any(PartitionOptions.class),
            eq(TABLE_ID),
            eq("theindex"),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition, fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(one).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "ok", 4);
  }

  @Test
  public void runNaiveReadUsingIndex() {
    PCollection<Struct> results =
        pipeline.apply(
            "read q",
            SpannerIO.read()
                .withTimestamp(Timestamp.now())
                .withSpannerConfig(spannerConfig)
                .withTable(TABLE_ID)
                .withColumns("id", "name")
                .withIndex("theindex")
                .withTimestampBound(TIMESTAMP_BOUND)
                .withBatching(false));

    when(mockBatchTx.readUsingIndex(
            eq(TABLE_ID),
            eq("theindex"),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class)))
        .thenReturn(ResultSets.forRows(FAKE_TYPE, FAKE_ROWS));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "ok", 1);
  }

  @Test
  public void readAllPipelineWithSpannerReadAllConfiguration() {
    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TIMESTAMP_BOUND));
    runReadAllPipeline(
        SpannerIO.readAll()
            .withProjectId(PROJECT_ID)
            .withInstanceId(INSTANCE_ID)
            .withDatabaseId(DATABASE_ID)
            .withServiceFactory(serviceFactory)
            .withLowPriority()
            .withTransaction(tx));
  }

  @Test
  public void readAllPipelineWithSpannerReadAllConfigurationAsValueProviders() {
    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TIMESTAMP_BOUND));
    runReadAllPipeline(
        SpannerIO.readAll()
            .withProjectId(StaticValueProvider.of(PROJECT_ID))
            .withInstanceId(StaticValueProvider.of(INSTANCE_ID))
            .withDatabaseId(StaticValueProvider.of(DATABASE_ID))
            .withServiceFactory(serviceFactory)
            .withHighPriority()
            .withTransaction(tx));
  }

  @Test
  public void readAllPipelineWithSpannerCreationTransactionConfiguration() {
    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withProjectId(PROJECT_ID)
                .withInstanceId(INSTANCE_ID)
                .withDatabaseId(DATABASE_ID)
                .withServiceFactory(serviceFactory)
                .withTimestampBound(TIMESTAMP_BOUND));
    runReadAllPipeline(SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));
  }

  @Test
  public void readAllPipelineWithSpannerCreationTransactionConfigurationAsValueProviders() {
    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withProjectId(StaticValueProvider.of(PROJECT_ID))
                .withInstanceId(StaticValueProvider.of(INSTANCE_ID))
                .withDatabaseId(StaticValueProvider.of(DATABASE_ID))
                .withServiceFactory(serviceFactory)
                .withTimestampBound(TIMESTAMP_BOUND));
    runReadAllPipeline(SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));
  }

  @Test
  public void readAllPipeline() {
    PCollectionView<Transaction> tx =
        pipeline.apply(
            "tx",
            SpannerIO.createTransaction()
                .withSpannerConfig(spannerConfig)
                .withTimestampBound(TIMESTAMP_BOUND));
    runReadAllPipeline(SpannerIO.readAll().withSpannerConfig(spannerConfig).withTransaction(tx));
  }

  private void runReadAllPipeline(SpannerIO.ReadAll readAllTransform) {
    PCollection<ReadOperation> reads =
        pipeline.apply(
            Create.of(
                ReadOperation.create().withQuery(QUERY_STATEMENT).withQueryName(QUERY_NAME),
                ReadOperation.create().withTable(TABLE_ID).withColumns("id", "name")));

    PCollection<Struct> results = reads.apply("read all", readAllTransform);

    when(mockBatchTx.partitionQuery(
            any(PartitionOptions.class),
            eq(Statement.of(QUERY_STATEMENT)),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Arrays.asList(fakePartition, fakePartition));
    when(mockBatchTx.partitionRead(
            any(PartitionOptions.class),
            eq(TABLE_ID),
            eq(KeySet.all()),
            eq(Arrays.asList("id", "name")),
            any(ReadQueryUpdateTransactionOption.class),
            any(ReadAndQueryOption.class)))
        .thenReturn(Collections.singletonList(fakePartition));

    when(mockBatchTx.execute(any(Partition.class)))
        .thenReturn(
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(0, 2)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(2, 4)),
            ResultSets.forRows(FAKE_TYPE, FAKE_ROWS.subList(4, 6)));

    PAssert.that(results).containsInAnyOrder(FAKE_ROWS);
    pipeline.run();
    verifyTableRequestMetricWasSet(spannerConfig, TABLE_ID, "ok", 2);
    verifyQueryRequestMetricWasSet(spannerConfig, QUERY_NAME, "ok", 3);
  }

  @Test
  public void runReadFailsToRetrieveSchema() {
    PCollection<Struct> spannerRows =
        pipeline.apply(
            SpannerIO.read()
                .withInstanceId(INSTANCE_ID)
                .withDatabaseId(DATABASE_ID)
                .withTable(TABLE_ID)
                .withColumns("id", "name"));

    Exception exception = assertThrows(IllegalStateException.class, spannerRows::getSchema);
    checkMessage("Cannot call getSchema when there is no schema", exception.getMessage());
  }

  private void checkMessage(String substring, @Nullable String message) {
    if (message != null) {
      assertThat(message, containsString(substring));
    } else {
      fail();
    }
  }

  private long getRequestMetricCount(HashMap<String, String> baseLabels) {
    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getCurrentContainer();
    return container.getCounter(name).getCumulative();
  }

  private long getTableRequestMetric(SpannerConfig config, String table, String status) {
    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");
    baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, table);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerTable(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            table));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);
    return getRequestMetricCount(baseLabels);
  }

  private long getQueryRequestMetric(SpannerConfig config, String queryName, String status) {
    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Read");
    baseLabels.put(MonitoringInfoConstants.Labels.SPANNER_QUERY_NAME, queryName);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerQuery(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            queryName));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);
    return getRequestMetricCount(baseLabels);
  }

  private void verifyTableRequestMetricWasSet(
      SpannerConfig config, String table, String status, long count) {
    assertEquals(count, getTableRequestMetric(config, table, status));
  }

  private void verifyQueryRequestMetricWasSet(
      SpannerConfig config, String queryName, String status, long count) {
    assertEquals(count, getQueryRequestMetric(config, queryName, status));
  }

  private HashMap<String, String> getBaseMetricsLabels(SpannerConfig config) {
    HashMap<String, String> baseLabels = new HashMap<>();
    baseLabels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    baseLabels.put(MonitoringInfoConstants.Labels.SERVICE, "Spanner");
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID,
        config.getProjectId() == null || config.getProjectId().get() == null
            ? SpannerOptions.getDefaultProjectId()
            : config.getProjectId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_INSTANCE_ID, config.getInstanceId().get());
    baseLabels.put(
        MonitoringInfoConstants.Labels.SPANNER_DATABASE_ID, config.getDatabaseId().get());
    return baseLabels;
  }
}

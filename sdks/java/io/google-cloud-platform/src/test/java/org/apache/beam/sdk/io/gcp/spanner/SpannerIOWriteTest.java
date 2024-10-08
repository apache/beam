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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
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
import com.google.cloud.spanner.CommitResponse;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.ReadQueryUpdateTransactionOption;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.OptionsImposter;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.runners.core.metrics.GcpResourceIdentifiers;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.BatchableMutationFilterFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.GatherSortCreateBatchesFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteToSpannerFn;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
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
  private static final String PROJECT_NAME = "test-project";
  private static final String INSTANCE_CONFIG_NAME = "regional-us-central1";
  private static final String INSTANCE_NAME = "test-instance";
  private static final String DATABASE_NAME = "test-database";
  private static final String TABLE_NAME = "test-table";
  private static final SpannerConfig SPANNER_CONFIG =
      SpannerConfig.create()
          .withDatabaseId(DATABASE_NAME)
          .withInstanceId(INSTANCE_NAME)
          .withProjectId(PROJECT_NAME);
  private static final String DEFAULT_PROJECT =
      Lineage.wrapSegment(SpannerOptions.getDefaultProjectId());

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();
  @Captor public transient ArgumentCaptor<Iterable<Mutation>> mutationBatchesCaptor;
  @Captor public transient ArgumentCaptor<ReadQueryUpdateTransactionOption> optionsCaptor;
  @Captor public transient ArgumentCaptor<Iterable<MutationGroup>> mutationGroupListCaptor;
  @Captor public transient ArgumentCaptor<MutationGroup> mutationGroupCaptor;

  private FakeServiceFactory serviceFactory;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    serviceFactory = new FakeServiceFactory();

    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Capture batches sent to writeAtLeastOnceWithOptions.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(mutationBatchesCaptor.capture(), optionsCaptor.capture()))
        .thenReturn(null);
    when(serviceFactory.mockInstance().getInstanceConfigId())
        .thenReturn(InstanceConfigId.of(PROJECT_NAME, INSTANCE_CONFIG_NAME));

    // Simplest schema: a table with int64 key
    // Verify case-insensitivity of table names by using different case for teble name.
    preparePkMetadata(tx, Arrays.asList(pkMetadata("tEsT-TaBlE", "key", "ASC")));
    prepareColumnMetadata(
        tx, Arrays.asList(columnMetadata("tEsT-TaBlE", "key", "INT64", CELLS_PER_KEY)));
    preparePgColumnMetadata(
        tx, Arrays.asList(columnMetadata("tEsT-TaBlE", "key", "bigint", CELLS_PER_KEY)));

    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
    MetricsEnvironment.setCurrentContainer(container);
  }

  private SpannerSchema getSchema() {
    return SpannerSchema.builder()
        .addColumn("tEsT-TaBlE", "key", "INT64", CELLS_PER_KEY)
        .addKeyPart("tEsT-TaBlE", "key", false)
        .build();
  }

  static Struct columnMetadata(
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

  static Struct pkMetadata(String tableName, String columnName, String ordering) {
    return Struct.newBuilder()
        .set("table_name")
        .to(tableName)
        .set("column_name")
        .to(columnName)
        .set("column_ordering")
        .to(ordering)
        .build();
  }

  static void prepareColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
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

  static void preparePgColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
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
                    return st.getSql().contains("information_schema.columns")
                        && st.getSql().contains("'public'");
                  }
                })))
        .thenReturn(ResultSets.forRows(type, rows));
  }

  static void preparePkMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
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
  public void runBatchQueryTestWithMaxCommitDelay() {
    SpannerIO.Write write =
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .withMaxCommitDelay(100L);
    assertEquals(100L, write.getSpannerConfig().getMaxCommitDelay().get().getMillis());

    Mutation mutation = buildUpsertMutation(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));
    mutations.apply(write);
    pipeline.run();

    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnceWithOptions(
            mutationsInNoOrder(buildMutationBatch(mutation)),
            any(ReadQueryUpdateTransactionOption.class),
            argThat(
                opts -> {
                  Options options = OptionsImposter.fromTransactionOptions(opts);
                  return java.time.Duration.ofMillis(100L)
                      .equals(OptionsImposter.maxCommitDelay(options));
                }));
  }

  @Test
  public void singleMutationPipeline() throws Exception {
    Mutation mutation = buildUpsertMutation(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

    mutations.apply(
        SpannerIO.write().withSpannerConfig(SPANNER_CONFIG).withServiceFactory(serviceFactory));
    PipelineResult result = pipeline.run();

    verifyBatches(buildMutationBatch(buildUpsertMutation(2L)));
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    PROJECT_NAME,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void singlePgMutationPipeline() throws Exception {
    Mutation mutation = buildUpsertMutation(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));
    PCollectionView<Dialect> pgDialectView =
        pipeline
            .apply("Create PG dialect", Create.of(Dialect.POSTGRESQL))
            .apply(View.asSingleton());

    mutations.apply(
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .withDialectView(pgDialectView));
    PipelineResult result = pipeline.run();

    verifyBatches(buildMutationBatch(buildUpsertMutation(2L)));
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    PROJECT_NAME,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void singleMutationPipelineNoProjectId() throws Exception {
    Mutation mutation = buildUpsertMutation(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

    SpannerConfig config =
        SpannerConfig.create().withInstanceId("test-instance").withDatabaseId("test-database");
    mutations.apply(SpannerIO.write().withSpannerConfig(config).withServiceFactory(serviceFactory));
    PipelineResult result = pipeline.run();

    // don't use VerifyBatches as that uses the common SPANNER_CONFIG with project ID:
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnceWithOptions(
            mutationsInNoOrder(buildMutationBatch(buildUpsertMutation(2L))),
            any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(config, TABLE_NAME, "ok", 1);
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    DEFAULT_PROJECT,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void singleMutationPipelineNullProjectId() throws Exception {
    Mutation mutation = buildUpsertMutation(2L);
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

    SpannerConfig config =
        SpannerConfig.create()
            .withProjectId((String) null)
            .withInstanceId("test-instance")
            .withDatabaseId("test-database");
    mutations.apply(SpannerIO.write().withSpannerConfig(config).withServiceFactory(serviceFactory));
    PipelineResult result = pipeline.run();

    // don't use VerifyBatches as that uses the common SPANNER_CONFIG with project ID:
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnceWithOptions(
            mutationsInNoOrder(buildMutationBatch(buildUpsertMutation(2L))),
            any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(config, TABLE_NAME, "ok", 1);
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    DEFAULT_PROJECT,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void singleMutationGroupPipeline() throws Exception {
    PCollection<MutationGroup> mutations =
        pipeline.apply(
            Create.<MutationGroup>of(
                buildMutationGroup(
                    buildUpsertMutation(1L), buildUpsertMutation(2L), buildUpsertMutation(3L))));
    mutations.apply(
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .grouped());
    PipelineResult result = pipeline.run();

    verifyBatches(
        buildMutationBatch(
            buildUpsertMutation(1L), buildUpsertMutation(2L), buildUpsertMutation(3L)));
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    PROJECT_NAME,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void singlePgMutationGroupPipeline() throws Exception {
    PCollection<MutationGroup> mutations =
        pipeline.apply(
            Create.<MutationGroup>of(
                buildMutationGroup(
                    buildUpsertMutation(1L), buildUpsertMutation(2L), buildUpsertMutation(3L))));
    PCollectionView<Dialect> pgDialectView =
        pipeline
            .apply("Create PG dialect", Create.of(Dialect.POSTGRESQL))
            .apply(View.asSingleton());

    mutations.apply(
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .withDialectView(pgDialectView)
            .grouped());
    PipelineResult result = pipeline.run();

    verifyBatches(
        buildMutationBatch(
            buildUpsertMutation(1L), buildUpsertMutation(2L), buildUpsertMutation(3L)));
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SINK),
        hasItem(
            Lineage.getFqName(
                "spanner",
                ImmutableList.of(
                    PROJECT_NAME,
                    INSTANCE_CONFIG_NAME,
                    INSTANCE_NAME,
                    DATABASE_NAME,
                    TABLE_NAME))));
  }

  @Test
  public void metricsForDifferentTables() throws Exception {
    Mutation mutation = buildUpsertMutation(2L);
    Mutation mutation2 =
        Mutation.newInsertOrUpdateBuilder("other-table").set("key").to("3L").build();

    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation, mutation2));

    mutations.apply(
        SpannerIO.write().withSpannerConfig(SPANNER_CONFIG).withServiceFactory(serviceFactory));
    pipeline.run();

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 1);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, "other-table", "ok", 1);
  }

  private void verifyBatches(Iterable<Mutation>... batches) {
    for (Iterable<Mutation> b : batches) {
      verify(serviceFactory.mockDatabaseClient(), times(1))
          .writeAtLeastOnceWithOptions(
              mutationsInNoOrder(b), any(ReadQueryUpdateTransactionOption.class));
    }
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", batches.length);
  }

  @Test
  public void noBatching() throws Exception {

    // This test uses a different mock/fake because it explicitly does not want to populate the
    // Spanner schema.
    FakeServiceFactory fakeServiceFactory = new FakeServiceFactory();
    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(fakeServiceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Capture batches sent to writeAtLeastOnceWithOptions.
    when(fakeServiceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(mutationBatchesCaptor.capture(), optionsCaptor.capture()))
        .thenReturn(null);

    PCollection<MutationGroup> mutations =
        pipeline.apply(
            Create.of(
                buildMutationGroup(buildUpsertMutation(1L)),
                buildMutationGroup(buildUpsertMutation(2L))));
    mutations.apply(
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(fakeServiceFactory)
            .withBatchSizeBytes(1)
            .grouped());
    pipeline.run();

    verify(fakeServiceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnceWithOptions(
            mutationsInNoOrder(buildMutationBatch(buildUpsertMutation(1L))),
            any(ReadQueryUpdateTransactionOption.class));
    verify(fakeServiceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnceWithOptions(
            mutationsInNoOrder(buildMutationBatch(buildUpsertMutation(2L))),
            any(ReadQueryUpdateTransactionOption.class));
    // If no batching then the DB schema is never read.
    verify(tx, never()).executeQuery(any());
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 2);
  }

  @Test
  public void streamingWrites() throws Exception {
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(buildUpsertMutation(1L), buildUpsertMutation(2L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(buildUpsertMutation(3L), buildUpsertMutation(4L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(buildUpsertMutation(5L), buildUpsertMutation(6L))
            .advanceWatermarkToInfinity();
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write().withSpannerConfig(SPANNER_CONFIG).withServiceFactory(serviceFactory));
    pipeline.run();

    verifyBatches(
        buildMutationBatch(buildUpsertMutation(1L), buildUpsertMutation(2L)),
        buildMutationBatch(buildUpsertMutation(3L), buildUpsertMutation(4L)),
        buildMutationBatch(buildUpsertMutation(5L), buildUpsertMutation(6L)));
  }

  @Test
  public void streamingWritesWithPriority() throws Exception {
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(buildUpsertMutation(1L), buildUpsertMutation(2L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(buildUpsertMutation(3L), buildUpsertMutation(4L))
            .advanceProcessingTime(Duration.standardMinutes(1))
            .addElements(buildUpsertMutation(5L), buildUpsertMutation(6L))
            .advanceWatermarkToInfinity();
    Write write =
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .withHighPriority();
    pipeline.apply(testStream).apply(write);
    pipeline.run();
    assertEquals(RpcPriority.HIGH, write.getSpannerConfig().getRpcPriority().get());
    verifyBatches(
        buildMutationBatch(buildUpsertMutation(1L), buildUpsertMutation(2L)),
        buildMutationBatch(buildUpsertMutation(3L), buildUpsertMutation(4L)),
        buildMutationBatch(buildUpsertMutation(5L), buildUpsertMutation(6L)));
  }

  @Test
  public void streamingWritesWithGrouping() throws Exception {

    // verify that grouping/sorting occurs when set.
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(
                buildUpsertMutation(1L),
                buildUpsertMutation(5L),
                buildUpsertMutation(2L),
                buildUpsertMutation(4L),
                buildUpsertMutation(3L),
                buildUpsertMutation(6L))
            .advanceWatermarkToInfinity();
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write()
                .withSpannerConfig(SPANNER_CONFIG)
                .withServiceFactory(serviceFactory)
                .withGroupingFactor(40)
                .withMaxNumRows(2));
    pipeline.run();

    // Output should be batches of sorted mutations.
    verifyBatches(
        buildMutationBatch(buildUpsertMutation(1L), buildUpsertMutation(2L)),
        buildMutationBatch(buildUpsertMutation(3L), buildUpsertMutation(4L)),
        buildMutationBatch(buildUpsertMutation(5L), buildUpsertMutation(6L)));
  }

  @Test
  public void streamingWritesWithGroupingWithPriority() throws Exception {

    // verify that grouping/sorting occurs when set.
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(
                buildUpsertMutation(1L),
                buildUpsertMutation(5L),
                buildUpsertMutation(2L),
                buildUpsertMutation(4L),
                buildUpsertMutation(3L),
                buildUpsertMutation(6L))
            .advanceWatermarkToInfinity();

    Write write =
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
            .withServiceFactory(serviceFactory)
            .withGroupingFactor(40)
            .withMaxNumRows(2)
            .withLowPriority();
    pipeline.apply(testStream).apply(write);
    pipeline.run();
    assertEquals(RpcPriority.LOW, write.getSpannerConfig().getRpcPriority().get());

    // Output should be batches of sorted mutations.
    verifyBatches(
        buildMutationBatch(buildUpsertMutation(1L), buildUpsertMutation(2L)),
        buildMutationBatch(buildUpsertMutation(3L), buildUpsertMutation(4L)),
        buildMutationBatch(buildUpsertMutation(5L), buildUpsertMutation(6L)));
  }

  @Test
  public void streamingWritesNoGrouping() throws Exception {

    // verify that grouping/sorting does not occur - batches should be created in received order.
    TestStream<Mutation> testStream =
        TestStream.create(SerializableCoder.of(Mutation.class))
            .addElements(
                buildUpsertMutation(1L),
                buildUpsertMutation(5L),
                buildUpsertMutation(2L),
                buildUpsertMutation(4L),
                buildUpsertMutation(3L),
                buildUpsertMutation(6L))
            .advanceWatermarkToInfinity();

    // verify that grouping/sorting does not occur when notset.
    pipeline
        .apply(testStream)
        .apply(
            SpannerIO.write()
                .withSpannerConfig(SPANNER_CONFIG)
                .withServiceFactory(serviceFactory)
                .withMaxNumRows(2));
    pipeline.run();

    verifyBatches(
        buildMutationBatch(buildUpsertMutation(1L), buildUpsertMutation(5L)),
        buildMutationBatch(buildUpsertMutation(2L), buildUpsertMutation(4L)),
        buildMutationBatch(buildUpsertMutation(3L), buildUpsertMutation(6L)));
  }

  @Test
  public void reportFailures() throws Exception {

    MutationGroup[] mutationGroups = new MutationGroup[10];
    for (int i = 0; i < mutationGroups.length; i++) {
      mutationGroups[i] = buildMutationGroup(buildUpsertMutation((long) i));
    }

    List<MutationGroup> mutationGroupList = Arrays.asList(mutationGroups);

    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
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
                    .withSpannerConfig(SPANNER_CONFIG)
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

    // writeAtLeastOnceWithOptions called once for the batch of mutations
    // (which as they are unbatched = each mutation group) then again for the individual retry.
    verify(serviceFactory.mockDatabaseClient(), times(20))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 0);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "already_exists", 20);
  }

  @Test
  public void deadlineExceededRetries() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(buildUpsertMutation((long) 1));

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with 2 timeouts and a success.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 1"))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout 2"))
        .thenReturn(new CommitResponse(Timestamp.now()));

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withSpannerConfig(SPANNER_CONFIG)
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
    verify(serviceFactory.mockDatabaseClient(), times(3))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 1);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "deadline_exceeded", 2);
  }

  @Test
  public void deadlineExceededFailsAfterRetries() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(buildUpsertMutation((long) 1));

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with all timeouts.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
        .thenThrow(
            SpannerExceptionFactory.newSpannerException(
                ErrorCode.DEADLINE_EXCEEDED, "simulated Timeout"));

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withSpannerConfig(SPANNER_CONFIG)
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

    long totalSleep =
        Mockito.mockingDetails(WriteToSpannerFn.sleeper).getInvocations().stream()
            .mapToLong(i -> i.getArgument(0))
            .reduce(0L, Long::sum);

    // Total sleep should be greater then 2x maxCumulativeBackoff: 120m,
    // because the batch is repeated inidividually due REPORT_FAILURES.
    assertTrue(
        String.format("Should be least 7200s of sleep, got %d", totalSleep),
        totalSleep >= Duration.standardHours(2).getMillis());

    int numSleeps = Mockito.mockingDetails(WriteToSpannerFn.sleeper).getInvocations().size();
    // Number of write attempts should be numSleeps + 2 write attempts:
    //      1 batch attempt, numSleeps/2 batch retries,
    // then 1 individual attempt + numSleeps/2 individual retries
    verify(serviceFactory.mockDatabaseClient(), times(numSleeps + 2))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 0);
    verifyTableWriteRequestMetricWasSet(
        SPANNER_CONFIG, TABLE_NAME, "deadline_exceeded", numSleeps + 2);
  }

  @Test
  public void retryOnSchemaChangeException() throws InterruptedException {
    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";
    retryOnAbortedExceptionWithMessage(errString);
  }

  @Test
  public void retryOnEmulatorRejectedConcurrentTransaction() throws InterruptedException {
    String errString =
        "Transaction 199 aborted due to active transaction 167. "
            + "The emulator only supports one transaction at a time.";
    retryOnAbortedExceptionWithMessage(errString);
  }

  public void retryOnAbortedExceptionWithMessage(String errString) throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(buildUpsertMutation((long) 1));
    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // respond with 2 timeouts and a success.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenThrow(SpannerExceptionFactory.newSpannerException(ErrorCode.ABORTED, errString))
        .thenReturn(new CommitResponse(Timestamp.now()));

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withSpannerConfig(SPANNER_CONFIG)
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
    verify(serviceFactory.mockDatabaseClient(), times(3))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 1);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "aborted", 2);
  }

  @Test
  public void retryMaxOnSchemaChangeException() throws InterruptedException {
    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";
    retryMaxOnAbortedExceptionWithMessage(errString);
  }

  @Test
  public void retryMaxOnEmulatorRejectedConcurrentTransaction() throws InterruptedException {
    String errString =
        "Transaction 199 aborted due to active transaction 167. "
            + "The emulator only supports one transaction at a time.";
    retryOnAbortedExceptionWithMessage(errString);
  }

  public void retryMaxOnAbortedExceptionWithMessage(String errString) throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(buildUpsertMutation((long) 1));

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // Respond with Aborted transaction
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
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
                    .withSpannerConfig(SPANNER_CONFIG)
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
    verify(serviceFactory.mockDatabaseClient(), times(5))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 0);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "aborted", 5);
  }

  @Test
  public void retryOnAbortedAndDeadlineExceeded() throws InterruptedException {
    List<Mutation> mutationList = Arrays.asList(buildUpsertMutation((long) 1));

    String errString =
        "Transaction aborted. "
            + "Database schema probably changed during transaction, retry may succeed.";

    // mock sleeper so that it does not actually sleep.
    WriteToSpannerFn.sleeper = Mockito.mock(Sleeper.class);

    // Respond with (1) Aborted transaction a couple of times (2) deadline exceeded
    // (3) Aborted transaction 3 times (4)  deadline exceeded and finally return success.
    when(serviceFactory
            .mockDatabaseClient()
            .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class)))
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
        .thenReturn(new CommitResponse(Timestamp.now()));

    SpannerWriteResult result =
        pipeline
            .apply(Create.of(mutationList))
            .apply(
                SpannerIO.write()
                    .withSpannerConfig(SPANNER_CONFIG)
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
    verify(serviceFactory.mockDatabaseClient(), times(8))
        .writeAtLeastOnceWithOptions(any(), any(ReadQueryUpdateTransactionOption.class));

    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "ok", 1);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "aborted", 5);
    verifyTableWriteRequestMetricWasSet(SPANNER_CONFIG, TABLE_NAME, "deadline_exceeded", 2);
  }

  @Test
  public void displayDataWrite() throws Exception {
    SpannerIO.Write write =
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
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
    write = SpannerIO.write().withSpannerConfig(SPANNER_CONFIG);

    data = DisplayData.from(write);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("groupingFactor", "DEFAULT"));
  }

  @Test
  public void displayDataWriteGrouped() throws Exception {
    SpannerIO.WriteGrouped writeGrouped =
        SpannerIO.write()
            .withSpannerConfig(SPANNER_CONFIG)
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
    writeGrouped = SpannerIO.write().withSpannerConfig(SPANNER_CONFIG).grouped();

    data = DisplayData.from(writeGrouped);
    assertThat(data.items(), hasSize(7));
    assertThat(data, hasDisplayItem("groupingFactor", "DEFAULT"));
  }

  @Test
  public void testBatchableMutationFilterFn_cells() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(2L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too big.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, 10000000, 3 * CELLS_PER_KEY, 1000);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
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
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(2L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too big.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_size() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(1L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too big.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    long mutationSize = MutationSizeEstimator.sizeOf(buildUpsertMutation(1L));
    BatchableMutationFilterFn testFn =
        new BatchableMutationFilterFn(null, null, mutationSize * 3, 1000, 1000);

    DoFn<MutationGroup, MutationGroup>.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
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
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(1L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too big.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_rows() {
    Mutation all = Mutation.delete(TABLE_NAME, KeySet.all());
    Mutation prefix = Mutation.delete(TABLE_NAME, KeySet.prefixRange(Key.of(1L)));
    Mutation range =
        Mutation.delete(
            TABLE_NAME, KeySet.range(KeyRange.openOpen(Key.of(1L), Key.newBuilder().build())));
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
          buildMutationGroup(
              buildUpsertMutation(1L),
              buildUpsertMutation(3L),
              buildUpsertMutation(4L),
              buildUpsertMutation(5L)), // not batchable - too many rows.
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
          buildMutationGroup(all),
          buildMutationGroup(prefix),
          buildMutationGroup(range)
        };

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 1000, 1000, 3);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
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
        containsInAnyOrder(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L), buildUpsertMutation(3L)),
            buildMutationGroup(buildDeleteMutation(1L))));

    // Verify captured unbatchable mutations
    Iterable<MutationGroup> unbatchableMutations =
        Iterables.concat(mutationGroupListCaptor.getAllValues());
    assertThat(
        unbatchableMutations,
        containsInAnyOrder(
            buildMutationGroup(
                buildUpsertMutation(1L),
                buildUpsertMutation(3L),
                buildUpsertMutation(4L),
                buildUpsertMutation(5L)), // not batchable - too many rows.
            buildMutationGroup(buildDeleteMutation(5L, 6L)), // not point delete.
            buildMutationGroup(all),
            buildMutationGroup(prefix),
            buildMutationGroup(range)));
  }

  @Test
  public void testBatchableMutationFilterFn_batchingDisabled() {
    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(1L)),
          buildMutationGroup(buildDeleteMutation(5L, 6L))
        };

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 0, 0, 0);

    BatchableMutationFilterFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
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

    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
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
          buildMutationGroup(buildUpsertMutation(4L)),
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(7L)),
          buildMutationGroup(buildUpsertMutation(12L)),
          buildMutationGroup(buildUpsertMutation(10L)),
          buildMutationGroup(buildUpsertMutation(11L)),
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(8L)),
          buildMutationGroup(buildUpsertMutation(3L)),
          buildMutationGroup(buildUpsertMutation(6L)),
          buildMutationGroup(buildUpsertMutation(9L)),
          buildMutationGroup(buildUpsertMutation(5L))
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
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(1L)),
                buildMutationGroup(buildUpsertMutation(2L)),
                buildMutationGroup(buildUpsertMutation(3L)),
                buildMutationGroup(buildUpsertMutation(4L)),
                buildMutationGroup(buildUpsertMutation(5L))),
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(6L)),
                buildMutationGroup(buildUpsertMutation(7L)),
                buildMutationGroup(buildDeleteMutation(8L)),
                buildMutationGroup(buildUpsertMutation(9L)),
                buildMutationGroup(buildUpsertMutation(10L))),
            Arrays.asList(
                buildMutationGroup(buildUpsertMutation(11L)),
                buildMutationGroup(buildUpsertMutation(12L)))));
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

    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
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
          buildMutationGroup(buildUpsertMutation(4L)),
          buildMutationGroup(buildUpsertMutation(1L)),
          buildMutationGroup(buildUpsertMutation(7L)),
          buildMutationGroup(buildUpsertMutation(9L)),
          buildMutationGroup(buildUpsertMutation(10L)),
          buildMutationGroup(buildUpsertMutation(11L)),
          // end group
          buildMutationGroup(buildUpsertMutation(2L)),
          buildMutationGroup(buildDeleteMutation(8L)), // end batch
          buildMutationGroup(buildUpsertMutation(3L)),
          buildMutationGroup(buildUpsertMutation(6L)), // end batch
          buildMutationGroup(buildUpsertMutation(5L))
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
    assertThat(
        mgListGroups.get(0),
        contains(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(4L))));
    assertThat(
        mgListGroups.get(1),
        contains(
            buildMutationGroup(buildUpsertMutation(7L)),
            buildMutationGroup(buildUpsertMutation(9L))));
    assertThat(
        mgListGroups.get(2),
        contains(
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L))));

    // second group at finishBundle should be 2,3,5,6,8
    assertThat(
        mgListGroups.get(3),
        contains(
            buildMutationGroup(buildUpsertMutation(2L)),
            buildMutationGroup(buildUpsertMutation(3L))));
    assertThat(
        mgListGroups.get(4),
        contains(
            buildMutationGroup(buildUpsertMutation(5L)),
            buildMutationGroup(buildUpsertMutation(6L))));
    assertThat(mgListGroups.get(5), contains(buildMutationGroup(buildDeleteMutation(8L))));
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

    long mutationSize = MutationSizeEstimator.sizeOf(buildUpsertMutation(1L));

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
    GatherSortCreateBatchesFn.ProcessContext mockProcessContext =
        Mockito.mock(ProcessContext.class);
    GatherSortCreateBatchesFn.FinishBundleContext mockFinishBundleContext =
        Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the output at finish bundle..
    doNothing()
        .when(mockFinishBundleContext)
        .output(mutationGroupListCaptor.capture(), any(), any());

    List<MutationGroup> mutationGroups =
        Arrays.asList(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(4L)),
            buildMutationGroup(
                buildUpsertMutation(5L),
                buildUpsertMutation(6L),
                buildUpsertMutation(7L),
                buildUpsertMutation(8L),
                buildUpsertMutation(9L)),
            buildMutationGroup(buildUpsertMutation(3L)),
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L)),
            buildMutationGroup(buildUpsertMutation(2L)));

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
    assertThat(
        batches.get(0),
        contains(
            buildMutationGroup(buildUpsertMutation(1L)),
            buildMutationGroup(buildUpsertMutation(2L)),
            buildMutationGroup(buildUpsertMutation(3L))));
    assertThat(
        batches.get(1),
        contains(
            buildMutationGroup(
                buildUpsertMutation(4L)))); // small batch : next mutation group is too big.
    assertThat(
        batches.get(2),
        contains(
            buildMutationGroup(
                buildUpsertMutation(5L),
                buildUpsertMutation(6L),
                buildUpsertMutation(7L),
                buildUpsertMutation(8L),
                buildUpsertMutation(9L))));
    assertThat(
        batches.get(3),
        contains(
            buildMutationGroup(buildUpsertMutation(10L)),
            buildMutationGroup(buildUpsertMutation(11L))));
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

  static MutationGroup buildMutationGroup(Mutation m, Mutation... other) {
    return MutationGroup.create(m, other);
  }

  static Mutation buildUpsertMutation(Long key) {
    return Mutation.newInsertOrUpdateBuilder(TABLE_NAME).set("key").to(key).build();
  }

  private static Iterable<Mutation> buildMutationBatch(Mutation... m) {
    return Arrays.asList(m);
  }

  private static Mutation buildDeleteMutation(Long... keys) {

    KeySet.Builder builder = KeySet.newBuilder();
    for (Long key : keys) {
      builder.addKey(Key.of(key));
    }
    return Mutation.delete(TABLE_NAME, builder.build());
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

  private void verifyTableWriteRequestMetricWasSet(
      SpannerConfig config, String table, String status, long count) {

    HashMap<String, String> baseLabels = getBaseMetricsLabels(config);
    baseLabels.put(MonitoringInfoConstants.Labels.METHOD, "Write");
    baseLabels.put(MonitoringInfoConstants.Labels.TABLE_ID, table);
    baseLabels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        GcpResourceIdentifiers.spannerTable(
            baseLabels.get(MonitoringInfoConstants.Labels.SPANNER_PROJECT_ID),
            config.getInstanceId().get(),
            config.getDatabaseId().get(),
            table));
    baseLabels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, baseLabels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getCurrentContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
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

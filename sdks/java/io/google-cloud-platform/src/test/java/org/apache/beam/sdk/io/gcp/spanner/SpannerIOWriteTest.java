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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.BatchFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.BatchableMutationFilterFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.GatherBundleAndSortFn;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn.FinishBundleContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
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
  @Captor public transient ArgumentCaptor<List<KV<byte[], byte[]>>> byteArrayKvListCaptor;

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
    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(g(m(1L)), g(m(2L))));
    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory)
            .withBatchSizeBytes(1)
            .grouped());
    pipeline.run();

    verifyBatches(batch(m(1L)), batch(m(2L)));
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
  public void displayData() throws Exception {
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(123);

    DisplayData data = DisplayData.from(write);
    assertThat(data.items(), hasSize(4));
    assertThat(data, hasDisplayItem("projectId", "test-project"));
    assertThat(data, hasDisplayItem("instanceId", "test-instance"));
    assertThat(data, hasDisplayItem("databaseId", "test-database"));
    assertThat(data, hasDisplayItem("batchSizeBytes", 123));
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
        new BatchableMutationFilterFn(null, null, 10000000, 3 * CELLS_PER_KEY);

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
        new BatchableMutationFilterFn(null, null, mutationSize * 3, 1000);

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
  public void testBatchableMutationFilterFn_batchingDisabled() {
    MutationGroup[] mutationGroups =
        new MutationGroup[] {g(m(1L)), g(m(2L)), g(del(1L)), g(del(5L, 6L))};

    BatchableMutationFilterFn testFn = new BatchableMutationFilterFn(null, null, 0, 0);

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
  public void testGatherBundleAndSortFn() throws Exception {
    GatherBundleAndSortFn testFn = new GatherBundleAndSortFn(10000000, 10, 100, null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    FinishBundleContext mockFinishBundleContext = Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(byteArrayKvListCaptor.capture());
    // Capture the outputs.
    doNothing().when(mockFinishBundleContext).output(byteArrayKvListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          g(m(4L)), g(m(1L)), g(m(5L), m(6L), m(7L), m(8L), m(9L)), g(del(2L)), g(m(3L))
        };

    // Process all elements as one bundle.
    testFn.startBundle();
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockProcessContext, never()).output(any());
    verify(mockFinishBundleContext, times(1)).output(any(), any(), any());

    // Verify sorted output... first decode it...
    List<MutationGroup> sorted =
        byteArrayKvListCaptor.getValue().stream()
            .map(kv -> WriteGrouped.decode(kv.getValue()))
            .collect(Collectors.toList());
    assertThat(
        sorted,
        contains(g(m(1L)), g(del(2L)), g(m(3L)), g(m(4L)), g(m(5L), m(6L), m(7L), m(8L), m(9L))));
  }

  @Test
  public void testGatherBundleAndSortFn_flushOversizedBundle() throws Exception {

    // Setup class to bundle every 3 mutations
    GatherBundleAndSortFn testFn = new GatherBundleAndSortFn(10000000, CELLS_PER_KEY, 3, null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    FinishBundleContext mockFinishBundleContext = Mockito.mock(FinishBundleContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(byteArrayKvListCaptor.capture());
    // Capture the outputs.
    doNothing().when(mockFinishBundleContext).output(byteArrayKvListCaptor.capture(), any(), any());

    MutationGroup[] mutationGroups =
        new MutationGroup[] {
          g(m(4L)),
          g(m(1L)),
          // end group
          g(m(5L), m(6L), m(7L), m(8L), m(9L)),
          // end group
          g(m(10L)),
          g(m(3L)),
          g(m(11L)),
          // end group.
          g(m(2L))
        };

    // Process all elements as one bundle.
    testFn.startBundle();
    for (MutationGroup m : mutationGroups) {
      when(mockProcessContext.element()).thenReturn(m);
      testFn.processElement(mockProcessContext);
    }
    testFn.finishBundle(mockFinishBundleContext);

    verify(mockProcessContext, times(3)).output(any());
    verify(mockFinishBundleContext, times(1)).output(any(), any(), any());

    // verify sorted output... needs decoding...
    List<List<KV<byte[], byte[]>>> kvGroups = byteArrayKvListCaptor.getAllValues();
    assertEquals(4, kvGroups.size());

    // decode list of lists of KV to a list of lists of MutationGroup.
    List<List<MutationGroup>> mgListGroups =
        kvGroups.stream()
            .map(
                l ->
                    l.stream()
                        .map(kv -> WriteGrouped.decode(kv.getValue()))
                        .collect(Collectors.toList()))
            .collect(Collectors.toList());

    // verify contents of 4 sorted groups.
    assertThat(mgListGroups.get(0), contains(g(m(1L)), g(m(4L))));
    assertThat(mgListGroups.get(1), contains(g(m(5L), m(6L), m(7L), m(8L), m(9L))));
    assertThat(mgListGroups.get(2), contains(g(m(3L)), g(m(10L)), g(m(11L))));
    assertThat(mgListGroups.get(3), contains(g(m(2L))));
  }

  @Test
  public void testBatchFn_cells() throws Exception {

    // Setup class to bundle every 3 mutations (3xCELLS_PER_KEY cell mutations)
    BatchFn testFn = new BatchFn(10000000, 3 * CELLS_PER_KEY, null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupListCaptor.capture());

    List<MutationGroup> mutationGroups =
        Arrays.asList(
            g(m(1L)),
            g(m(4L)),
            g(m(5L), m(6L), m(7L), m(8L), m(9L)),
            g(m(3L)),
            g(m(10L)),
            g(m(11L)),
            g(m(2L)));

    List<KV<byte[], byte[]>> encodedInput =
        mutationGroups.stream()
            .map(mg -> KV.of((byte[]) null, WriteGrouped.encode(mg)))
            .collect(Collectors.toList());

    // Process elements.
    when(mockProcessContext.element()).thenReturn(encodedInput);
    testFn.processElement(mockProcessContext);

    verify(mockProcessContext, times(4)).output(any());

    List<Iterable<MutationGroup>> batches = mutationGroupListCaptor.getAllValues();
    assertEquals(4, batches.size());

    // verify contents of 4 batches.
    assertThat(batches.get(0), contains(g(m(1L)), g(m(4L))));
    assertThat(batches.get(1), contains(g(m(5L), m(6L), m(7L), m(8L), m(9L))));
    assertThat(batches.get(2), contains(g(m(3L)), g(m(10L)), g(m(11L))));
    assertThat(batches.get(3), contains(g(m(2L))));
  }

  @Test
  public void testBatchFn_size() throws Exception {

    long mutationSize = MutationSizeEstimator.sizeOf(m(1L));

    // Setup class to bundle every 3 mutations by size)
    BatchFn testFn = new BatchFn(mutationSize * 3, 1000, null);

    ProcessContext mockProcessContext = Mockito.mock(ProcessContext.class);
    when(mockProcessContext.sideInput(any())).thenReturn(getSchema());

    // Capture the outputs.
    doNothing().when(mockProcessContext).output(mutationGroupListCaptor.capture());

    List<MutationGroup> mutationGroups =
        Arrays.asList(
            g(m(1L)),
            g(m(4L)),
            g(m(5L), m(6L), m(7L), m(8L), m(9L)),
            g(m(3L)),
            g(m(10L)),
            g(m(11L)),
            g(m(2L)));

    List<KV<byte[], byte[]>> encodedInput =
        mutationGroups.stream()
            .map(mg -> KV.of((byte[]) null, WriteGrouped.encode(mg)))
            .collect(Collectors.toList());

    // Process elements.
    when(mockProcessContext.element()).thenReturn(encodedInput);
    testFn.processElement(mockProcessContext);

    verify(mockProcessContext, times(4)).output(any());

    List<Iterable<MutationGroup>> batches = mutationGroupListCaptor.getAllValues();
    assertEquals(4, batches.size());

    // verify contents of 4 batches.
    assertThat(batches.get(0), contains(g(m(1L)), g(m(4L))));
    assertThat(batches.get(1), contains(g(m(5L), m(6L), m(7L), m(8L), m(9L))));
    assertThat(batches.get(2), contains(g(m(3L)), g(m(10L)), g(m(11L))));
    assertThat(batches.get(3), contains(g(m(2L))));
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

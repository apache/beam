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
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Description;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;

/**
 * Unit tests for {@link SpannerIO}.
 */
@RunWith(JUnit4.class)
public class SpannerIOWriteTest implements Serializable {
  private static final long CELLS_PER_KEY = 7;

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;

  @Before @SuppressWarnings("unchecked") public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();

    ReadOnlyTransaction tx = mock(ReadOnlyTransaction.class);
    when(serviceFactory.mockDatabaseClient().readOnlyTransaction()).thenReturn(tx);

    // Simplest schema: a table with int64 key
    preparePkMetadata(tx, Arrays.asList(pkMetadata("test", "key", "ASC")));
    prepareColumnMetadata(tx, Arrays.asList(columnMetadata("test", "key", "INT64", CELLS_PER_KEY)));
  }

  private static Struct columnMetadata(
      String tableName, String columnName, String type, long cellsMutated) {
    return Struct.newBuilder().add("table_name", Value.string(tableName))
        .add("column_name", Value.string(columnName))
        .add("spanner_type", Value.string(type))
        .add("cells_mutated", Value.int64(cellsMutated))
        .build();
  }

  private static Struct pkMetadata(String tableName, String columnName, String ordering) {
    return Struct.newBuilder().add("table_name", Value.string(tableName))
        .add("column_name", Value.string(columnName)).add("column_ordering", Value.string(ordering))
        .build();
  }

  private void prepareColumnMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type = Type.struct(Type.StructField.of("table_name", Type.string()),
        Type.StructField.of("column_name", Type.string()),
        Type.StructField.of("spanner_type", Type.string()),
        Type.StructField.of("cells_mutated", Type.int64()));
    when(tx.executeQuery(argThat(new ArgumentMatcher<Statement>() {

      @Override public boolean matches(Object argument) {
        if (!(argument instanceof Statement)) {
          return false;
        }
        Statement st = (Statement) argument;
        return st.getSql().contains("information_schema.columns");
      }
    }))).thenReturn(ResultSets.forRows(type, rows));
  }

  private void preparePkMetadata(ReadOnlyTransaction tx, List<Struct> rows) {
    Type type = Type.struct(Type.StructField.of("table_name", Type.string()),
        Type.StructField.of("column_name", Type.string()),
        Type.StructField.of("column_ordering", Type.string()));
    when(tx.executeQuery(argThat(new ArgumentMatcher<Statement>() {

      @Override public boolean matches(Object argument) {
        if (!(argument instanceof Statement)) {
          return false;
        }
        Statement st = (Statement) argument;
        return st.getSql().contains("information_schema.index_columns");
      }
    }))).thenReturn(ResultSets.forRows(type, rows));
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
  @Category(NeedsRunner.class)
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

    verifyBatches(
        batch(m(2L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void singleMutationGroupPipeline() throws Exception {
    PCollection<MutationGroup> mutations = pipeline
        .apply(Create.<MutationGroup>of(g(m(1L), m(2L), m(3L))));
    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory)
            .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L), m(2L), m(3L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void batching() throws Exception {
    MutationGroup one = g(m(1L));
    MutationGroup two = g(m(2L));
    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(one, two));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1000000000)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L), m(2L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void batchingWithDeletes() throws Exception {
    PCollection<MutationGroup> mutations = pipeline
        .apply(Create.of(g(m(1L)), g(m(2L)), g(del(3L)), g(del(4L))));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1000000000)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L), m(2L), del(3L), del(4L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void noBatchingRangeDelete() throws Exception {
    Mutation all = Mutation.delete("test", KeySet.all());
    Mutation prefix = Mutation.delete("test", KeySet.prefixRange(Key.of(1L)));
    Mutation range = Mutation.delete("test", KeySet.range(KeyRange.openOpen(Key.of(1L), Key
        .newBuilder().build())));

    PCollection<MutationGroup> mutations = pipeline.apply(Create
        .of(
            g(m(1L)),
            g(m(2L)),
            g(del(5L, 6L)),
            g(delRange(50L, 55L)),
            g(delRange(11L, 20L)),
            g(all),
            g(prefix), g(range)
        )
    );
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1000000000)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L), m(2L)),
        batch(del(5L, 6L)),
        batch(delRange(11L, 20L)),
        batch(delRange(50L, 55L)),
        batch(all),
        batch(prefix),
        batch(range)
    );
  }

  private void verifyBatches(Iterable<Mutation>... batches) {
    for (Iterable<Mutation> b : batches) {
      verify(serviceFactory.mockDatabaseClient(), times(1)).writeAtLeastOnce(mutationsInNoOrder(b));
    }

  }

  @Test
  @Category(NeedsRunner.class)
  public void sizeBatchingGroups() throws Exception {
    // Accumulate two items per batch.
    long batchSize = MutationSizeEstimator.sizeOf(g(m(1L))) * 2;

    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(g(m(1L)), g(m(2L)), g(m(3L))));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(batchSize)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());

    pipeline.run();

    // The content of batches is not deterministic. Just verify that the size is correct.
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(iterableOfSize(2));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(iterableOfSize(1));
  }

  @Test
  @Category(NeedsRunner.class)
  public void cellBatchingGroups() throws Exception {
    // Accumulate two items per batch.
    long maxNumMutations = CELLS_PER_KEY * 2;

    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(g(m(1L)), g(m(2L)), g(m(3L))));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withMaxNumMutations(maxNumMutations)
        .withBatchSizeBytes(Integer.MAX_VALUE)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());

    pipeline.run();

    // The content of batches is not deterministic. Just verify that the size is correct.
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(iterableOfSize(2));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(iterableOfSize(1));
  }

  @Test
  @Category(NeedsRunner.class)
  public void noBatching() throws Exception {
    PCollection<MutationGroup> mutations = pipeline.apply(Create.of(g(m(1L)), g(m(2L))));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1)
        .withSampler(fakeSampler(m(1000L)))
        .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L)),
        batch(m(2L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void batchingPlusSampling() throws Exception {
    PCollection<MutationGroup> mutations = pipeline
        .apply(Create.of(
            g(m(1L)), g(m(2L)), g(m(3L)), g(m(4L)),  g(m(5L)),
            g(m(6L)), g(m(7L)), g(m(8L)), g(m(9L)),  g(m(10L)))
        );

    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1000000000)
        .withSampler(fakeSampler(m(2L), m(5L), m(10L)))
        .grouped());
    pipeline.run();

    verifyBatches(
        batch(m(1L), m(2L)),
        batch(m(3L), m(4L), m(5L)),
        batch(m(6L), m(7L), m(8L), m(9L), m(10L))
    );
  }

  @Test
  @Category(NeedsRunner.class)
  public void noBatchingPlusSampling() throws Exception {
    PCollection<MutationGroup> mutations = pipeline
        .apply(Create.of(g(m(1L)), g(m(2L)), g(m(3L)), g(m(4L)), g(m(5L))));
    mutations.apply(SpannerIO.write()
        .withProjectId("test-project")
        .withInstanceId("test-instance")
        .withDatabaseId("test-database")
        .withServiceFactory(serviceFactory)
        .withBatchSizeBytes(1)
        .withSampler(fakeSampler(m(2L)))
        .grouped());

    pipeline.run();

    verifyBatches(
        batch(m(1L)),
        batch(m(2L)),
        batch(m(3L)),
        batch(m(4L)),
        batch(m(5L))
    );
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
    return argThat(new ArgumentMatcher<Iterable<Mutation>>() {

      @Override
      public boolean matches(Object argument) {
        if (!(argument instanceof Iterable)) {
          return false;
        }
        ImmutableSet<Mutation> actual = ImmutableSet.copyOf((Iterable) argument);
        return actual.equals(mutations);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Iterable must match ").appendValue(mutations);
      }

    });
  }

  private Iterable<Mutation> iterableOfSize(final int size) {
    return argThat(new ArgumentMatcher<Iterable<Mutation>>() {

      @Override
      public boolean matches(Object argument) {
        return argument instanceof Iterable && Iterables.size((Iterable<?>) argument) == size;
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("The size of the iterable must equal ").appendValue(size);
      }
    });
  }

  private static FakeSampler fakeSampler(Mutation... mutations) {
    SpannerSchema.Builder schema = SpannerSchema.builder();
    schema.addColumn("test", "key", "INT64");
    schema.addKeyPart("test", "key", false);
    return new FakeSampler(schema.build(), Arrays.asList(mutations));
  }

  private static class FakeSampler
      extends PTransform<PCollection<KV<String, byte[]>>, PCollection<KV<String, List<byte[]>>>> {

    private final SpannerSchema schema;
    private final List<Mutation> mutations;

    private FakeSampler(SpannerSchema schema, List<Mutation> mutations) {
      this.schema = schema;
      this.mutations = mutations;
    }

    @Override
    public PCollection<KV<String, List<byte[]>>> expand(
        PCollection<KV<String, byte[]>> input) {
      MutationGroupEncoder coder = new MutationGroupEncoder(schema);
      Map<String, List<byte[]>> map = new HashMap<>();
      for (Mutation m : mutations) {
        String table = m.getTable();
        List<byte[]> list = map.computeIfAbsent(table, k -> new ArrayList<>());
        list.add(coder.encodeKey(m));
      }
      List<KV<String, List<byte[]>>> result = new ArrayList<>();
      for (Map.Entry<String, List<byte[]>> entry : map.entrySet()) {
        entry.getValue().sort(SpannerIO.SerializableBytesComparator.INSTANCE);
        result.add(KV.of(entry.getKey(), entry.getValue()));
      }
      return input.getPipeline().apply(Create.of(result));
    }
  }
}

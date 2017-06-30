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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
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
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private FakeServiceFactory serviceFactory;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    serviceFactory = new FakeServiceFactory();
  }

  @Test
  public void emptyTransform() throws Exception {
    SpannerIO.Write write = SpannerIO.write();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.validate(null);
  }

  @Test
  public void emptyInstanceId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withDatabaseId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires instance id to be set with");
    write.validate(null);
  }

  @Test
  public void emptyDatabaseId() throws Exception {
    SpannerIO.Write write = SpannerIO.write().withInstanceId("123");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("requires database id to be set with");
    write.validate(null);
  }

  @Test
  @Category(NeedsRunner.class)
  public void singleMutationPipeline() throws Exception {
    Mutation mutation = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2).build();
    PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory));
    pipeline.run();
    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(1)));
  }

  @Test
  @Category(NeedsRunner.class)
  public void singleMutationGroupPipeline() throws Exception {
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    Mutation three = Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build();
    PCollection<MutationGroup> mutations = pipeline
        .apply(Create.<MutationGroup>of(g(one, two, three)));
    mutations.apply(
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withServiceFactory(serviceFactory)
            .grouped());
    pipeline.run();
    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(3)));
  }

  @Test
  public void batching() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(1000000000)
            .withServiceFactory(serviceFactory);
    SpannerWriteGroupFn writerFn = new SpannerWriteGroupFn(write);
    DoFnTester<MutationGroup, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(one, two));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(2)));
  }

  @Test
  public void batchingGroups() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());
    MutationGroup three = g(Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build());

    // Have a room to accumulate one more item.
    long batchSize = MutationSizeEstimator.sizeOf(one) + 1;

    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(batchSize)
            .withServiceFactory(serviceFactory);
    SpannerWriteGroupFn writerFn = new SpannerWriteGroupFn(write);
    DoFnTester<MutationGroup, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(one, two, three));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(2)));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(1)));
  }

  @Test
  public void noBatching() throws Exception {
    MutationGroup one = g(Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build());
    MutationGroup two = g(Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build());
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(0) // turn off batching.
            .withServiceFactory(serviceFactory);
    SpannerWriteGroupFn writerFn = new SpannerWriteGroupFn(write);
    DoFnTester<MutationGroup, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(one, two));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(2))
        .writeAtLeastOnce(argThat(new IterableOfSize(1)));
  }

  @Test
  public void groups() throws Exception {
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    Mutation three = Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build();

    // Smallest batch size
    long batchSize = 1;

    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSizeBytes(batchSize)
            .withServiceFactory(serviceFactory);
    SpannerWriteGroupFn writerFn = new SpannerWriteGroupFn(write);
    DoFnTester<MutationGroup, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(g(one, two, three)));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(3)));
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

  private static class IterableOfSize extends ArgumentMatcher<Iterable<Mutation>> {
    private final int size;

    private IterableOfSize(int size) {
      this.size = size;
    }

    @Override
    public boolean matches(Object argument) {
      return argument instanceof Iterable && Iterables.size((Iterable<?>) argument) == size;
    }
  }

  private static MutationGroup g(Mutation m, Mutation... other) {
    return MutationGroup.create(m, other);
  }
}

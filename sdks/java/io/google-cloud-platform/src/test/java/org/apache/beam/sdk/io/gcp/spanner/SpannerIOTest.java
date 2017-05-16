package org.apache.beam.sdk.io.gcp.spanner;

import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.annotation.concurrent.GuardedBy;

import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;


/**
 * Unit tests for {@link SpannerIO}.
 */
@RunWith(JUnit4.class)
public class SpannerIOTest implements Serializable {
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
  public void batching() throws Exception {
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSize(1000000000)
            .withServiceFactory(serviceFactory);
    SpannerIO.SpannerWriteFn writerFn = new SpannerIO.SpannerWriteFn(write);
    DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(one, two));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(1))
        .writeAtLeastOnce(argThat(new IterableOfSize(2)));
  }

  @Test
  public void batchingGroups() throws Exception {
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    Mutation three = Mutation.newInsertOrUpdateBuilder("test").set("three").to(3).build();

    // Have a room to accumulate one more item.
    long batchSize = MutationSizeEstimator.sizeOf(one) + 1;

    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSize(batchSize)
            .withServiceFactory(serviceFactory);
    SpannerIO.SpannerWriteFn writerFn = new SpannerIO.SpannerWriteFn(write);
    DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writerFn);
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
    Mutation one = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation two = Mutation.newInsertOrUpdateBuilder("test").set("two").to(2).build();
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId("test-project")
            .withInstanceId("test-instance")
            .withDatabaseId("test-database")
            .withBatchSize(0) // turn off batching.
            .withServiceFactory(serviceFactory);
    SpannerIO.SpannerWriteFn writerFn = new SpannerIO.SpannerWriteFn(write);
    DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writerFn);
    fnTester.processBundle(Arrays.asList(one, two));

    verify(serviceFactory.mockSpanner())
        .getDatabaseClient(DatabaseId.of("test-project", "test-instance", "test-database"));
    verify(serviceFactory.mockDatabaseClient(), times(2))
        .writeAtLeastOnce(argThat(new IterableOfSize(1)));
  }

  private static class FakeServiceFactory
      implements ServiceFactory<Spanner, SpannerOptions>, Serializable {
    // Marked as static so they could be returned by serviceFactory, which is serializable.
    private static final Object lock = new Object();

    @GuardedBy("lock")
    private static final List<Spanner> mockSpanners = new ArrayList<>();

    @GuardedBy("lock")
    private static final List<DatabaseClient> mockDatabaseClients = new ArrayList<>();

    @GuardedBy("lock")
    private static int count = 0;

    private final int index;

    public FakeServiceFactory() {
      synchronized (lock) {
        index = count++;
        mockSpanners.add(mock(Spanner.class, withSettings().serializable()));
        mockDatabaseClients.add(mock(DatabaseClient.class, withSettings().serializable()));
      }
      ApiFuture voidFuture = mock(ApiFuture.class, withSettings().serializable());
      when(mockSpanner().getDatabaseClient(Matchers.any(DatabaseId.class)))
          .thenReturn(mockDatabaseClient());
      when(mockSpanner().closeAsync()).thenReturn(voidFuture);
    }

    DatabaseClient mockDatabaseClient() {
      synchronized (lock) {
        return mockDatabaseClients.get(index);
      }
    }

    Spanner mockSpanner() {
      synchronized (lock) {
        return mockSpanners.get(index);
      }
    }

    @Override
    public Spanner create(SpannerOptions serviceOptions) {
      return mockSpanner();
    }
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
}

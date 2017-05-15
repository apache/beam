package org.apache.beam.sdk.io.gcp.spanner;

import avro.shaded.com.google.common.collect.Iterables;
import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.*;
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

import java.io.Serializable;
import java.util.Arrays;

import static org.mockito.Mockito.*;

@RunWith(JUnit4.class)
public class SpannerIOTest implements Serializable {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();
    @Rule
    public transient ExpectedException thrown = ExpectedException.none();
    // Tests that use serviceFactory must be serializable, to avoid parallel execution.
    private ServiceFactory<Spanner, SpannerOptions> serviceFactory;
    // Marked as static so they could be returned by serviceFactory, which is serializable.
    private static Spanner mockSpanner;
    private static DatabaseClient mockDatabaseClient;

    @Before
    public void setUp() throws Exception {
        ApiFuture voidFuture = mock(ApiFuture.class, withSettings().serializable());
        mockSpanner = mock(Spanner.class, withSettings().serializable());
        mockDatabaseClient = mock(DatabaseClient.class, withSettings().serializable());
        serviceFactory = mock(ServiceFactory.class, withSettings().serializable());
        when(mockSpanner.getDatabaseClient(Matchers.any(DatabaseId.class))).thenReturn(mockDatabaseClient);
        when(mockSpanner.closeAsync()).thenReturn(voidFuture);
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
    public synchronized void singleMutationPipeline() throws Exception {
        Mutation mutation = Mutation
                .newInsertOrUpdateBuilder("test").set("one").to(2).build();
        PCollection<Mutation> mutations = pipeline.apply(Create.of(mutation));

        SpannerIO.Write write = SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance").withDatabaseId
                        ("test-database").withServiceFactory(serviceFactory);
        mutations.apply(write);
        pipeline.run();
        verify(mockSpanner).getDatabaseClient(DatabaseId.of("test-project",
                "test-instance", "test-database"));
        verify(mockDatabaseClient, times(1)).writeAtLeastOnce(argThat(new IterableOfSize(1)));
    }

    @Test
    public synchronized void batching() throws Exception {
        Mutation one = Mutation
                .newInsertOrUpdateBuilder("test").set("one").to(1).build();
        Mutation two = Mutation
                .newInsertOrUpdateBuilder("test").set("two").to(2).build();
        SpannerIO.Write write = SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance")
                .withDatabaseId("test-database")
                .withBatchSize(1000000000)
                .withServiceFactory(serviceFactory);
        SpannerIO.SpannerWriterFn writerFn = new SpannerIO.SpannerWriterFn(write);
        DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writerFn);
        fnTester.processBundle(Arrays.asList(one, two));

        verify(mockSpanner).getDatabaseClient(DatabaseId.of("test-project",
                "test-instance", "test-database"));
        verify(mockDatabaseClient, times(1)).writeAtLeastOnce(argThat(new IterableOfSize(2)));
    }

    @Test
    public synchronized void noBatching() throws Exception {
        Mutation one = Mutation
                .newInsertOrUpdateBuilder("test").set("one").to(1).build();
        Mutation two = Mutation
                .newInsertOrUpdateBuilder("test").set("two").to(2).build();
        SpannerIO.Write write = SpannerIO.write()
                .withProjectId("test-project")
                .withInstanceId("test-instance")
                .withDatabaseId("test-database")
                .withBatchSize(0)  // turn off batching.
                .withServiceFactory(serviceFactory);
        SpannerIO.SpannerWriterFn writerFn = new SpannerIO.SpannerWriterFn(write);
        DoFnTester<Mutation, Void> fnTester = DoFnTester.of(writerFn);
        fnTester.processBundle(Arrays.asList(one, two));

        verify(mockSpanner).getDatabaseClient(DatabaseId.of("test-project",
                "test-instance", "test-database"));
        verify(mockDatabaseClient, times(2)).writeAtLeastOnce(argThat(new IterableOfSize(1)));
    }

    private static class FakeServiceFactory implements ServiceFactory<Spanner, SpannerOptions>,
            Serializable {
        @Override
        public Spanner create(SpannerOptions serviceOptions) {
            return mockSpanner;
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

package org.apache.beam.sdk.io.gcp.firestore;

import com.google.cloud.firestore.*;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class FirestoreWriterFnTest {
    private static final String PROJECT_ID = "testProject";
    private static final String COLLECTION_ID = "testCollection";
    private static final String DOCUMENT_ID = "testDocument";
//    private static final V1Options V_1_OPTIONS;

    static {
//        V_1_OPTIONS = V1Options.from(PROJECT_ID, NAMESPACE, null);
    }

    @Mock
    private Firestore mockFirestore;
    @Mock
    FirestoreFactory mockFirestoreFactory;
    @Mock
    BatchRequesterFactory<String> mockFirestoreBatchRequesterFactory;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    FakeBatchRequester<String> fakeBatchRequester;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(mockFirestoreFactory.create(any(FirestoreOptions.class))).thenReturn(mockFirestore);
        fakeBatchRequester = new FakeBatchRequester<>(mockFirestore);
        when(mockFirestoreBatchRequesterFactory.create(any(Firestore.class))).thenReturn(fakeBatchRequester);
    }

    @Test
    public void testDatastoreWriteFnDisplayData() {
        FirestoreWriterFn<?> firestoreWriter = new FirestoreWriterFn<>(PROJECT_ID, COLLECTION_ID, DOCUMENT_ID, mockFirestoreFactory, new FakeWriteBatcher(), mockFirestoreBatchRequesterFactory);
        DisplayData displayData = DisplayData.from(firestoreWriter);
        assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
        assertThat(displayData, hasDisplayItem("collectionId", COLLECTION_ID));
        assertThat(displayData, hasDisplayItem("documentId", DOCUMENT_ID));
    }

    /**
     * Tests {@link FirestoreWriterFn} with entities less than one batch.
     */
    @Test
    public void testFirestoreWriterFnWithOneBatch() throws Exception {
        firestoreWriterFnTest(100);
    }

    /**
     * Tests {@link FirestoreWriterFn} with entities of more than one batches, but not a multiple.
     */
    @Test
    public void testFirestoreWriterFnWithMultipleBatches() throws Exception {
        firestoreWriterFnTest(WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_START * 3 + 100);
    }

    /**
     * Tests {@link FirestoreWriterFn} with entities of several batches, using an exact multiple of
     * write batch size.
     */
    @Test
    public void testFirestoreWriterFnWithBatchesExactMultiple() throws Exception {
        firestoreWriterFnTest(WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_START * 2);
    }

    @Rule
    public TestPipeline pipeline = TestPipeline.create();

    // A helper method to test FirestoreWriterFn for various batch sizes.
    private void firestoreWriterFnTest(int numMutations) throws Exception {
        // Create the requested number of mutations.
        List<String> mutations = new ArrayList<>(numMutations);
        for (int i = 0; i < numMutations; ++i) {
            mutations.add("{value " + i + "}");
        }

        FirestoreWriterFn firestoreWriter = new FirestoreWriterFn<>(PROJECT_ID, COLLECTION_ID, DOCUMENT_ID, mockFirestoreFactory, new FakeWriteBatcher(), mockFirestoreBatchRequesterFactory);
        DoFnTester<String, Void> doFnTester = DoFnTester.of(firestoreWriter);
        doFnTester.setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE);
        doFnTester.processBundle(mutations);

//        PCollection<String> input = pipeline.apply(Create.of(Arrays.asList("val1", "val2"))).setCoder(StringUtf8Coder.of());
//        input.apply(ParDo.of(new FirestoreWriterFn<>(PROJECT_ID, COLLECTION_ID, DOCUMENT_ID, mockFirestoreFactory, new FakeWriteBatcher(), mockFirestoreBatchRequesterFactory)));
//        pipeline.run();

        assertEquals(1, fakeBatchRequester.getStorage().keySet().size());
    }

    //    /**
//     * Tests {@link FirestoreWriterFn} with large entities that need to be split into more batches.
//     */
//    @Test
//    public void testDatatoreWriterFnWithLargeEntities() throws Exception {
//        List<Mutation> mutations = new ArrayList<>();
//        int entitySize = 0;
//        for (int i = 0; i < 12; ++i) {
//            Entity entity =
//                    Entity.newBuilder()
//                            .setKey(makeKey("key" + i, i + 1))
//                            .putProperties(
//                                    "long",
//                                    makeValue(new String(new char[900_000])).setExcludeFromIndexes(true).build())
//                            .build();
//            entitySize = entity.getSerializedSize(); // Take the size of any one entity.
//            mutations.add(makeUpsert(entity).build());
//        }
//
//        DatastoreWriterFn datastoreWriter =
//                new DatastoreWriterFn(
//                        StaticValueProvider.of(PROJECT_ID), null, mockDatastoreFactory, new FakeWriteBatcher());
//        DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
//        doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
//        doFnTester.processBundle(mutations);
//
//        // This test is over-specific currently; it requires that we split the 12 entity writes into 3
//        // requests, but we only need each CommitRequest to be less than 10MB in size.
//        int entitiesPerRpc = DATASTORE_BATCH_UPDATE_BYTES_LIMIT / entitySize;
//        int start = 0;
//        while (start < mutations.size()) {
//            int end = Math.min(mutations.size(), start + entitiesPerRpc);
//            CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
//            commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
//            commitRequest.addAllMutations(mutations.subList(start, end));
//            // Verify all the batch requests were made with the expected mutations.
//            verify(mockDatastore).commit(commitRequest.build());
//            start = end;
//        }
//    }
//
//    /**
//     * Tests {@link FirestoreWriterFn} with a failed request which is retried.
//     */
//    @Test
//    public void testDatatoreWriterFnRetriesErrors() throws Exception {
//        List<Mutation> mutations = new ArrayList<>();
//        int numRpcs = 2;
//        for (int i = 0; i < WriteBatcherImpl.FIRESTORE_BATCH_UPDATE_ENTITIES_START * numRpcs; ++i) {
//            mutations.add(
//                    makeUpsert(Entity.newBuilder().setKey(makeKey("key" + i, i + 1)).build()).build());
//        }
//
//        CommitResponse successfulCommit = CommitResponse.getDefaultInstance();
//        when(mockDatastore.commit(any(CommitRequest.class)))
//                .thenReturn(successfulCommit)
//                .thenThrow(new DatastoreException("commit", Code.DEADLINE_EXCEEDED, "", null))
//                .thenReturn(successfulCommit);
//
//        DatastoreWriterFn datastoreWriter =
//                new DatastoreWriterFn(
//                        StaticValueProvider.of(PROJECT_ID), null, mockDatastoreFactory, new FakeWriteBatcher());
//        DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
//        doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
//        doFnTester.processBundle(mutations);
//    }
}

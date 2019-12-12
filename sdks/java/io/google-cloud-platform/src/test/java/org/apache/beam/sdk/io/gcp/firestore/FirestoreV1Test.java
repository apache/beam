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
package org.apache.beam.sdk.io.gcp.firestore;

import org.apache.beam.sdk.io.gcp.firestore.FirestoreV1.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;

import java.util.Set;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link FirestoreV1}.
 */
@RunWith(JUnit4.class)
public class FirestoreV1Test {
    private static final String PROJECT_ID = "testProject";
    private static final String COLLECTION_ID = "testCollection";
    private static final String DOCUMENT_ID = "testDocument";
//    private static final V1Options V_1_OPTIONS;

    static {
//        V_1_OPTIONS = V1Options.from(PROJECT_ID, NAMESPACE, null);
    }

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testWriteDisplayData() {
        Write<?> write = FirestoreIO.v1().write()
                .to(COLLECTION_ID)
                .withProjectId(PROJECT_ID)
                .withDocumentId(DOCUMENT_ID);

        DisplayData displayData = DisplayData.from(write);

        assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
        assertThat(displayData, hasDisplayItem("collectionId", COLLECTION_ID));
        assertThat(displayData, hasDisplayItem("documentId", DOCUMENT_ID));
    }

    @Test
    public void testWritePrimitiveDisplayData() {
        DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
        Write<?> write = FirestoreIO.v1().write()
                .to(COLLECTION_ID)
                .withProjectId(PROJECT_ID)
                .withDocumentId(DOCUMENT_ID);

        Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
        assertThat("FirestoreIO write should include the project in its primitive display data", displayData, hasItem(hasDisplayItem("projectId")));
        assertThat("FirestoreIO write should include the collection in its primitive display data", displayData, hasItem(hasDisplayItem("collectionId")));
        assertThat("FirestoreIO write should include the document in its primitive display data", displayData, hasItem(hasDisplayItem("documentId")));
    }

    /**
     * Test building a Write using builder methods.
     */
    @Test
    public void testBuildWrite() {
        Write<?> write = FirestoreIO.v1().write()
                .to(COLLECTION_ID)
                .withProjectId(PROJECT_ID)
                .withDocumentId(DOCUMENT_ID);

        assertEquals(PROJECT_ID, write.getProjectId().get());
        assertEquals(COLLECTION_ID, write.getCollectionId());
        assertEquals(DOCUMENT_ID, write.getDocumentId());
    }

//
//    /**
//     * Tests {@link DatastoreV1.Read#getEstimatedSizeBytes} to fetch and return estimated size for a
//     * query.
//     */
//    @Test
//    public void testEstimatedSizeBytes() throws Exception {
//        long entityBytes = 100L;
//        // In seconds
//        long timestamp = 1234L;
//
//        RunQueryRequest latestTimestampRequest = makeRequest(makeLatestTimestampQuery(NAMESPACE), NAMESPACE);
//        RunQueryResponse latestTimestampResponse = makeLatestTimestampResponse(timestamp);
//        // Per Kind statistics request and response
//        RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE, timestamp), NAMESPACE);
//        RunQueryResponse statResponse = makeStatKindResponse(entityBytes);
//
//        when(mockDatastore.runQuery(latestTimestampRequest)).thenReturn(latestTimestampResponse);
//        when(mockDatastore.runQuery(statRequest)).thenReturn(statResponse);
//
//        assertEquals(entityBytes, getEstimatedSizeBytes(mockDatastore, QUERY, NAMESPACE));
//        verify(mockDatastore, times(1)).runQuery(latestTimestampRequest);
//        verify(mockDatastore, times(1)).runQuery(statRequest);
//    }

    /**
     * Test options. *
     */
    public interface RuntimeTestOptions extends PipelineOptions {
        ValueProvider<String> getDatastoreProject();

        void setDatastoreProject(ValueProvider<String> value);

        ValueProvider<String> getGqlQuery();

        void setGqlQuery(ValueProvider<String> value);

        ValueProvider<String> getNamespace();

        void setNamespace(ValueProvider<String> value);
    }

    /**
     * Test to ensure that {@link ValueProvider} values are not accessed at pipeline construction time
     * when built with {@link DatastoreV1.Read#withQuery(Query)}.
     */
//    @Test
//    public void testRuntimeOptionsNotCalledInApplyQuery() {
//        RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
//        Pipeline pipeline = TestPipeline.create(options);
//        pipeline
//                .apply(
//                        DatastoreIO.v1()
//                                .read()
//                                .withProjectId(options.getDatastoreProject())
//                                .withQuery(QUERY)
//                                .withNamespace(options.getNamespace()))
//                .apply(DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));
//    }

    /**
     * Test to ensure that {@link ValueProvider} values are not accessed at pipeline construction time
     * when built with {@link DatastoreV1.Read#withLiteralGqlQuery(String)}.
     */
//    @Test
//    public void testRuntimeOptionsNotCalledInApplyGqlQuery() {
//        RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
//        Pipeline pipeline = TestPipeline.create(options);
//        pipeline
//                .apply(
//                        DatastoreIO.v1()
//                                .read()
//                                .withProjectId(options.getDatastoreProject())
//                                .withLiteralGqlQuery(options.getGqlQuery()))
//                .apply(DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));
//    }

    /**
     * Builds a per-kind statistics response with the given entity size.
     */
//    private static RunQueryResponse makeStatKindResponse(long entitySizeInBytes) {
//        RunQueryResponse.Builder statKindResponse = RunQueryResponse.newBuilder();
//        Entity.Builder entity = Entity.newBuilder();
//        entity.setKey(makeKey("dummyKind", "dummyId"));
//        entity.putProperties("entity_bytes", makeValue(entitySizeInBytes).build());
//        EntityResult.Builder entityResult = EntityResult.newBuilder();
//        entityResult.setEntity(entity);
//        QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
//        batch.addEntityResults(entityResult);
//        statKindResponse.setBatch(batch);
//        return statKindResponse.build();
//    }
//
//    /**
//     * Builds a response of the given timestamp.
//     */
//    private static RunQueryResponse makeLatestTimestampResponse(long timestamp) {
//        RunQueryResponse.Builder timestampResponse = RunQueryResponse.newBuilder();
//        Entity.Builder entity = Entity.newBuilder();
//        entity.setKey(makeKey("dummyKind", "dummyId"));
//        entity.putProperties("timestamp", makeValue(new Date(timestamp * 1000)).build());
//        EntityResult.Builder entityResult = EntityResult.newBuilder();
//        entityResult.setEntity(entity);
//        QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
//        batch.addEntityResults(entityResult);
//        timestampResponse.setBatch(batch);
//        return timestampResponse.build();
//    }
//
//    /**
//     * Builds a per-kind statistics query for the given timestamp and namespace.
//     */
//    private static Query makeStatKindQuery(String namespace, long timestamp) {
//        Query.Builder statQuery = Query.newBuilder();
//        if (namespace == null) {
//            statQuery.addKindBuilder().setName("__Stat_Kind__");
//        } else {
//            statQuery.addKindBuilder().setName("__Stat_Ns_Kind__");
//        }
//        statQuery.setFilter(
//                makeAndFilter(
////                        makeFilter("kind_name", EQUAL, makeValue(KIND).build()).build(),
//                        makeFilter("timestamp", EQUAL, makeValue(timestamp * 1000000L).build()).build()));
//        return statQuery.build();
//    }
//
//    /**
//     * Builds a latest timestamp statistics query.
//     */
//    private static Query makeLatestTimestampQuery(String namespace) {
//        Query.Builder timestampQuery = Query.newBuilder();
//        if (namespace == null) {
//            timestampQuery.addKindBuilder().setName("__Stat_Total__");
//        } else {
//            timestampQuery.addKindBuilder().setName("__Stat_Ns_Total__");
//        }
//        timestampQuery.addOrder(makeOrder("timestamp", DESCENDING));
//        timestampQuery.setLimit(Int32Value.newBuilder().setValue(1));
//        return timestampQuery.build();
//    }
//
//    /**
//     * Generate dummy query splits.
//     */
//    private List<Query> splitQuery(Query query, int numSplits) {
//        List<Query> queries = new ArrayList<>();
//        int offsetOfOriginal = query.getOffset();
//        for (int i = 0; i < numSplits; i++) {
//            Query.Builder q = Query.newBuilder();
////            q.addKindBuilder().setName(KIND);
//            // Making sub-queries unique (and not equal to the original query) by setting different
//            // offsets.
//            q.setOffset(++offsetOfOriginal);
//            queries.add(q.build());
//        }
//        return queries;
//    }
}
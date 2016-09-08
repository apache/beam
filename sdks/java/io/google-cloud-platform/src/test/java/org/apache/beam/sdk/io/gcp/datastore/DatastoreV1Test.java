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
package org.apache.beam.sdk.io.gcp.datastore;

import static com.google.datastore.v1.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeDelete;
import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1.client.DatastoreHelper.makeUpsert;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DATASTORE_BATCH_UPDATE_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.DEFAULT_BUNDLE_SIZE_BYTES;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.QUERY_BATCH_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.getEstimatedSizeBytes;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.makeRequest;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.isValidKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.QuerySplitter;
import com.google.protobuf.Int32Value;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DatastoreWriterFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DeleteEntity;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DeleteEntityFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DeleteKey;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DeleteKeyFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.ReadFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.SplitQueryFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.V1Options;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.UpsertFn;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.V1DatastoreFactory;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Write;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.DoFnTester.CloningBehavior;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link DatastoreV1}.
 */
@RunWith(JUnit4.class)
public class DatastoreV1Test {
  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  private static final V1Options V_1_OPTIONS;
  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
    V_1_OPTIONS = V1Options.from(PROJECT_ID, QUERY, NAMESPACE);
  }
  private DatastoreV1.Read initialRead;

  @Mock
  Datastore mockDatastore;
  @Mock
  QuerySplitter mockQuerySplitter;
  @Mock
  V1DatastoreFactory mockDatastoreFactory;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    initialRead = DatastoreIO.v1().read()
        .withProjectId(PROJECT_ID).withQuery(QUERY).withNamespace(NAMESPACE);

    when(mockDatastoreFactory.getDatastore(any(PipelineOptions.class), any(String.class)))
        .thenReturn(mockDatastore);
    when(mockDatastoreFactory.getQuerySplitter())
        .thenReturn(mockQuerySplitter);
  }

  @Test
  public void testBuildRead() throws Exception {
    DatastoreV1.Read read = DatastoreIO.v1().read()
        .withProjectId(PROJECT_ID).withQuery(QUERY).withNamespace(NAMESPACE);
    assertEquals(QUERY, read.getQuery());
    assertEquals(PROJECT_ID, read.getProjectId());
    assertEquals(NAMESPACE, read.getNamespace());
  }

  /**
   * {@link #testBuildRead} but constructed in a different order.
   */
  @Test
  public void testBuildReadAlt() throws Exception {
    DatastoreV1.Read read =  DatastoreIO.v1().read()
        .withProjectId(PROJECT_ID).withNamespace(NAMESPACE).withQuery(QUERY);
    assertEquals(QUERY, read.getQuery());
    assertEquals(PROJECT_ID, read.getProjectId());
    assertEquals(NAMESPACE, read.getNamespace());
  }

  @Test
  public void testReadValidationFailsProject() throws Exception {
    DatastoreV1.Read read =  DatastoreIO.v1().read().withQuery(QUERY);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("project");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQuery() throws Exception {
    DatastoreV1.Read read =  DatastoreIO.v1().read().withProjectId(PROJECT_ID);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("query");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQueryLimitZero() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(0)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit 0: must be positive");

    DatastoreIO.v1().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationFailsQueryLimitNegative() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(-5)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit -5: must be positive");

    DatastoreIO.v1().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationSucceedsNamespace() throws Exception {
    DatastoreV1.Read read =  DatastoreIO.v1().read().withProjectId(PROJECT_ID).withQuery(QUERY);
    /* Should succeed, as a null namespace is fine. */
    read.validate(null);
  }

  @Test
  public void testReadDisplayData() {
    DatastoreV1.Read read =  DatastoreIO.v1().read()
        .withProjectId(PROJECT_ID)
        .withQuery(QUERY)
        .withNamespace(NAMESPACE);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
    assertThat(displayData, hasDisplayItem("query", QUERY.toString()));
    assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testSourcePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PBegin, ? extends POutput> read = DatastoreIO.v1().read().withProjectId(
        "myProject").withQuery(Query.newBuilder().build());

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat("DatastoreIO read should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
  }

  @Test
  public void testWriteDoesNotAllowNullProject() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    DatastoreIO.v1().write().withProjectId(null);
  }

  @Test
  public void testWriteValidationFailsWithNoProject() throws Exception {
    Write write =  DatastoreIO.v1().write();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    write.validate(null);
  }

  @Test
  public void testWriteValidationSucceedsWithProject() throws Exception {
    Write write =  DatastoreIO.v1().write().withProjectId(PROJECT_ID);
    write.validate(null);
  }

  @Test
  public void testWriteDisplayData() {
    Write write =  DatastoreIO.v1().write().withProjectId(PROJECT_ID);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  @Test
  public void testDeleteEntityDoesNotAllowNullProject() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    DatastoreIO.v1().deleteEntity().withProjectId(null);
  }

  @Test
  public void testDeleteEntityValidationFailsWithNoProject() throws Exception {
    DeleteEntity deleteEntity = DatastoreIO.v1().deleteEntity();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    deleteEntity.validate(null);
  }

  @Test
  public void testDeleteEntityValidationSucceedsWithProject() throws Exception {
    DeleteEntity deleteEntity = DatastoreIO.v1().deleteEntity().withProjectId(PROJECT_ID);
    deleteEntity.validate(null);
  }

  @Test
  public void testDeleteEntityDisplayData() {
    DeleteEntity deleteEntity =  DatastoreIO.v1().deleteEntity().withProjectId(PROJECT_ID);

    DisplayData displayData = DisplayData.from(deleteEntity);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  @Test
  public void testDeleteKeyDoesNotAllowNullProject() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    DatastoreIO.v1().deleteKey().withProjectId(null);
  }

  @Test
  public void testDeleteKeyValidationFailsWithNoProject() throws Exception {
    DeleteKey deleteKey = DatastoreIO.v1().deleteKey();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    deleteKey.validate(null);
  }

  @Test
  public void testDeleteKeyValidationSucceedsWithProject() throws Exception {
    DeleteKey deleteKey = DatastoreIO.v1().deleteKey().withProjectId(PROJECT_ID);
    deleteKey.validate(null);
  }

  @Test
  public void testDeleteKeyDisplayData() {
    DeleteKey deleteKey =  DatastoreIO.v1().deleteKey().withProjectId(PROJECT_ID);

    DisplayData displayData = DisplayData.from(deleteKey);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testWritePrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PCollection<Entity>, ?> write =
        DatastoreIO.v1().write().withProjectId("myProject");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("DatastoreIO write should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
    assertThat("DatastoreIO write should include the upsertFn in its primitive display data",
        displayData, hasItem(hasDisplayItem("upsertFn")));

  }

  @Test
  @Category(RunnableOnService.class)
  public void testDeleteEntityPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PCollection<Entity>, ?> write =
        DatastoreIO.v1().deleteEntity().withProjectId("myProject");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("DatastoreIO write should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
    assertThat("DatastoreIO write should include the deleteEntityFn in its primitive display data",
        displayData, hasItem(hasDisplayItem("deleteEntityFn")));

  }

  @Test
  @Category(RunnableOnService.class)
  public void testDeleteKeyPrimitiveDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
    PTransform<PCollection<Key>, ?> write =
        DatastoreIO.v1().deleteKey().withProjectId("myProject");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("DatastoreIO write should include the project in its primitive display data",
        displayData, hasItem(hasDisplayItem("projectId")));
    assertThat("DatastoreIO write should include the deleteKeyFn in its primitive display data",
        displayData, hasItem(hasDisplayItem("deleteKeyFn")));

  }

  /**
   * Test building a Write using builder methods.
   */
  @Test
  public void testBuildWrite() throws Exception {
    DatastoreV1.Write write =  DatastoreIO.v1().write().withProjectId(PROJECT_ID);
    assertEquals(PROJECT_ID, write.getProjectId());
  }

  /**
   * Test the detection of complete and incomplete keys.
   */
  @Test
  public void testHasNameOrId() {
    Key key;
    // Complete with name, no ancestor
    key = makeKey("bird", "finch").build();
    assertTrue(isValidKey(key));

    // Complete with id, no ancestor
    key = makeKey("bird", 123).build();
    assertTrue(isValidKey(key));

    // Incomplete, no ancestor
    key = makeKey("bird").build();
    assertFalse(isValidKey(key));

    // Complete with name and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", "horned").build();
    assertTrue(isValidKey(key));

    // Complete with id and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", 123).build();
    assertTrue(isValidKey(key));

    // Incomplete with ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird").build();
    assertFalse(isValidKey(key));

    key = makeKey().build();
    assertFalse(isValidKey(key));
  }

  /**
   * Test that entities with incomplete keys cannot be updated.
   */
  @Test
  public void testAddEntitiesWithIncompleteKeys() throws Exception {
    Key key = makeKey("bird").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    UpsertFn upsertFn = new UpsertFn();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Entities to be written to the Cloud Datastore must have complete keys");

    upsertFn.apply(entity);
  }

  @Test
  /**
   * Test that entities with valid keys are transformed to upsert mutations.
   */
  public void testAddEntities() throws Exception {
    Key key = makeKey("bird", "finch").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    UpsertFn upsertFn = new UpsertFn();

    Mutation exceptedMutation = makeUpsert(entity).build();
    assertEquals(upsertFn.apply(entity), exceptedMutation);
  }

  /**
   * Test that entities with incomplete keys cannot be deleted.
   */
  @Test
  public void testDeleteEntitiesWithIncompleteKeys() throws Exception {
    Key key = makeKey("bird").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    DeleteEntityFn deleteEntityFn = new DeleteEntityFn();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Entities to be deleted from the Cloud Datastore must have complete keys");

    deleteEntityFn.apply(entity);
  }

  /**
   * Test that entities with valid keys are transformed to delete mutations.
   */
  @Test
  public void testDeleteEntities() throws Exception {
    Key key = makeKey("bird", "finch").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    DeleteEntityFn deleteEntityFn = new DeleteEntityFn();

    Mutation exceptedMutation = makeDelete(entity.getKey()).build();
    assertEquals(deleteEntityFn.apply(entity), exceptedMutation);
  }

  /**
   * Test that incomplete keys cannot be deleted.
   */
  @Test
  public void testDeleteIncompleteKeys() throws Exception {
    Key key = makeKey("bird").build();
    DeleteKeyFn deleteKeyFn = new DeleteKeyFn();

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Keys to be deleted from the Cloud Datastore must be complete");

    deleteKeyFn.apply(key);
  }

  /**
   * Test that valid keys are transformed to delete mutations.
   */
  @Test
  public void testDeleteKeys() throws Exception {
    Key key = makeKey("bird", "finch").build();
    DeleteKeyFn deleteKeyFn = new DeleteKeyFn();

    Mutation exceptedMutation = makeDelete(key).build();
    assertEquals(deleteKeyFn.apply(key), exceptedMutation);
  }

  @Test
  public void testDatastoreWriteFnDisplayData() {
    DatastoreWriterFn datastoreWriter = new DatastoreWriterFn(PROJECT_ID);
    DisplayData displayData = DisplayData.from(datastoreWriter);
    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  /** Tests {@link DatastoreWriterFn} with entities less than one batch. */
  @Test
  public void testDatatoreWriterFnWithOneBatch() throws Exception {
    datastoreWriterFnTest(100);
  }

  /** Tests {@link DatastoreWriterFn} with entities of more than one batches, but not a multiple. */
  @Test
  public void testDatatoreWriterFnWithMultipleBatches() throws Exception {
    datastoreWriterFnTest(DATASTORE_BATCH_UPDATE_LIMIT * 3 + 100);
  }

  /**
   * Tests {@link DatastoreWriterFn} with entities of several batches, using an exact multiple of
   * write batch size.
   */
  @Test
  public void testDatatoreWriterFnWithBatchesExactMultiple() throws Exception {
    datastoreWriterFnTest(DATASTORE_BATCH_UPDATE_LIMIT * 2);
  }

  // A helper method to test DatastoreWriterFn for various batch sizes.
  private void datastoreWriterFnTest(int numMutations) throws Exception {
    // Create the requested number of mutations.
    List<Mutation> mutations = new ArrayList<>(numMutations);
    for (int i = 0; i < numMutations; ++i) {
      mutations.add(
          makeUpsert(Entity.newBuilder().setKey(makeKey("key" + i, i + 1)).build()).build());
    }

    DatastoreWriterFn datastoreWriter = new DatastoreWriterFn(PROJECT_ID, mockDatastoreFactory);
    DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    doFnTester.processBundle(mutations);

    int start = 0;
    while (start < numMutations) {
      int end = Math.min(numMutations, start + DATASTORE_BATCH_UPDATE_LIMIT);
      CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
      commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
      commitRequest.addAllMutations(mutations.subList(start, end));
      // Verify all the batch requests were made with the expected mutations.
      verify(mockDatastore, times(1)).commit(commitRequest.build());
      start = end;
    }
  }

  /**
   * Tests {@link DatastoreV1.Read#getEstimatedSizeBytes} to fetch and return estimated size for a
   * query.
   */
  @Test
  public void testEstimatedSizeBytes() throws Exception {
    long entityBytes = 100L;
    // In seconds
    long timestamp = 1234L;

    RunQueryRequest latestTimestampRequest = makeRequest(makeLatestTimestampQuery(NAMESPACE),
        NAMESPACE);
    RunQueryResponse latestTimestampResponse = makeLatestTimestampResponse(timestamp);
    // Per Kind statistics request and response
    RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE, timestamp), NAMESPACE);
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastore.runQuery(latestTimestampRequest))
        .thenReturn(latestTimestampResponse);
    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);

    assertEquals(entityBytes, getEstimatedSizeBytes(mockDatastore, QUERY, NAMESPACE));
    verify(mockDatastore, times(1)).runQuery(latestTimestampRequest);
    verify(mockDatastore, times(1)).runQuery(statRequest);
  }

  /**
   * Tests {@link SplitQueryFn} when number of query splits is specified.
   */
  @Test
  public void testSplitQueryFnWithNumSplits() throws Exception {
    int numSplits = 100;
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, numSplits));

    SplitQueryFn splitQueryFn = new SplitQueryFn(V_1_OPTIONS, numSplits, mockDatastoreFactory);
    DoFnTester<Query, KV<Integer, Query>> doFnTester = DoFnTester.of(splitQueryFn);
    /**
     * Although Datastore client is marked transient in {@link SplitQueryFn}, when injected through
     * mock factory using a when clause for unit testing purposes, it is not serializable
     * because it doesn't have a no-arg constructor. Thus disabling the cloning to prevent the
     * doFn from being serialized.
     */
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<KV<Integer, Query>> queries = doFnTester.processBundle(QUERY);

    assertEquals(queries.size(), numSplits);
    verifyUniqueKeys(queries);
    verify(mockQuerySplitter, times(1)).getSplits(
        eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class));
    verifyZeroInteractions(mockDatastore);
  }

  /**
   * Tests {@link SplitQueryFn} when no query splits is specified.
   */
  @Test
  public void testSplitQueryFnWithoutNumSplits() throws Exception {
    // Force SplitQueryFn to compute the number of query splits
    int numSplits = 0;
    int expectedNumSplits = 20;
    long entityBytes = expectedNumSplits * DEFAULT_BUNDLE_SIZE_BYTES;
    // In seconds
    long timestamp = 1234L;

    RunQueryRequest latestTimestampRequest = makeRequest(makeLatestTimestampQuery(NAMESPACE),
        NAMESPACE);
    RunQueryResponse latestTimestampResponse = makeLatestTimestampResponse(timestamp);

    // Per Kind statistics request and response
    RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE, timestamp), NAMESPACE);
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastore.runQuery(latestTimestampRequest))
        .thenReturn(latestTimestampResponse);
    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, expectedNumSplits));

    SplitQueryFn splitQueryFn = new SplitQueryFn(V_1_OPTIONS, numSplits, mockDatastoreFactory);
    DoFnTester<Query, KV<Integer, Query>> doFnTester = DoFnTester.of(splitQueryFn);
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<KV<Integer, Query>> queries = doFnTester.processBundle(QUERY);

    assertEquals(queries.size(), expectedNumSplits);
    verifyUniqueKeys(queries);
    verify(mockQuerySplitter, times(1)).getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class));
    verify(mockDatastore, times(1)).runQuery(latestTimestampRequest);
    verify(mockDatastore, times(1)).runQuery(statRequest);
  }

  /**
   * Tests {@link DatastoreV1.Read.SplitQueryFn} when the query has a user specified limit.
   */
  @Test
  public void testSplitQueryFnWithQueryLimit() throws Exception {
    Query queryWithLimit = QUERY.toBuilder().clone()
        .setLimit(Int32Value.newBuilder().setValue(1))
        .build();

    SplitQueryFn splitQueryFn = new SplitQueryFn(V_1_OPTIONS, 10, mockDatastoreFactory);
    DoFnTester<Query, KV<Integer, Query>> doFnTester = DoFnTester.of(splitQueryFn);
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<KV<Integer, Query>> queries = doFnTester.processBundle(queryWithLimit);

    assertEquals(queries.size(), 1);
    verifyUniqueKeys(queries);
    verifyNoMoreInteractions(mockDatastore);
    verifyNoMoreInteractions(mockQuerySplitter);
  }

  /** Tests {@link ReadFn} with a query limit less than one batch. */
  @Test
  public void testReadFnWithOneBatch() throws Exception {
    readFnTest(5);
  }

  /** Tests {@link ReadFn} with a query limit more than one batch, and not a multiple. */
  @Test
  public void testReadFnWithMultipleBatches() throws Exception {
    readFnTest(QUERY_BATCH_LIMIT + 5);
  }

  /** Tests {@link ReadFn} for several batches, using an exact multiple of batch size results. */
  @Test
  public void testReadFnWithBatchesExactMultiple() throws Exception {
    readFnTest(5 * QUERY_BATCH_LIMIT);
  }

  /** Helper Methods */

  /** A helper function that verifies if all the queries have unique keys. */
  private void verifyUniqueKeys(List<KV<Integer, Query>> queries) {
    Set<Integer> keys = new HashSet<>();
    for (KV<Integer, Query> kv: queries) {
      keys.add(kv.getKey());
    }
    assertEquals(keys.size(), queries.size());
  }

  /**
   * A helper function that creates mock {@link Entity} results in response to a query. Always
   * indicates that more results are available, unless the batch is limited to fewer than
   * {@link DatastoreV1.Read#QUERY_BATCH_LIMIT} results.
   */
  private static RunQueryResponse mockResponseForQuery(Query q) {
    // Every query DatastoreV1 sends should have a limit.
    assertTrue(q.hasLimit());

    // The limit should be in the range [1, QUERY_BATCH_LIMIT]
    int limit = q.getLimit().getValue();
    assertThat(limit, greaterThanOrEqualTo(1));
    assertThat(limit, lessThanOrEqualTo(QUERY_BATCH_LIMIT));

    // Create the requested number of entities.
    List<EntityResult> entities = new ArrayList<>(limit);
    for (int i = 0; i < limit; ++i) {
      entities.add(
          EntityResult.newBuilder()
              .setEntity(Entity.newBuilder().setKey(makeKey("key" + i, i + 1)))
              .build());
    }

    // Fill out the other parameters on the returned result batch.
    RunQueryResponse.Builder ret = RunQueryResponse.newBuilder();
    ret.getBatchBuilder()
        .addAllEntityResults(entities)
        .setEntityResultType(EntityResult.ResultType.FULL)
        .setMoreResults(
            limit == QUERY_BATCH_LIMIT
                ? QueryResultBatch.MoreResultsType.NOT_FINISHED
                : QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);

    return ret.build();
  }

  /** Helper function to run a test reading from a {@link ReadFn}. */
  private void readFnTest(int numEntities) throws Exception {
    // An empty query to read entities.
    Query query = Query.newBuilder().setLimit(
        Int32Value.newBuilder().setValue(numEntities)).build();

    // Use mockResponseForQuery to generate results.
    when(mockDatastore.runQuery(any(RunQueryRequest.class)))
        .thenAnswer(new Answer<RunQueryResponse>() {
          @Override
          public RunQueryResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
            Query q = ((RunQueryRequest) invocationOnMock.getArguments()[0]).getQuery();
            return mockResponseForQuery(q);
          }
        });

    ReadFn readFn = new ReadFn(V_1_OPTIONS, mockDatastoreFactory);
    DoFnTester<Query, Entity> doFnTester = DoFnTester.of(readFn);
    /**
     * Although Datastore client is marked transient in {@link ReadFn}, when injected through
     * mock factory using a when clause for unit testing purposes, it is not serializable
     * because it doesn't have a no-arg constructor. Thus disabling the cloning to prevent the
     * test object from being serialized.
     */
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<Entity> entities = doFnTester.processBundle(query);

    int expectedNumCallsToRunQuery = (int) Math.ceil((double) numEntities / QUERY_BATCH_LIMIT);
    verify(mockDatastore, times(expectedNumCallsToRunQuery)).runQuery(any(RunQueryRequest.class));
    // Validate the number of results.
    assertEquals(numEntities, entities.size());
  }

  /** Builds a per-kind statistics response with the given entity size. */
  private static RunQueryResponse makeStatKindResponse(long entitySizeInBytes) {
    RunQueryResponse.Builder statKindResponse = RunQueryResponse.newBuilder();
    Entity.Builder entity = Entity.newBuilder();
    entity.setKey(makeKey("dummyKind", "dummyId"));
    entity.getMutableProperties().put("entity_bytes", makeValue(entitySizeInBytes).build());
    EntityResult.Builder entityResult = EntityResult.newBuilder();
    entityResult.setEntity(entity);
    QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
    batch.addEntityResults(entityResult);
    statKindResponse.setBatch(batch);
    return statKindResponse.build();
  }

  /** Builds a response of the given timestamp. */
  private static RunQueryResponse makeLatestTimestampResponse(long timestamp) {
    RunQueryResponse.Builder timestampResponse = RunQueryResponse.newBuilder();
    Entity.Builder entity = Entity.newBuilder();
    entity.setKey(makeKey("dummyKind", "dummyId"));
    entity.getMutableProperties().put("timestamp", makeValue(new Date(timestamp * 1000)).build());
    EntityResult.Builder entityResult = EntityResult.newBuilder();
    entityResult.setEntity(entity);
    QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
    batch.addEntityResults(entityResult);
    timestampResponse.setBatch(batch);
    return timestampResponse.build();
  }

  /** Builds a per-kind statistics query for the given timestamp and namespace. */
  private static Query makeStatKindQuery(String namespace, long timestamp) {
    Query.Builder statQuery = Query.newBuilder();
    if (namespace == null) {
      statQuery.addKindBuilder().setName("__Stat_Kind__");
    } else {
      statQuery.addKindBuilder().setName("__Stat_Ns_Kind__");
    }
    statQuery.setFilter(makeAndFilter(
        makeFilter("kind_name", EQUAL, makeValue(KIND).build()).build(),
        makeFilter("timestamp", EQUAL, makeValue(timestamp * 1000000L).build()).build()));
    return statQuery.build();
  }

  /** Builds a latest timestamp statistics query. */
  private static Query makeLatestTimestampQuery(String namespace) {
    Query.Builder timestampQuery = Query.newBuilder();
    if (namespace == null) {
      timestampQuery.addKindBuilder().setName("__Stat_Total__");
    } else {
      timestampQuery.addKindBuilder().setName("__Stat_Ns_Total__");
    }
    timestampQuery.addOrder(makeOrder("timestamp", DESCENDING));
    timestampQuery.setLimit(Int32Value.newBuilder().setValue(1));
    return timestampQuery.build();
  }


  /** Generate dummy query splits. */
  private List<Query> splitQuery(Query query, int numSplits) {
    List<Query> queries = new LinkedList<>();
    for (int i = 0; i < numSplits; i++) {
      queries.add(query.toBuilder().clone().build());
    }
    return queries;
  }
}

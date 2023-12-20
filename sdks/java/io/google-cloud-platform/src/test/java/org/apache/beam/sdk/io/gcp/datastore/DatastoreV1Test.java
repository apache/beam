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
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.DATASTORE_BATCH_UPDATE_BYTES_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.DEFAULT_BUNDLE_SIZE_BYTES;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.QUERY_BATCH_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.getEstimatedSizeBytes;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.makeRequest;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.Read.translateGqlQueryWithLimitCheck;
import static org.apache.beam.sdk.io.gcp.datastore.DatastoreV1.isValidKey;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.EntityResult;
import com.google.datastore.v1.GqlQuery;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Query;
import com.google.datastore.v1.QueryResultBatch;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.datastore.v1.client.Datastore;
import com.google.datastore.v1.client.DatastoreException;
import com.google.datastore.v1.client.QuerySplitter;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.runners.core.metrics.MonitoringInfoMetricName;
import org.apache.beam.sdk.Pipeline;
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
import org.apache.beam.sdk.metrics.MetricsEnvironment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.DoFnTester.CloningBehavior;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DatastoreV1}. */
@RunWith(Enclosed.class)
public class DatastoreV1Test {
  private static final String PROJECT_ID = "testProject";
  private static final String DATABASE_ID = "";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  private static final String LOCALHOST = "localhost:9955";
  private static final String GQL_QUERY = "SELECT * from " + KIND;
  private static final Instant TIMESTAMP = Instant.now();
  private static final Timestamp TIMESTAMP_PROTO = Timestamps.fromMillis(TIMESTAMP.getMillis());
  private static final V1Options V_1_OPTIONS;

  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
    V_1_OPTIONS = V1Options.from(PROJECT_ID, StaticValueProvider.of(DATABASE_ID), NAMESPACE, null);
  }

  @Mock protected Datastore mockDatastore;
  @Mock protected QuerySplitter mockQuerySplitter;
  @Mock protected V1DatastoreFactory mockDatastoreFactory;

  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(mockDatastoreFactory.getDatastore(any(PipelineOptions.class), any(String.class), any()))
        .thenReturn(mockDatastore);
    when(mockDatastoreFactory.getDatastore(
            any(PipelineOptions.class), any(String.class), any(String.class), any()))
        .thenReturn(mockDatastore);
    when(mockDatastoreFactory.getQuerySplitter()).thenReturn(mockQuerySplitter);
    // Setup the ProcessWideContainer for testing metrics are set.
    MetricsContainerImpl container = new MetricsContainerImpl(null);
    MetricsEnvironment.setProcessWideContainer(container);
  }

  @RunWith(JUnit4.class)
  public static class SingletonTests extends DatastoreV1Test {
    @Test
    public void testBuildRead() throws Exception {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withQuery(QUERY)
              .withNamespace(NAMESPACE);
      assertEquals(QUERY, read.getQuery());
      assertEquals(PROJECT_ID, read.getProjectId().get());
      assertEquals(DATABASE_ID, read.getDatabaseId().get());
      assertEquals(NAMESPACE, read.getNamespace().get());
    }

    @Test
    public void testBuildReadWithReadTime() throws Exception {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withQuery(QUERY)
              .withReadTime(TIMESTAMP);
      assertEquals(TIMESTAMP, read.getReadTime());
      assertEquals(QUERY, read.getQuery());
      assertEquals(PROJECT_ID, read.getProjectId().get());
      assertEquals(DATABASE_ID, read.getDatabaseId().get());
    }

    @Test
    public void testBuildReadWithGqlQuery() throws Exception {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withLiteralGqlQuery(GQL_QUERY)
              .withNamespace(NAMESPACE);
      assertEquals(GQL_QUERY, read.getLiteralGqlQuery().get());
      assertEquals(PROJECT_ID, read.getProjectId().get());
      assertEquals(DATABASE_ID, read.getDatabaseId().get());
      assertEquals(NAMESPACE, read.getNamespace().get());
    }

    /** {@link #testBuildRead} but constructed in a different order. */
    @Test
    public void testBuildReadAlt() throws Exception {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withReadTime(TIMESTAMP)
              .withQuery(QUERY)
              .withNamespace(NAMESPACE)
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withLocalhost(LOCALHOST);
      assertEquals(TIMESTAMP, read.getReadTime());
      assertEquals(QUERY, read.getQuery());
      assertEquals(PROJECT_ID, read.getProjectId().get());
      assertEquals(DATABASE_ID, read.getDatabaseId().get());
      assertEquals(NAMESPACE, read.getNamespace().get());
      assertEquals(LOCALHOST, read.getLocalhost());
    }

    @Test
    public void testReadValidationFailsQueryAndGqlQuery() throws Exception {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withLiteralGqlQuery(GQL_QUERY)
              .withQuery(QUERY);

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("withQuery() and withLiteralGqlQuery() are exclusive");
      read.expand(null);
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
      Query invalidLimit =
          Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(-5)).build();
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Invalid query limit -5: must be positive");

      DatastoreIO.v1().read().withQuery(invalidLimit);
    }

    @Test
    public void testReadDisplayData() {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withQuery(QUERY)
              .withNamespace(NAMESPACE)
              .withReadTime(TIMESTAMP);

      DisplayData displayData = DisplayData.from(read);

      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
      assertThat(displayData, hasDisplayItem("databaseId", DATABASE_ID));
      assertThat(displayData, hasDisplayItem("query", QUERY.toString()));
      assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
      assertThat(displayData, hasDisplayItem("readTime", TIMESTAMP));
    }

    @Test
    public void testReadDisplayDataWithGqlQuery() {
      DatastoreV1.Read read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withLiteralGqlQuery(GQL_QUERY)
              .withNamespace(NAMESPACE)
              .withReadTime(TIMESTAMP);

      DisplayData displayData = DisplayData.from(read);

      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
      assertThat(displayData, hasDisplayItem("gqlQuery", GQL_QUERY));
      assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
      assertThat(displayData, hasDisplayItem("readTime", TIMESTAMP));
    }

    @Test
    public void testSourcePrimitiveDisplayData() {
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
      int numSplits = 98;
      PTransform<PBegin, PCollection<Entity>> read =
          DatastoreIO.v1()
              .read()
              .withProjectId(PROJECT_ID)
              .withDatabaseId(DATABASE_ID)
              .withQuery(Query.newBuilder().build())
              .withNumQuerySplits(numSplits);

      String assertMessage =
          "DatastoreIO read should include the '%s' in its primitive display data";
      Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
      assertThat(
          String.format(assertMessage, "project id"),
          displayData,
          hasItem(hasDisplayItem("projectId", PROJECT_ID)));
      assertThat(
          String.format(assertMessage, "number of query splits"),
          displayData,
          hasItem(hasDisplayItem("numQuerySplits", numSplits)));
    }

    @Test
    public void testWriteDisplayData() {
      Write write = DatastoreIO.v1().write().withProjectId(PROJECT_ID).withDatabaseId(DATABASE_ID);

      DisplayData displayData = DisplayData.from(write);

      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
      assertThat(displayData, hasDisplayItem("databaseId", DATABASE_ID));
    }

    @Test
    public void testDeleteEntityDisplayData() {
      DeleteEntity deleteEntity =
          DatastoreIO.v1().deleteEntity().withProjectId(PROJECT_ID).withDatabaseId(DATABASE_ID);

      DisplayData displayData = DisplayData.from(deleteEntity);

      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
      assertThat(displayData, hasDisplayItem("databaseId", DATABASE_ID));
    }

    @Test
    public void testDeleteKeyDisplayData() {
      DeleteKey deleteKey =
          DatastoreIO.v1().deleteKey().withProjectId(PROJECT_ID).withDatabaseId(DATABASE_ID);

      DisplayData displayData = DisplayData.from(deleteKey);

      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
      assertThat(displayData, hasDisplayItem("databaseId", DATABASE_ID));
    }

    @Test
    public void testWritePrimitiveDisplayData() {
      int hintNumWorkers = 10;
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
      PTransform<PCollection<Entity>, ?> write =
          DatastoreIO.v1().write().withProjectId("myProject").withHintNumWorkers(hintNumWorkers);

      Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
      assertThat(
          "DatastoreIO write should include the project in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("projectId")));
      assertThat(
          "DatastoreIO write should include the upsertFn in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("upsertFn")));
      assertThat(
          "DatastoreIO write should include ramp-up throttling worker count hint if enabled",
          displayData,
          hasItem(hasDisplayItem("hintNumWorkers", hintNumWorkers)));
    }

    @Test
    public void testWritePrimitiveDisplayDataDisabledThrottler() {
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
      PTransform<PCollection<Entity>, ?> write =
          DatastoreIO.v1().write().withProjectId("myProject").withRampupThrottlingDisabled();

      Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
      assertThat(
          "DatastoreIO write should include the project in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("projectId")));
      assertThat(
          "DatastoreIO write should include the upsertFn in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("upsertFn")));
      assertThat(
          "DatastoreIO write should include ramp-up throttling worker count hint if enabled",
          displayData,
          not(hasItem(hasDisplayItem("hintNumWorkers"))));
    }

    @Test
    public void testDeleteEntityPrimitiveDisplayData() {
      int hintNumWorkers = 10;
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
      PTransform<PCollection<Entity>, ?> write =
          DatastoreIO.v1()
              .deleteEntity()
              .withProjectId("myProject")
              .withHintNumWorkers(hintNumWorkers);

      Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
      assertThat(
          "DatastoreIO write should include the project in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("projectId")));
      assertThat(
          "DatastoreIO write should include the deleteEntityFn in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("deleteEntityFn")));
      assertThat(
          "DatastoreIO write should include ramp-up throttling worker count hint if enabled",
          displayData,
          hasItem(hasDisplayItem("hintNumWorkers", hintNumWorkers)));
    }

    @Test
    public void testDeleteKeyPrimitiveDisplayData() {
      int hintNumWorkers = 10;
      DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();
      PTransform<PCollection<Key>, ?> write =
          DatastoreIO.v1()
              .deleteKey()
              .withProjectId("myProject")
              .withHintNumWorkers(hintNumWorkers);

      Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
      assertThat(
          "DatastoreIO write should include the project in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("projectId")));
      assertThat(
          "DatastoreIO write should include the deleteKeyFn in its primitive display data",
          displayData,
          hasItem(hasDisplayItem("deleteKeyFn")));
      assertThat(
          "DatastoreIO write should include ramp-up throttling worker count hint if enabled",
          displayData,
          hasItem(hasDisplayItem("hintNumWorkers", hintNumWorkers)));
    }

    /** Test building a Write using builder methods. */
    @Test
    public void testBuildWrite() throws Exception {
      DatastoreV1.Write write = DatastoreIO.v1().write().withProjectId(PROJECT_ID);
      assertEquals(PROJECT_ID, write.getProjectId());
    }

    /** Test the detection of complete and incomplete keys. */
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

    /** Test that entities with incomplete keys cannot be updated. */
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
    /** Test that entities with valid keys are transformed to upsert mutations. */
    public void testAddEntities() throws Exception {
      Key key = makeKey("bird", "finch").build();
      Entity entity = Entity.newBuilder().setKey(key).build();
      UpsertFn upsertFn = new UpsertFn();

      Mutation expectedMutation = makeUpsert(entity).build();
      assertEquals(expectedMutation, upsertFn.apply(entity));
    }

    /** Test that entities with incomplete keys cannot be deleted. */
    @Test
    public void testDeleteEntitiesWithIncompleteKeys() throws Exception {
      Key key = makeKey("bird").build();
      Entity entity = Entity.newBuilder().setKey(key).build();
      DeleteEntityFn deleteEntityFn = new DeleteEntityFn();

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage(
          "Entities to be deleted from the Cloud Datastore must have complete keys");

      deleteEntityFn.apply(entity);
    }

    /** Test that entities with valid keys are transformed to delete mutations. */
    @Test
    public void testDeleteEntities() throws Exception {
      Key key = makeKey("bird", "finch").build();
      Entity entity = Entity.newBuilder().setKey(key).build();
      DeleteEntityFn deleteEntityFn = new DeleteEntityFn();

      Mutation expectedMutation = makeDelete(entity.getKey()).build();
      assertEquals(expectedMutation, deleteEntityFn.apply(entity));
    }

    /** Test that incomplete keys cannot be deleted. */
    @Test
    public void testDeleteIncompleteKeys() throws Exception {
      Key key = makeKey("bird").build();
      DeleteKeyFn deleteKeyFn = new DeleteKeyFn();

      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Keys to be deleted from the Cloud Datastore must be complete");

      deleteKeyFn.apply(key);
    }

    /** Test that valid keys are transformed to delete mutations. */
    @Test
    public void testDeleteKeys() {
      Key key = makeKey("bird", "finch").build();
      DeleteKeyFn deleteKeyFn = new DeleteKeyFn();

      Mutation expectedMutation = makeDelete(key).build();
      assertEquals(expectedMutation, deleteKeyFn.apply(key));
    }

    @Test
    public void testDatastoreWriteFnDisplayData() {
      DatastoreWriterFn datastoreWriter = new DatastoreWriterFn(PROJECT_ID, null);
      DisplayData displayData = DisplayData.from(datastoreWriter);
      assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
    }

    /** Tests {@link DatastoreWriterFn} with entities less than one batch. */
    @Test
    public void testDatastoreWriterFnWithOneBatch() throws Exception {
      datastoreWriterFnTest(100);
      verifyMetricWasSet("BatchDatastoreWrite", "ok", "", 2);
    }

    /**
     * Tests {@link DatastoreWriterFn} with entities of more than one batches, but not a multiple.
     */
    @Test
    public void testDatastoreWriterFnWithMultipleBatches() throws Exception {
      datastoreWriterFnTest(DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START * 3 + 100);
      verifyMetricWasSet("BatchDatastoreWrite", "ok", "", 5);
    }

    /**
     * Tests {@link DatastoreWriterFn} with entities of several batches, using an exact multiple of
     * write batch size.
     */
    @Test
    public void testDatastoreWriterFnWithBatchesExactMultiple() throws Exception {
      datastoreWriterFnTest(DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START * 2);
      verifyMetricWasSet("BatchDatastoreWrite", "ok", "", 2);
    }

    // A helper method to test DatastoreWriterFn for various batch sizes.
    private void datastoreWriterFnTest(int numMutations) throws Exception {
      // Create the requested number of mutations.
      List<Mutation> mutations = new ArrayList<>(numMutations);
      for (int i = 0; i < numMutations; ++i) {
        mutations.add(
            makeUpsert(Entity.newBuilder().setKey(makeKey("key" + i, i + 1)).build()).build());
      }

      DatastoreWriterFn datastoreWriter =
          new DatastoreWriterFn(
              StaticValueProvider.of(PROJECT_ID),
              null,
              mockDatastoreFactory,
              new FakeWriteBatcher());
      DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      doFnTester.processBundle(mutations);

      int start = 0;
      while (start < numMutations) {
        int end = Math.min(numMutations, start + DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START);
        CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
        commitRequest.setProjectId(PROJECT_ID);
        commitRequest.setDatabaseId(DATABASE_ID);
        commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
        commitRequest.addAllMutations(mutations.subList(start, end));
        // Verify all the batch requests were made with the expected mutations.
        verify(mockDatastore, times(1)).commit(commitRequest.build());
        start = end;
      }
    }

    /**
     * Tests {@link DatastoreWriterFn} with large entities that need to be split into more batches.
     */
    @Test
    public void testDatastoreWriterFnWithLargeEntities() throws Exception {
      List<Mutation> mutations = new ArrayList<>();
      int entitySize = 0;
      for (int i = 0; i < 12; ++i) {
        Entity entity =
            Entity.newBuilder()
                .setKey(makeKey("key" + i, i + 1))
                .putProperties(
                    "long",
                    makeValue(new String(new char[900_000])).setExcludeFromIndexes(true).build())
                .build();
        entitySize = entity.getSerializedSize(); // Take the size of any one entity.
        mutations.add(makeUpsert(entity).build());
      }

      DatastoreWriterFn datastoreWriter =
          new DatastoreWriterFn(
              StaticValueProvider.of(PROJECT_ID),
              null,
              mockDatastoreFactory,
              new FakeWriteBatcher());
      DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      doFnTester.processBundle(mutations);

      // This test is over-specific currently; it requires that we split the 12 entity writes into 3
      // requests, but we only need each CommitRequest to be less than 10MB in size.
      int entitiesPerRpc = DATASTORE_BATCH_UPDATE_BYTES_LIMIT / entitySize;
      int start = 0;
      while (start < mutations.size()) {
        int end = Math.min(mutations.size(), start + entitiesPerRpc);
        CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
        commitRequest.setProjectId(PROJECT_ID);
        commitRequest.setDatabaseId(DATABASE_ID);
        commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
        commitRequest.addAllMutations(mutations.subList(start, end));
        // Verify all the batch requests were made with the expected mutations.
        verify(mockDatastore).commit(commitRequest.build());
        start = end;
      }
    }

    /** Tests {@link DatastoreWriterFn} correctly flushes batch upon receive same entity keys. */
    @Test
    public void testDatastoreWriterFnWithDuplicateEntities() throws Exception {
      List<Mutation> mutations = new ArrayList<>();
      for (int i : Arrays.asList(0, 1, 0, 2)) {
        // this will generate entities having key 0, 1, 0, 2 and random values
        mutations.add(
            makeUpsert(
                    Entity.newBuilder()
                        .setKey(makeKey("key" + i))
                        .putProperties("value", makeValue(UUID.randomUUID().toString()).build())
                        .build())
                .build());
      }

      DatastoreWriterFn datastoreWriter =
          new DatastoreWriterFn(
              StaticValueProvider.of(PROJECT_ID),
              null,
              mockDatastoreFactory,
              new FakeWriteBatcher());
      DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      doFnTester.processBundle(mutations);

      // first invocation has key [0, 1]
      CommitRequest.Builder commitRequest = CommitRequest.newBuilder();
      commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
      commitRequest.addAllMutations(mutations.subList(0, 2));
      commitRequest.setProjectId(PROJECT_ID);
      commitRequest.setDatabaseId(DATABASE_ID);
      verify(mockDatastore, times(1)).commit(commitRequest.build());

      // second invocation has key [0, 2] because the second 0 triggered a flush batch
      commitRequest = CommitRequest.newBuilder();
      commitRequest.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
      commitRequest.addAllMutations(mutations.subList(2, 4));
      commitRequest.setProjectId(PROJECT_ID);
      commitRequest.setDatabaseId(DATABASE_ID);
      verify(mockDatastore, times(1)).commit(commitRequest.build());
      verifyMetricWasSet("BatchDatastoreWrite", "ok", "", 2);
    }

    /** Tests {@link DatastoreWriterFn} with a failed request which is retried. */
    @Test
    public void testDatastoreWriterFnRetriesErrors() throws Exception {
      List<Mutation> mutations = new ArrayList<>();
      int numRpcs = 2;
      for (int i = 0; i < DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START * numRpcs; ++i) {
        mutations.add(
            makeUpsert(Entity.newBuilder().setKey(makeKey("key" + i, i + 1)).build()).build());
      }

      CommitResponse successfulCommit = CommitResponse.getDefaultInstance();
      when(mockDatastore.commit(any(CommitRequest.class)))
          .thenReturn(successfulCommit)
          .thenThrow(new DatastoreException("commit", Code.DEADLINE_EXCEEDED, "", null))
          .thenReturn(successfulCommit);

      DatastoreWriterFn datastoreWriter =
          new DatastoreWriterFn(
              StaticValueProvider.of(PROJECT_ID),
              null,
              mockDatastoreFactory,
              new FakeWriteBatcher());
      DoFnTester<Mutation, Void> doFnTester = DoFnTester.of(datastoreWriter);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      doFnTester.processBundle(mutations);
      verifyMetricWasSet("BatchDatastoreWrite", "ok", "", 2);
      verifyMetricWasSet("BatchDatastoreWrite", "unknown", "", 1);
    }

    /** Test options. * */
    public interface RuntimeTestOptions extends PipelineOptions {
      ValueProvider<String> getDatastoreProject();

      void setDatastoreProject(ValueProvider<String> value);

      ValueProvider<String> getGqlQuery();

      void setGqlQuery(ValueProvider<String> value);

      ValueProvider<String> getNamespace();

      void setNamespace(ValueProvider<String> value);
    }

    /**
     * Test to ensure that {@link ValueProvider} values are not accessed at pipeline construction
     * time when built with {@link DatastoreV1.Read#withQuery(Query)}.
     */
    @Test
    public void testRuntimeOptionsNotCalledInApplyQuery() {
      RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
      Pipeline pipeline = TestPipeline.create(options);
      pipeline
          .apply(
              DatastoreIO.v1()
                  .read()
                  .withProjectId(options.getDatastoreProject())
                  .withQuery(QUERY)
                  .withNamespace(options.getNamespace()))
          .apply(DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));
    }

    /**
     * Test to ensure that {@link ValueProvider} values are not accessed at pipeline construction
     * time when built with {@link DatastoreV1.Read#withLiteralGqlQuery(String)}.
     */
    @Test
    public void testRuntimeOptionsNotCalledInApplyGqlQuery() {
      RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);
      Pipeline pipeline = TestPipeline.create(options);
      pipeline
          .apply(
              DatastoreIO.v1()
                  .read()
                  .withProjectId(options.getDatastoreProject())
                  .withLiteralGqlQuery(options.getGqlQuery()))
          .apply(DatastoreIO.v1().write().withProjectId(options.getDatastoreProject()));
    }

    @Test
    public void testWriteBatcherWithoutData() {
      DatastoreV1.WriteBatcher writeBatcher = new DatastoreV1.WriteBatcherImpl();
      writeBatcher.start();
      assertEquals(
          DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherFastQueries() {
      DatastoreV1.WriteBatcher writeBatcher = new DatastoreV1.WriteBatcherImpl();
      writeBatcher.start();
      writeBatcher.addRequestLatency(0, 1000, 200);
      writeBatcher.addRequestLatency(0, 1000, 200);
      assertEquals(
          DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_LIMIT, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSlowQueries() {
      DatastoreV1.WriteBatcher writeBatcher = new DatastoreV1.WriteBatcherImpl();
      writeBatcher.start();
      writeBatcher.addRequestLatency(0, 10000, 200);
      writeBatcher.addRequestLatency(0, 10000, 200);
      assertEquals(120, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSizeNotBelowMinimum() {
      DatastoreV1.WriteBatcher writeBatcher = new DatastoreV1.WriteBatcherImpl();
      writeBatcher.start();
      writeBatcher.addRequestLatency(0, 75000, 50);
      writeBatcher.addRequestLatency(0, 75000, 50);
      assertEquals(DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_MIN, writeBatcher.nextBatchSize(0));
    }

    @Test
    public void testWriteBatcherSlidingWindow() {
      DatastoreV1.WriteBatcher writeBatcher = new DatastoreV1.WriteBatcherImpl();
      writeBatcher.start();
      writeBatcher.addRequestLatency(0, 30000, 50);
      writeBatcher.addRequestLatency(50000, 8000, 200);
      writeBatcher.addRequestLatency(100000, 8000, 200);
      assertEquals(150, writeBatcher.nextBatchSize(150000));
    }
  }

  @RunWith(Parameterized.class)
  public static class ParameterizedTests extends DatastoreV1Test {
    @Parameter(0)
    public Instant readTime;

    @Parameter(1)
    public Timestamp readTimeProto;

    @Parameters(name = "readTime = {0}, readTimeProto = {1}")
    public static Collection<Object[]> data() {
      return Arrays.asList(new Object[] {null, null}, new Object[] {TIMESTAMP, TIMESTAMP_PROTO});
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

      RunQueryRequest latestTimestampRequest =
          makeRequest(
              PROJECT_ID, DATABASE_ID, makeLatestTimestampQuery(NAMESPACE), NAMESPACE, readTime);
      RunQueryResponse latestTimestampResponse = makeLatestTimestampResponse(timestamp);
      // Per Kind statistics request and response
      RunQueryRequest statRequest =
          makeRequest(
              PROJECT_ID,
              DATABASE_ID,
              makeStatKindQuery(NAMESPACE, timestamp),
              NAMESPACE,
              readTime);
      RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

      when(mockDatastore.runQuery(latestTimestampRequest)).thenReturn(latestTimestampResponse);
      when(mockDatastore.runQuery(statRequest)).thenReturn(statResponse);

      assertEquals(
          entityBytes,
          getEstimatedSizeBytes(
              mockDatastore, PROJECT_ID, DATABASE_ID, QUERY, NAMESPACE, readTime));
      verify(mockDatastore, times(1)).runQuery(latestTimestampRequest);
      verify(mockDatastore, times(1)).runQuery(statRequest);
    }

    /** Tests {@link SplitQueryFn} when number of query splits is specified. */
    @Test
    public void testSplitQueryFnWithNumSplits() throws Exception {
      int numSplits = 100;

      when(mockQuerySplitter.getSplits(
              eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class)))
          .thenReturn(splitQuery(QUERY, numSplits));
      when(mockQuerySplitter.getSplits(
              eq(QUERY),
              any(PartitionId.class),
              eq(numSplits),
              any(Datastore.class),
              eq(readTimeProto)))
          .thenReturn(splitQuery(QUERY, numSplits));

      SplitQueryFn splitQueryFn =
          new SplitQueryFn(V_1_OPTIONS, numSplits, readTime, mockDatastoreFactory);
      DoFnTester<Query, Query> doFnTester = DoFnTester.of(splitQueryFn);
      /**
       * Although Datastore client is marked transient in {@link SplitQueryFn}, when injected
       * through mock factory using a when clause for unit testing purposes, it is not serializable
       * because it doesn't have a no-arg constructor. Thus disabling the cloning to prevent the
       * doFn from being serialized.
       */
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      List<Query> queries = doFnTester.processBundle(QUERY);

      assertEquals(queries.size(), numSplits);

      // Confirms that sub-queries are not equal to original when there is more than one split.
      for (Query subQuery : queries) {
        assertNotEquals(subQuery, QUERY);
      }

      if (readTime == null) {
        verify(mockQuerySplitter, times(1))
            .getSplits(eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class));
      } else {
        verify(mockQuerySplitter, times(1))
            .getSplits(
                eq(QUERY),
                any(PartitionId.class),
                eq(numSplits),
                any(Datastore.class),
                eq(readTimeProto));
      }
      verifyZeroInteractions(mockDatastore);
    }

    /** Tests {@link SplitQueryFn} when no query splits is specified. */
    @Test
    public void testSplitQueryFnWithoutNumSplits() throws Exception {
      // Force SplitQueryFn to compute the number of query splits
      int numSplits = 0;
      int expectedNumSplits = 20;
      long entityBytes = expectedNumSplits * DEFAULT_BUNDLE_SIZE_BYTES;
      // In seconds
      long timestamp = 1234L;

      RunQueryRequest latestTimestampRequest =
          makeRequest(
              PROJECT_ID, DATABASE_ID, makeLatestTimestampQuery(NAMESPACE), NAMESPACE, readTime);
      RunQueryResponse latestTimestampResponse = makeLatestTimestampResponse(timestamp);

      // Per Kind statistics request and response
      RunQueryRequest statRequest =
          makeRequest(
              PROJECT_ID,
              DATABASE_ID,
              makeStatKindQuery(NAMESPACE, timestamp),
              NAMESPACE,
              readTime);
      RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

      when(mockDatastore.runQuery(latestTimestampRequest)).thenReturn(latestTimestampResponse);
      when(mockDatastore.runQuery(statRequest)).thenReturn(statResponse);
      when(mockQuerySplitter.getSplits(
              eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class)))
          .thenReturn(splitQuery(QUERY, expectedNumSplits));
      when(mockQuerySplitter.getSplits(
              eq(QUERY),
              any(PartitionId.class),
              eq(expectedNumSplits),
              any(Datastore.class),
              eq(readTimeProto)))
          .thenReturn(splitQuery(QUERY, expectedNumSplits));

      SplitQueryFn splitQueryFn =
          new SplitQueryFn(V_1_OPTIONS, numSplits, readTime, mockDatastoreFactory);
      DoFnTester<Query, Query> doFnTester = DoFnTester.of(splitQueryFn);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      List<Query> queries = doFnTester.processBundle(QUERY);

      assertEquals(expectedNumSplits, queries.size());
      if (readTime == null) {
        verify(mockQuerySplitter, times(1))
            .getSplits(
                eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class));
      } else {
        verify(mockQuerySplitter, times(1))
            .getSplits(
                eq(QUERY),
                any(PartitionId.class),
                eq(expectedNumSplits),
                any(Datastore.class),
                eq(readTimeProto));
      }
      verify(mockDatastore, times(1)).runQuery(latestTimestampRequest);
      verify(mockDatastore, times(1)).runQuery(statRequest);
    }

    /** Tests {@link DatastoreV1.Read.SplitQueryFn} when the query has a user specified limit. */
    @Test
    public void testSplitQueryFnWithQueryLimit() throws Exception {
      Query queryWithLimit =
          QUERY.toBuilder().setLimit(Int32Value.newBuilder().setValue(1)).build();

      SplitQueryFn splitQueryFn = new SplitQueryFn(V_1_OPTIONS, 10, readTime, mockDatastoreFactory);
      DoFnTester<Query, Query> doFnTester = DoFnTester.of(splitQueryFn);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      List<Query> queries = doFnTester.processBundle(queryWithLimit);

      assertEquals(1, queries.size());
      verifyNoMoreInteractions(mockDatastore);
      verifyNoMoreInteractions(mockQuerySplitter);
    }

    /** Tests {@link ReadFn} with a query limit less than one batch. */
    @Test
    public void testReadFnWithOneBatch() throws Exception {
      readFnTest(5, readTime);
      verifyMetricWasSet("BatchDatastoreRead", "ok", NAMESPACE, 1);
    }

    /** Tests {@link ReadFn} with a query limit more than one batch, and not a multiple. */
    @Test
    public void testReadFnWithMultipleBatches() throws Exception {
      readFnTest(QUERY_BATCH_LIMIT + 5, readTime);
      verifyMetricWasSet("BatchDatastoreRead", "ok", NAMESPACE, 2);
    }

    /** Tests {@link ReadFn} for several batches, using an exact multiple of batch size results. */
    @Test
    public void testReadFnWithBatchesExactMultiple() throws Exception {
      readFnTest(5 * QUERY_BATCH_LIMIT, readTime);
      verifyMetricWasSet("BatchDatastoreRead", "ok", NAMESPACE, 5);
    }

    /** Tests that {@link ReadFn} retries after an error. */
    @Test
    public void testReadFnRetriesErrors() throws Exception {
      // An empty query to read entities.
      Query query = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(1)).build();

      // Use mockResponseForQuery to generate results.
      when(mockDatastore.runQuery(any(RunQueryRequest.class)))
          .thenThrow(new DatastoreException("RunQuery", Code.DEADLINE_EXCEEDED, "", null))
          .thenAnswer(
              invocationOnMock -> {
                Query q = ((RunQueryRequest) invocationOnMock.getArguments()[0]).getQuery();
                return mockResponseForQuery(q);
              });

      ReadFn readFn = new ReadFn(V_1_OPTIONS, readTime, mockDatastoreFactory);
      DoFnTester<Query, Entity> doFnTester = DoFnTester.of(readFn);
      doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
      doFnTester.processBundle(query);
      verifyMetricWasSet("BatchDatastoreRead", "ok", NAMESPACE, 1);
      verifyMetricWasSet("BatchDatastoreRead", "unknown", NAMESPACE, 1);
    }

    @Test
    public void testTranslateGqlQueryWithLimit() throws Exception {
      String gql = "SELECT * from DummyKind LIMIT 10";
      String gqlWithZeroLimit = gql + " LIMIT 0";
      GqlQuery gqlQuery = GqlQuery.newBuilder().setQueryString(gql).setAllowLiterals(true).build();
      GqlQuery gqlQueryWithZeroLimit =
          GqlQuery.newBuilder().setQueryString(gqlWithZeroLimit).setAllowLiterals(true).build();

      RunQueryRequest gqlRequest =
          makeRequest(PROJECT_ID, DATABASE_ID, gqlQuery, V_1_OPTIONS.getNamespace(), readTime);
      RunQueryRequest gqlRequestWithZeroLimit =
          makeRequest(
              PROJECT_ID, DATABASE_ID, gqlQueryWithZeroLimit, V_1_OPTIONS.getNamespace(), readTime);
      when(mockDatastore.runQuery(gqlRequestWithZeroLimit))
          .thenThrow(
              new DatastoreException(
                  "runQuery",
                  Code.INVALID_ARGUMENT,
                  "invalid query",
                  // dummy
                  new RuntimeException()));
      when(mockDatastore.runQuery(gqlRequest))
          .thenReturn(RunQueryResponse.newBuilder().setQuery(QUERY).build());
      assertEquals(
          translateGqlQueryWithLimitCheck(
              gql, mockDatastore, PROJECT_ID, DATABASE_ID, V_1_OPTIONS.getNamespace(), readTime),
          QUERY);
      verify(mockDatastore, times(1)).runQuery(gqlRequest);
      verify(mockDatastore, times(1)).runQuery(gqlRequestWithZeroLimit);
    }

    @Test
    public void testTranslateGqlQueryWithNoLimit() throws Exception {
      String gql = "SELECT * from DummyKind";
      String gqlWithZeroLimit = gql + " LIMIT 0";
      GqlQuery gqlQueryWithZeroLimit =
          GqlQuery.newBuilder().setQueryString(gqlWithZeroLimit).setAllowLiterals(true).build();

      RunQueryRequest gqlRequestWithZeroLimit =
          makeRequest(
              PROJECT_ID, DATABASE_ID, gqlQueryWithZeroLimit, V_1_OPTIONS.getNamespace(), readTime);
      when(mockDatastore.runQuery(gqlRequestWithZeroLimit))
          .thenReturn(RunQueryResponse.newBuilder().setQuery(QUERY).build());
      assertEquals(
          translateGqlQueryWithLimitCheck(
              gql,
              mockDatastore,
              V_1_OPTIONS.getProjectId(),
              V_1_OPTIONS.getDatabaseId(),
              V_1_OPTIONS.getNamespace(),
              readTime),
          QUERY);
      verify(mockDatastore, times(1)).runQuery(gqlRequestWithZeroLimit);
    }

    @Test
    public void testTranslateGqlQueryWithException() throws Exception {
      String gql = "SELECT * from DummyKind";
      String gqlWithZeroLimit = gql + " LIMIT 0";
      GqlQuery gqlQueryWithZeroLimit =
          GqlQuery.newBuilder().setQueryString(gqlWithZeroLimit).setAllowLiterals(true).build();
      RunQueryRequest gqlRequestWithZeroLimit =
          makeRequest(
              PROJECT_ID, DATABASE_ID, gqlQueryWithZeroLimit, V_1_OPTIONS.getNamespace(), readTime);
      when(mockDatastore.runQuery(gqlRequestWithZeroLimit))
          .thenThrow(new RuntimeException("TestException"));

      thrown.expect(RuntimeException.class);
      thrown.expectMessage("TestException");
      translateGqlQueryWithLimitCheck(
          gql,
          mockDatastore,
          V_1_OPTIONS.getProjectId(),
          V_1_OPTIONS.getDatabaseId(),
          V_1_OPTIONS.getNamespace(),
          readTime);
    }
  }

  /** Helper Methods */

  /**
   * A helper function that creates mock {@link Entity} results in response to a query. Always
   * indicates that more results are available, unless the batch is limited to fewer than {@link
   * DatastoreV1.Read#QUERY_BATCH_LIMIT} results.
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
  protected void readFnTest(int numEntities, Instant readTime) throws Exception {
    // An empty query to read entities.
    Query query =
        Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(numEntities)).build();

    // Use mockResponseForQuery to generate results.
    when(mockDatastore.runQuery(any(RunQueryRequest.class)))
        .thenAnswer(
            invocationOnMock -> {
              Query q = ((RunQueryRequest) invocationOnMock.getArguments()[0]).getQuery();
              return mockResponseForQuery(q);
            });

    ReadFn readFn = new ReadFn(V_1_OPTIONS, readTime, mockDatastoreFactory);
    DoFnTester<Query, Entity> doFnTester = DoFnTester.of(readFn);
    /**
     * Although Datastore client is marked transient in {@link ReadFn}, when injected through mock
     * factory using a when clause for unit testing purposes, it is not serializable because it
     * doesn't have a no-arg constructor. Thus disabling the cloning to prevent the test object from
     * being serialized.
     */
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<Entity> entities = doFnTester.processBundle(query);

    ArgumentCaptor<RunQueryRequest> requestCaptor = ArgumentCaptor.forClass(RunQueryRequest.class);

    int expectedNumCallsToRunQuery = (int) Math.ceil((double) numEntities / QUERY_BATCH_LIMIT);
    verify(mockDatastore, times(expectedNumCallsToRunQuery)).runQuery(requestCaptor.capture());
    // Validate the number of results.
    assertEquals(numEntities, entities.size());
    // Validate read Time.
    RunQueryRequest request = requestCaptor.getValue();
    if (readTime != null) {
      assertEquals(
          readTime.getMillis(), Timestamps.toMillis(request.getReadOptions().getReadTime()));
    } else {
      assertFalse(request.hasReadOptions());
    }
  }

  /** Builds a per-kind statistics response with the given entity size. */
  private static RunQueryResponse makeStatKindResponse(long entitySizeInBytes) {
    RunQueryResponse.Builder statKindResponse = RunQueryResponse.newBuilder();
    Entity.Builder entity = Entity.newBuilder();
    entity.setKey(makeKey("dummyKind", "dummyId"));
    entity.putProperties("entity_bytes", makeValue(entitySizeInBytes).build());
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
    entity.putProperties("timestamp", makeValue(new Date(timestamp * 1000)).build());
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
    statQuery.setFilter(
        makeAndFilter(
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
  private static List<Query> splitQuery(Query query, int numSplits) {
    List<Query> queries = new ArrayList<>();
    int offsetOfOriginal = query.getOffset();
    for (int i = 0; i < numSplits; i++) {
      Query.Builder q = Query.newBuilder();
      q.addKindBuilder().setName(KIND);
      // Making sub-queries unique (and not equal to the original query) by setting different
      // offsets.
      q.setOffset(++offsetOfOriginal);
      queries.add(q.build());
    }
    return queries;
  }

  /**
   * A WriteBatcher for unit tests, which does no timing-based adjustments (so unit tests have
   * consistent results).
   */
  static class FakeWriteBatcher implements DatastoreV1.WriteBatcher {
    @Override
    public void start() {}

    @Override
    public void addRequestLatency(
        long timeSinceEpochMillis, long latencyMillis, int numMutations) {}

    @Override
    public int nextBatchSize(long timeSinceEpochMillis) {
      return DatastoreV1.DATASTORE_BATCH_UPDATE_ENTITIES_START;
    }
  }

  private static void verifyMetricWasSet(
      String method, String status, String namespace, long count) {
    // Verify the metric as reported.
    HashMap<String, String> labels = new HashMap<>();
    labels.put(MonitoringInfoConstants.Labels.PTRANSFORM, "");
    labels.put(MonitoringInfoConstants.Labels.SERVICE, "Datastore");
    labels.put(MonitoringInfoConstants.Labels.METHOD, method);
    labels.put(
        MonitoringInfoConstants.Labels.RESOURCE,
        "//bigtable.googleapis.com/projects/" + PROJECT_ID + "/namespaces/" + namespace);
    labels.put(MonitoringInfoConstants.Labels.DATASTORE_PROJECT, PROJECT_ID);
    labels.put(MonitoringInfoConstants.Labels.DATASTORE_NAMESPACE, namespace);
    labels.put(MonitoringInfoConstants.Labels.STATUS, status);

    MonitoringInfoMetricName name =
        MonitoringInfoMetricName.named(MonitoringInfoConstants.Urns.API_REQUEST_COUNT, labels);
    MetricsContainerImpl container =
        (MetricsContainerImpl) MetricsEnvironment.getProcessWideContainer();
    assertEquals(count, (long) container.getCounter(name).getCumulative());
  }
}

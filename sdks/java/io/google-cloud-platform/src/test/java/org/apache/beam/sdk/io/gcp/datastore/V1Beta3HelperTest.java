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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.QUERY_BATCH_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.getEstimatedSizeBytes;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.makeRequest;
import static com.google.datastore.v1beta3.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1beta3.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.ReadFn;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.SplitQueryFn;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.V1Beta3DatastoreFactory;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.V1Beta3Options;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.transforms.DoFnTester.CloningBehavior;
import org.apache.beam.sdk.values.KV;

import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.PartitionId;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.datastore.v1beta3.client.QuerySplitter;
import com.google.protobuf.Int32Value;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Tests for {@link V1Beta3Helper}.
 */
@RunWith(JUnit4.class)
public class V1Beta3HelperTest {
  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  private static final V1Beta3Options v1Beta3Options;
  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
    v1Beta3Options = V1Beta3Options.from(PROJECT_ID, QUERY, NAMESPACE);
  }

  @Mock
  Datastore mockDatastore;
  @Mock
  QuerySplitter mockQuerySplitter;
  @Mock
  V1Beta3DatastoreFactory mockDatastoreFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockDatastoreFactory.getDatastore(any(PipelineOptions.class), any(String.class)))
        .thenReturn(mockDatastore);
    when(mockDatastoreFactory.getQuerySplitter())
        .thenReturn(mockQuerySplitter);
  }

  /**
   * Tests {@link V1Beta3Helper#getEstimatedSizeBytes} to fetch and return estimated size for a
   * query.
   */
  @Test
  public void testEstimatedSizeBytes() throws Exception {
    long entityBytes = 100L;
    // Per Kind statistics request and response
    RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE), NAMESPACE);
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);

    assertEquals(entityBytes, getEstimatedSizeBytes(mockDatastore, QUERY, NAMESPACE));
    verify(mockDatastore, times(1)).runQuery(statRequest);
  }

  /**
   * Tests {@link V1Beta3Helper.SplitQueryFn} when number of query splits is specified.
   */
  @Test
  public void testSplitQueryFnWithNumSplits() throws Exception {
    int numSplits = 100;
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, numSplits));

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, numSplits, mockDatastoreFactory);
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
   * Tests {@link V1Beta3Helper.SplitQueryFn} when no query splits is specified.
   */
  @Test
  public void testSplitQueryFnWithoutNumSplits() throws Exception {
    // Force SplitQueryFn to compute the number of query splits
    int numSplits = 0;
    // 1280 MB
    long entityBytes = 1280 * 1024 * 1024L;
    int expectedNumSplits = 20;

    // Per Kind statistics request and response
    RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE), NAMESPACE);
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, expectedNumSplits));

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, numSplits, mockDatastoreFactory);
    DoFnTester<Query, KV<Integer, Query>> doFnTester = DoFnTester.of(splitQueryFn);
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<KV<Integer, Query>> queries = doFnTester.processBundle(QUERY);

    assertEquals(queries.size(), expectedNumSplits);
    verifyUniqueKeys(queries);
    verify(mockQuerySplitter, times(1)).getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class));
    verify(mockDatastore, times(1)).runQuery(statRequest);
  }

  /**
   * Tests {@link V1Beta3Helper.SplitQueryFn} when the query has a user specified limit.
   */
  @Test
  public void testSplitQueryFnWithQueryLimit() throws Exception {
    Query queryWithLimit = QUERY.toBuilder().clone()
        .setLimit(Int32Value.newBuilder().setValue(1))
        .build();

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, 10, mockDatastoreFactory);
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
   * {@link V1Beta3Helper#QUERY_BATCH_LIMIT} results.
   */
  private static RunQueryResponse mockResponseForQuery(Query q) {
    // Every query V1Beta3 sends should have a limit.
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

    ReadFn readFn = new ReadFn(v1Beta3Options, mockDatastoreFactory);
    DoFnTester<Query, Entity> doFnTester = DoFnTester.of(readFn);
    /**
     * Although Datastore client is marked transient in {@link ReadFn}, when injected through
     * mock factory using a when clause for unit testing purposes, it is not serializable
     * because it doesn't have a no-arg constructor. Thus disabling the cloning to prevent the
     * test object from being serialized.
     */
    doFnTester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);
    List<Entity> entities = doFnTester.processBundle(query);

    // Validate the number of results.
    assertEquals(numEntities, entities.size());
  }

  /** Builds a per-kind statistics response with the given entity size. */
  private static RunQueryResponse makeStatKindResponse(long entitySizeInBytes) {
    RunQueryResponse.Builder timestampResponse = RunQueryResponse.newBuilder();
    Entity.Builder entity = Entity.newBuilder();
    entity.setKey(makeKey("dummyKind", "dummyId"));
    entity.getMutableProperties().put("entity_bytes", makeValue(entitySizeInBytes).build());
    EntityResult.Builder entityResult = EntityResult.newBuilder();
    entityResult.setEntity(entity);
    QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
    batch.addEntityResults(entityResult);
    timestampResponse.setBatch(batch);
    return timestampResponse.build();
  }

  /** Builds a per-kind statistics query for the given timestamp and namespace. */
  private static Query makeStatKindQuery(String namespace) {
    Query.Builder statQuery = Query.newBuilder();
    if (namespace == null) {
      statQuery.addKindBuilder().setName("__Stat_Kind__");
    } else {
      statQuery.addKindBuilder().setName("__Ns_Stat_Kind__");
    }
    statQuery.setFilter(makeFilter("kind_name", EQUAL, makeValue(KIND)).build());
    statQuery.addOrder(makeOrder("timestamp", DESCENDING));
    statQuery.setLimit(Int32Value.newBuilder().setValue(1));
    return statQuery.build();
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

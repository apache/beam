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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Source.MAX_QUERY_SPLITS;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Source.QUERY_SPLIT_FACTOR;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static com.google.datastore.v1beta3.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1beta3.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeAndFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.TestCredential;

import com.google.common.collect.ImmutableList;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.PartitionId;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.datastore.v1beta3.client.DatastoreFactory;
import com.google.datastore.v1beta3.client.DatastoreOptions;
import com.google.datastore.v1beta3.client.QuerySplitter;
import com.google.protobuf.Int32Value;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for {@link V1Beta3Source}.
 */
@RunWith(JUnit4.class)
public class V1Beta3SourceTest {
  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  private static final Query.Builder QUERY_BUILDER;

  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY_BUILDER = q;
    QUERY = q.build();
  }

  @Mock
  DatastoreFactory mockDatastoreFactory;

  @Mock
  QuerySplitter mockQuerySplitter;

  @Mock
  Datastore mockDatastore;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Helper function to create a test {@code DataflowPipelineOptions}.
   */
  static final GcpOptions testPipelineOptions() {
    GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
    options.setGcpCredential(new TestCredential());
    return options;
  }

  @Test
  public void testValidationFailsProject() {
    V1Beta3Source source = new V1Beta3Source(null, QUERY, NAMESPACE);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("project");
    source.validate();
  }

  @Test
  public void testValidationFailsQuery() {
    thrown.expect(NullPointerException.class);
    V1Beta3Source source = new V1Beta3Source(PROJECT_ID, null, NAMESPACE);
    source.validate();
  }

  @Test
  public void testDisplayData() {
    V1Beta3Source source = new V1Beta3Source(PROJECT_ID, QUERY, NAMESPACE);

    DisplayData displayData = DisplayData.from(source);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
    assertThat(displayData, hasDisplayItem("query", QUERY.toString()));
    assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
  }

  /**
   * Verifies that {@link V1Beta3Source#getEstimatedSizeBytes} correctly fetches and returns the
   * size of entities from datastore.
   */
  @Test
  public void testEstimatedSizeBytes() throws Exception {
    V1Beta3Source source = makeSource(QUERY);
    // Latest timestamp request and response
    RunQueryRequest timestampRequest = source.makeRequest(makeLatestTimestampQuery());
    RunQueryResponse timestampResponse = makeLatestTimestampQueryResponse();
    long latestTimestamp = getTimestampFromQueryResponse(timestampResponse);
    long entityBytes = 100L;
    // Per Kind statistics request and response
    RunQueryRequest statRequest = source.makeRequest(makeStatKindQuery(latestTimestamp, NAMESPACE));
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastoreFactory.create(any(DatastoreOptions.class)))
        .thenReturn(mockDatastore);

    when(mockDatastore.runQuery(timestampRequest))
        .thenReturn(timestampResponse);

    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);

    assertEquals(entityBytes, source.getEstimatedSizeBytes(testPipelineOptions()));
  }

  /**
   * Verifies that {@link V1Beta3Source#splitIntoBundles} doesn't split if the query has a limit.
   * @throws Exception
   */
  @Test
  public void testSplitIntoBundlesWithQueryLimit() throws Exception {
    Int32Value queryLimit = Int32Value.newBuilder().setValue(100).build();
    Query queryWithLimit = QUERY_BUILDER.setLimit(queryLimit).build();
    V1Beta3Source source = makeSource(queryWithLimit);

    List<V1Beta3Source> subSources = source.splitIntoBundles(1L, testPipelineOptions());
    assertEquals(1, subSources.size());
    assertEquals(ImmutableList.of(queryWithLimit), subSources.get(0).getQueries());
    verifyZeroInteractions(mockQuerySplitter);
    verifyZeroInteractions(mockDatastore);
  }

  /**
   * Verifies that {@link V1Beta3Source#splitIntoBundles} doesn't split further
   * if it is a sub-source.
   */
  @Test
  public void testSplitIntoBundlesForSubSource() throws Exception {
    V1Beta3Source source = makeSubSource(QUERY);

    List<V1Beta3Source> subSources = source.splitIntoBundles(1L, testPipelineOptions());
    assertEquals(1, subSources.size());
    assertEquals(ImmutableList.of(QUERY), subSources.get(0).getQueries());
    verifyZeroInteractions(mockQuerySplitter);
    verifyZeroInteractions(mockDatastore);
  }

  /**
   * Verifies that {@link V1Beta3Source#splitIntoBundles} returns the expected sub-sources.
   */
  @Test
  public void testSplitIntoBundlesReturnsSubSources() throws Exception {
    long estimatedSizeBytes = 1000L;
    long desiredSizeBytes = 100L;
    int expectedNumSubSources = (int) Math.round((double) estimatedSizeBytes / desiredSizeBytes);
    int expectQueriesPerSubSource = QUERY_SPLIT_FACTOR;

    testSplitIntoBundles(estimatedSizeBytes, desiredSizeBytes, expectedNumSubSources,
        expectQueriesPerSubSource);
  }

  /**
   * Verifies that in {@link V1Beta3Source#splitIntoBundles} the number of splits to the
   * given query does not exceed {@link V1Beta3Source#MAX_QUERY_SPLITS}.
   */
  @Test
  public void testSplitIntoBundlesLimitsQuerySplits() throws Exception {
    long estimatedSizeBytes = 100000000L;
    long desiredSizeBytes = 100L;
    // To maintain atleast one query per sub-source, the number of sub-sources is limited
    // to the max allowed query splits
    int expectedNumSubSources = MAX_QUERY_SPLITS;
    int expectQueriesPerSubSource = 1;

    testSplitIntoBundles(estimatedSizeBytes, desiredSizeBytes, expectedNumSubSources,
        expectQueriesPerSubSource);
  }

  /**
   * Verifies that {@link V1Beta3Source#splitIntoBundles} defaults to a fixed number of
   * splits when {@link V1Beta3Source#getEstimatedSizeBytes} fails.
   */
  @Test
  public void testSplitIntoBundlesWhenEstimateSizeUnavailable() throws Exception {
    V1Beta3Source source = spy(makeSource(QUERY));
    PipelineOptions options = testPipelineOptions();
    long numSubSources = 12;
    int numQuerySplits = (int) numSubSources * QUERY_SPLIT_FACTOR;

    // throw exception
    doThrow(new NoSuchElementException()).when(source).getEstimatedSizeBytes(options);

    when(mockDatastoreFactory.create(any(DatastoreOptions.class)))
        .thenReturn(mockDatastore);

    when(mockQuerySplitter.getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, numQuerySplits));

    List<V1Beta3Source> subSources = source.splitIntoBundles(1L, options);

    verify(mockQuerySplitter, times(1)).getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class));
    assertEquals(numSubSources, subSources.size());
    for (V1Beta3Source subSource: subSources) {
      assertTrue(subSource.isSubSource());
      assertEquals(subSource.getQueries().size(), QUERY_SPLIT_FACTOR);
    }
  }

  /**
   * Verifies that {@link V1Beta3Source#splitIntoBundles} returns without splitting
   * source when query splitting fails.
   */
  @Test
  public void testSplitIntoBundlesWhenQuerySplittingFails() throws Exception {
    V1Beta3Source source = spy(makeSource(QUERY));
    PipelineOptions options = testPipelineOptions();
    long estimatedSizeBytes = 10000L;
    long desiredSizeBytes = 100L;
    int numQuerySplits = (int) (estimatedSizeBytes / desiredSizeBytes) * QUERY_SPLIT_FACTOR;
    // stub for estimatedSizeBytes
    doReturn(estimatedSizeBytes).when(source).getEstimatedSizeBytes(options);

    when(mockDatastoreFactory.create(any(DatastoreOptions.class)))
        .thenReturn(mockDatastore);

    when(mockQuerySplitter.getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class)))
        .thenThrow(new IllegalArgumentException());

    List<V1Beta3Source> subSources = source.splitIntoBundles(desiredSizeBytes, options);

    verify(mockQuerySplitter, times(1)).getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class));
    assertEquals(1, subSources.size());
    assertEquals(ImmutableList.of(QUERY), subSources.get(0).getQueries());
  }

  /**
   * A common method for some of the splitIntoBundle tests.
   *
   * It verifies that for the given {@param estimatedSizeBytes} and {@param desiredSizeBytes}
   * the sub-sources resulting from a call to {@link V1Beta3Source#splitIntoBundles}
   * matches the {@param expectedNumSubSources} and {@param expectedQueriesPerSubSource}.
   */
  private void testSplitIntoBundles(long estimatedSizeBytes, long desiredSizeBytes,
      int expectedNumSubSources, int expectedQueriesPerSubSource) throws Exception {
    V1Beta3Source source = spy(makeSource(QUERY));
    PipelineOptions options = testPipelineOptions();
    long numSubSources = Math.round((double) estimatedSizeBytes / desiredSizeBytes);
    int numQuerySplits = (int) Math.min(numSubSources * QUERY_SPLIT_FACTOR, MAX_QUERY_SPLITS);

    // stub for estimatedSizeBytes
    doReturn(estimatedSizeBytes).when(source).getEstimatedSizeBytes(options);

    when(mockDatastoreFactory.create(any(DatastoreOptions.class)))
        .thenReturn(mockDatastore);

    when(mockQuerySplitter.getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, numQuerySplits));

    List<V1Beta3Source> subSources = source.splitIntoBundles(desiredSizeBytes, options);

    verify(mockQuerySplitter, times(1)).getSplits(
        any(Query.class), any(PartitionId.class), eq(numQuerySplits), any(Datastore.class));
    assertEquals(expectedNumSubSources, subSources.size());
    for (V1Beta3Source subSource: subSources) {
      assertTrue(subSource.isSubSource());
      assertEquals(subSource.getQueries().size(), expectedQueriesPerSubSource);
    }
  }

  // Helper Methods.
  // TODO: Convert these to helper methods across the source and tests.

  // Builds a per-kind statistics response with the given entity size.
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

  // Builds a per-kind statistics query for the given timestamp and namespace.
  private static Query makeStatKindQuery(long timestamp, String namespace) {
    Query.Builder statQuery = Query.newBuilder();
    if (namespace == null) {
      statQuery.addKindBuilder().setName("__Stat_Kind__");
    } else {
      statQuery.addKindBuilder().setName("__Ns_Stat_Kind__");
    }
    statQuery.setFilter(makeAndFilter(
        makeFilter("kind_name", EQUAL, makeValue(KIND)).build(),
        makeFilter("timestamp", EQUAL, makeValue(timestamp)).build()));

    return statQuery.build();
  }

  // Returns the timestamp from a given response.
  private static long getTimestampFromQueryResponse(RunQueryResponse response) {
    Entity entity = response.getBatch().getEntityResults(0).getEntity();
    return entity.getProperties().get("timestamp").getTimestampValue().getNanos();
  }

  // Builds a query that computes the latest timestamp of datastore statistics.
  private static Query makeLatestTimestampQuery() {
    Query.Builder timestampQuery = Query.newBuilder();
    timestampQuery.addKindBuilder().setName("__Stat_Total__");
    timestampQuery.addOrder(makeOrder("timestamp", DESCENDING));
    timestampQuery.setLimit(Int32Value.newBuilder().setValue(1));
    return timestampQuery.build();
  }

  // Builds a response that contains the latest timestamp of datastore statistics.
  private static RunQueryResponse makeLatestTimestampQueryResponse() {
    RunQueryResponse.Builder timestampResponse = RunQueryResponse.newBuilder();
    Entity.Builder entity = Entity.newBuilder();
    entity.setKey(makeKey("dummyKind", "dummyId"));
    entity.getMutableProperties().put("timestamp", makeValue(new Date()).build());
    EntityResult.Builder entityResult = EntityResult.newBuilder();
    entityResult.setEntity(entity);
    QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
    batch.addEntityResults(entityResult);
    timestampResponse.setBatch(batch);
    return timestampResponse.build();
  }

  private V1Beta3Source makeSource(Query query) {
    return makeSource(query, false);
  }

  private V1Beta3Source makeSubSource(Query query) {
    return makeSource(query, true);
  }

  private V1Beta3Source makeSource(Query query, boolean isSubSource) {
    return new V1Beta3Source(PROJECT_ID, query,
        ImmutableList.of(query), NAMESPACE, isSubSource, mockDatastoreFactory,
        mockQuerySplitter);
  }

  // Dummy query splits
  private List<Query> splitQuery(Query query, int numSplits) {
    List<Query> queries = new LinkedList<>();
    for (int i = 0; i < numSplits; i++) {
      queries.add(query);
    }
    return queries;
  }
}

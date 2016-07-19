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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Source.V1Beta3Reader.QUERY_BATCH_LIMIT;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Source.V1Beta3Reader;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.protobuf.ByteString;
import com.google.protobuf.Int32Value;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests for {@link V1Beta3Source.V1Beta3Reader}.
 */
@RunWith(JUnit4.class)
public class V1Beta3ReaderTest {

  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;

  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
  }

  V1Beta3Source spiedSource;
  V1Beta3Reader reader;
  @Mock
  Datastore mockDatastore;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    spiedSource = spy(makeSource(QUERY));
    reader = makeReader(spiedSource);
  }

  /**
   * Tests the {@link V1Beta3Reader#start} when there are no entities for a query.
   */
  @Test
  public void testReaderWithZeroEntities() throws Exception {
    when(mockDatastore.runQuery(any(RunQueryRequest.class)))
        .thenReturn(makeEmptyResponse());

    assertFalse(reader.start());
    verify(mockDatastore, times(1)).runQuery(any(RunQueryRequest.class));
  }

  /**
   * Tests the {@link V1Beta3Reader#advance} when there is only entity returned for a query.
   */
  @Test
  public void testReaderWithOneEntity() throws Exception {
    testReaderWithEntities(reader, makeEntities(1));
  }

  /**
   * Tests the {@link V1Beta3Reader#advance} when there is are multiple entities returned for a
   * query but fit within a single batch.
   */
  @Test
  public void testReaderWithMultipleEntities() throws Exception {
    // Entities fit in a single batch.
    testReaderWithEntities(reader, makeEntities(400));
  }

  /**
   * Tests the {@link V1Beta3Reader#advance} when there is are multiple entities returned for a
   * query and extend across multiple batches.
   */
  @Test
  public void testReaderWithMultipleBatches() throws Exception {
    // Entities that extend to 3 batches.
    testReaderWithEntities(reader, makeEntities(1200));
  }

  /**
   * Tests the {@link V1Beta3Reader#advance} when there are multiple queries (split-points) for a
   * given source.
   */
  @Test
  public void testReaderWithMultipleQueries() throws Exception {
    List<Query> queries = makeQueries(5);
    List<Entity> entities = makeEntities(5);
    V1Beta3Reader spiedReader = spy(reader);

    doReturn(queries).when(spiedSource).getQueries();
    for (int i = 0; i < queries.size(); i++) {
      doReturn(ImmutableList.of(makeEntityResult(entities.get(i))).iterator())
          .when(spiedReader).getIteratorAndMoveCursor(queries.get(i));
    }

    assertTrue(spiedReader.start());
    for (int i = 0; i < entities.size() - 1; i++) {
      assertEquals(entities.get(i), spiedReader.getCurrent());
      assertTrue(spiedReader.advance());
    }

    assertEquals(entities.get(entities.size() - 1), spiedReader.getCurrent());
    assertFalse(spiedReader.advance());
  }

  /**
   * Tests the {@link V1Beta3Reader#advance} when there is user-limit set on the query.
   */
  @Test
  public void testReaderWithQueryLimit() throws Exception {
    int userQueryLimit = 10;
    Query queryWithLimit = QUERY.toBuilder().clone().setLimit(
        Int32Value.newBuilder().setValue(userQueryLimit).build()).build();
    List<Entity> entities = makeEntities(userQueryLimit);

    V1Beta3Source source = makeSource(queryWithLimit);
    V1Beta3Reader reader = makeReader(source);

    RunQueryRequest request = makeRequest(queryWithLimit);
    RunQueryResponse response = makeQueryResponse(entities, userQueryLimit);

    when(mockDatastore.runQuery(request))
        .thenReturn(response);

    assertTrue(reader.start());
    for (int i = 0; i < entities.size() - 1; i++) {
      assertEquals(entities.get(i), reader.getCurrent());
      assertTrue(reader.advance());
    }

    assertEquals(entities.get(entities.size() - 1), reader.getCurrent());
    assertFalse(reader.advance());
  }

  /**
   * Tests the {@link V1Beta3Reader#getSplitPointsConsumed} when there is source has multiple
   * split-points.
   */
  @Test
  public void testSplitPointsConsumed() throws Exception {
    List<Query> queries = makeQueries(5);
    List<Entity> entities = makeEntities(5);
    V1Beta3Reader spiedReader = spy(reader);

    doReturn(queries).when(spiedSource).getQueries();
    for (int i = 0; i < queries.size(); i++) {
      doReturn(ImmutableList.of(makeEntityResult(entities.get(i))).iterator())
          .when(spiedReader).getIteratorAndMoveCursor(queries.get(i));
    }

    // Before start is called
    assertEquals(0, spiedReader.getSplitPointsConsumed());
    spiedReader.start();
    for (int i = 0; i < queries.size(); i++) {
      // When the first split-point is being processed.
      assertEquals(i, spiedReader.getSplitPointsConsumed());
      spiedReader.advance();
    }

    // After all split-points have been processed.
    assertEquals(queries.size(), spiedReader.getSplitPointsConsumed());
  }

  /**
   * Tests the {@link V1Beta3Reader#getSplitPointsRemaining} when there is source has multiple
   * split-points.
   */
  @Test
  public void testSplitPointsRemaining() throws Exception {
    List<Query> queries = makeQueries(5);
    List<Entity> entities = makeEntities(5);
    V1Beta3Reader spiedReader = spy(reader);

    doReturn(queries).when(spiedSource).getQueries();
    for (int i = 0; i < queries.size(); i++) {
      doReturn(ImmutableList.of(makeEntityResult(entities.get(i))).iterator())
          .when(spiedReader).getIteratorAndMoveCursor(queries.get(i));
    }

    // Before start is called
    assertEquals(queries.size(), spiedReader.getSplitPointsRemaining());
    spiedReader.start();
    for (int i = queries.size(); i > 0; i--) {
      // When the first split-point is being processed.
      assertEquals(i, spiedReader.getSplitPointsRemaining());
      spiedReader.advance();
    }

    // After all split-points have been processed.
    assertEquals(0, spiedReader.getSplitPointsRemaining());
  }

  /**
   * Tests the {@link V1Beta3Reader#getFractionConsumed} when there is source has multiple
   * split-points.
   */
  @Test
  public void testFractionsConsumed() throws Exception {
    List<Query> queries = makeQueries(5);
    List<Entity> entities = makeEntities(5);
    V1Beta3Reader spiedReader = spy(reader);

    doReturn(queries).when(spiedSource).getQueries();
    for (int i = 0; i < queries.size(); i++) {
      doReturn(ImmutableList.of(makeEntityResult(entities.get(i))).iterator())
          .when(spiedReader).getIteratorAndMoveCursor(queries.get(i));
    }

    double epsilon = 0.001;
    // Before start is called
    assertEquals(0.0, spiedReader.getFractionConsumed(), epsilon);
    spiedReader.start();
    for (int i = 0; i < queries.size(); i++) {
      // When the first split-point is being processed.
      assertEquals(i / queries.size(), spiedReader.getFractionConsumed(), epsilon);
      spiedReader.advance();
    }

    // After all split-points have been processed.
    assertEquals(1.0, spiedReader.getFractionConsumed(), epsilon);
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    List<Query> queries = makeQueries(5);
    List<Entity> entities = makeEntities(5);
    V1Beta3Reader spiedReader = spy(reader);

    doReturn(queries).when(spiedSource).getQueries();
    for (int i = 0; i < queries.size(); i++) {
      doReturn(ImmutableList.of(makeEntityResult(entities.get(i))).iterator())
          .when(spiedReader).getIteratorAndMoveCursor(queries.get(i));
    }

    // Advance one split point
    spiedReader.start();
    spiedReader.advance();

    // Split at a fraction of 0.6
    V1Beta3Source residual = spiedReader.splitAtFraction(0.6);
    V1Beta3Source primary = spiedReader.getCurrentSource();

    // Verify that the primary and residual sources have the correct proportion of split points
    assertEquals(3, primary.getQueries().size());
    assertEquals(2, residual.getQueries().size());

    // Verify the reader associated with the primary source continues to work.
    assertTrue(spiedReader.advance());
    assertFalse(spiedReader.advance());
  }

  /**
   * A common test method for testing {@link V1Beta3Reader} for a given list of entities, when
   * no user-limit it set for a query.
   */
  private void testReaderWithEntities(V1Beta3Reader reader, List<Entity> entities)
      throws Exception {
    List<RunQueryRequest> requests = new LinkedList<>();
    List<RunQueryResponse> responses = new LinkedList<>();
    int batchStart = 0;
    int numBatches = 0;
    ByteString endCursor = null;

    // For each batch configure the mockDatastore to return the correct response for a each request
    while (batchStart < entities.size()) {
      int batchEnd = Math.min(batchStart + QUERY_BATCH_LIMIT, entities.size());
      Query query = makeQuery(QUERY, QUERY_BATCH_LIMIT, endCursor);
      RunQueryRequest request = makeRequest(query);
      RunQueryResponse response = makeQueryResponse(
          entities.subList(batchStart, batchEnd), batchEnd);

      when(mockDatastore.runQuery(request))
          .thenReturn(response);

      batchStart = batchEnd;
      endCursor = response.getBatch().getEndCursor();
      requests.add(request);
      responses.add(response);
      numBatches++;
    }

    assertTrue(reader.start());

    for (int i = 0; i < entities.size() - 1; i++) {
      assertEquals(entities.get(i), reader.getCurrent());
      assertTrue(reader.advance());
    }

    for (int i = 0; i < numBatches; i++) {
      verify(mockDatastore, times(1)).runQuery(requests.get(i));
    }

    assertEquals(entities.get(entities.size() - 1), reader.getCurrent());
    assertFalse(reader.advance());
    verifyNoMoreInteractions(mockDatastore);
  }

  // Helper methods

  private V1Beta3Reader makeReader(V1Beta3Source source) {
    return new V1Beta3Reader(source, mockDatastore);
  }

  private V1Beta3Source makeSource(Query query) {
    return new V1Beta3Source(PROJECT_ID, query, NAMESPACE);
  }

  private RunQueryRequest makeRequest(Query query) {
    return spiedSource.makeRequest(query);
  }

  private EntityResult makeEntityResult(Entity entity) {
    EntityResult.Builder entityResult = EntityResult.newBuilder();
    entityResult.setEntity(entity);
    return entityResult.build();
  }

  // Builds a list of unique queries.
  private List<Query> makeQueries(int count) {
    List<Query> queries = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      Query.Builder query = QUERY.toBuilder().clone();
      // Set a different offset to each query to make them unique.
      query.setOffset(i);
      queries.add(query.build());
    }

    return ImmutableList.copyOf(queries);
  }

  private Query makeQuery(Query query, int batchLimit, ByteString startCursor) {
    Query.Builder batchQuery = query.toBuilder().clone();
    batchQuery.setLimit(Int32Value.newBuilder().setValue(batchLimit));
    if (startCursor != null) {
      batchQuery.setStartCursor(startCursor);
    }
    return batchQuery.build();
  }

  private RunQueryResponse makeEmptyResponse() {
    return makeQueryResponse(ImmutableList.<Entity>of(), 0);
  }

  // Create a query response containing the given list of entities.
  private RunQueryResponse makeQueryResponse(List<Entity> entities, int endCursor) {
    RunQueryResponse.Builder response = RunQueryResponse.newBuilder();
    QueryResultBatch.Builder batch = QueryResultBatch.newBuilder();
    EntityResult.Builder entityResult = EntityResult.newBuilder();

    for (Entity entity: entities) {
      entityResult.setEntity(entity);
      batch.addEntityResults(entityResult);
    }

    batch.setEndCursor(byteStringFromInt(endCursor));

    response.setBatch(batch);
    return response.build();
  }

  // Creates a list of unique entities.
  private List<Entity> makeEntities(int count) {
    List<Entity> entities = new LinkedList<>();
    for (int i = 0; i < count; i++) {
      Entity.Builder entity = Entity.newBuilder();
      entity.getMutableProperties().put("id", makeValue(i).build());
      entities.add(entity.build());
    }
    return entities;
  }

  private ByteString byteStringFromInt(int i) {
    return ByteString.copyFrom(Ints.toByteArray(i));
  }
}

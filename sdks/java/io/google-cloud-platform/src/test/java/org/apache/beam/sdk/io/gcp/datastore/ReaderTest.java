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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.Reader.QUERY_BATCH_LIMIT;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.makeRequest;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.Reader;

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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests for {@link V1Beta3Helper.Reader}.
 */
@RunWith(JUnit4.class)
public class ReaderTest {
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;

  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
  }

  Reader reader;
  @Mock
  Datastore mockDatastore;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    reader = makeReader(QUERY, NAMESPACE, mockDatastore);
  }

  /**
   * Tests the {@link Reader#start} when there are no entities for a query.
   */
  @Test
  public void testReaderWithZeroEntities() throws Exception {
    when(mockDatastore.runQuery(any(RunQueryRequest.class)))
        .thenReturn(makeEmptyResponse());

    assertFalse(reader.start());
    verify(mockDatastore, times(1)).runQuery(any(RunQueryRequest.class));
  }

  /**
   * Tests the {@link Reader#advance} when there is only entity returned for a query.
   */
  @Test
  public void testReaderWithOneEntity() throws Exception {
    testReaderWithEntities(reader, makeEntities(1));
  }

  /**
   * Tests the {@link Reader#advance} when there is are multiple entities returned for a
   * query but fit within a single batch.
   */
  @Test
  public void testReaderWithMultipleEntities() throws Exception {
    // Entities fit in a single batch.
    testReaderWithEntities(reader, makeEntities(400));
  }

  /**
   * Tests the {@link Reader#advance} when there is are multiple entities returned for a
   * query and extend across multiple batches.
   */
  @Test
  public void testReaderWithMultipleBatches() throws Exception {
    // Entities that extend to 3 batches.
    testReaderWithEntities(reader, makeEntities(1200));
  }

  /**
   * Tests the {@link Reader#advance} when there is user limit set on the query.
   */
  @Test
  public void testReaderWithQueryLimit() throws Exception {
    int userQueryLimit = 10;
    Query queryWithLimit = QUERY.toBuilder().clone().setLimit(
        Int32Value.newBuilder().setValue(userQueryLimit).build()).build();
    List<Entity> entities = makeEntities(userQueryLimit);

    Reader reader = makeReader(queryWithLimit, NAMESPACE, mockDatastore);

    RunQueryRequest request = makeRequest(queryWithLimit, NAMESPACE);
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
   * A common test method for testing {@link Reader} for a given list of entities, when
   * no user-limit it set for a query.
   */
  private void testReaderWithEntities(Reader reader, List<Entity> entities)
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
      RunQueryRequest request = makeRequest(query, NAMESPACE);
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

  private Reader makeReader(Query query, String namespace, Datastore datastore) {
    return new Reader(query, namespace, mockDatastore);
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


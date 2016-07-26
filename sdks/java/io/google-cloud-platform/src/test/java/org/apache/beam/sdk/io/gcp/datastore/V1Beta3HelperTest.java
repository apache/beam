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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.getEstimatedSizeBytes;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.makeRequest;
import static com.google.datastore.v1beta3.PropertyFilter.Operator.EQUAL;
import static com.google.datastore.v1beta3.PropertyOrder.Direction.DESCENDING;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeOrder;
import static com.google.datastore.v1beta3.client.DatastoreHelper.makeValue;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.ReadFn;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.Reader;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.SplitQueryFn;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3Helper.V1Beta3Options;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;

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
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests for {@link V1Beta3Helper}.
 */
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

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  /**
   * Verifies that {@link V1Beta3Helper#getEstimatedSizeBytes} correctly fetches and returns the
   * size of entities from datastore.
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
   * Verifies that {@link V1Beta3Helper.SplitQueryFn} correctly splits the queries when
   * {@code numSplits} is specified by the user.
   */
  @Test
  public void testSplitQueryFnWithNumSplits() throws Exception {
    int numSplits = 100;
    ProcessContext mockContext = mock(ProcessContext.class);

    when(mockContext.element())
        .thenReturn(QUERY);
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, numSplits));

    doNothing().when(mockContext).output(QUERY);

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, numSplits,
        mockDatastore, mockQuerySplitter);
    splitQueryFn.processElement(mockContext);

    verify(mockContext, times(1)).element();
    verify(mockContext, times(numSplits)).output(QUERY);
    verify(mockQuerySplitter, times(1)).getSplits(
        eq(QUERY), any(PartitionId.class), eq(numSplits), any(Datastore.class));
    verifyZeroInteractions(mockDatastore);
  }

  /**
   * Verifies that {@link V1Beta3Helper.SplitQueryFn} correctly splits the queries when
   * {@code numSplits} is specified by the user.
   */
  @Test
  public void testSplitQueryFnWithoutNumSplits() throws Exception {
    // Force SplitQueryFn to compute the number of query splits
    int numSplits = 0;
    ProcessContext mockContext = mock(ProcessContext.class);
    // 1280 MB
    long entityBytes = 1280 * 1024 * 1024L;
    int expectedNumSplits = 20;

    // Per Kind statistics request and response
    RunQueryRequest statRequest = makeRequest(makeStatKindQuery(NAMESPACE), NAMESPACE);
    RunQueryResponse statResponse = makeStatKindResponse(entityBytes);

    when(mockDatastore.runQuery(statRequest))
        .thenReturn(statResponse);
    when(mockContext.element())
        .thenReturn(QUERY);
    when(mockQuerySplitter.getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class)))
        .thenReturn(splitQuery(QUERY, expectedNumSplits));

    doNothing().when(mockContext).output(QUERY);

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, numSplits,
        mockDatastore, mockQuerySplitter);

    splitQueryFn.processElement(mockContext);

    verify(mockContext, times(1)).element();
    verify(mockContext, times(expectedNumSplits)).output(QUERY);
    verify(mockQuerySplitter, times(1)).getSplits(
        eq(QUERY), any(PartitionId.class), eq(expectedNumSplits), any(Datastore.class));
    verify(mockDatastore, times(1)).runQuery(statRequest);
  }

  /**
   * Verifies that {@link V1Beta3Helper.SplitQueryFn} does not split when the query has
   * a user specified limit.
   */
  @Test
  public void testSplitQueryFnWithQueryLimit() throws Exception {
    Query queryWithLimit = QUERY.toBuilder().clone()
        .setLimit(Int32Value.newBuilder().setValue(1))
        .build();
    ProcessContext mockContext = mock(ProcessContext.class);

    when(mockContext.element())
        .thenReturn(queryWithLimit);

    doNothing().when(mockContext).output(queryWithLimit);

    SplitQueryFn splitQueryFn = new SplitQueryFn(v1Beta3Options, 10,
        mockDatastore, mockQuerySplitter);

    splitQueryFn.processElement(mockContext);
    verify(mockContext, times(1)).output(queryWithLimit);
    verifyNoMoreInteractions(mockDatastore);
    verifyNoMoreInteractions(mockQuerySplitter);
  }

  /**
   * Verifies that {@link V1Beta3Helper.ReadFn} reads and outputs all the entities for a given
   * query.
   */
  @Test
  public void testReadFn() throws Exception {
    ProcessContext mockContext = mock(ProcessContext.class);
    Entity entity = Entity.newBuilder().build();
    final int entityCount = 10;
    Reader mockReader = mock(Reader.class);
    ReadFn spiedReadFn = spy(new ReadFn(v1Beta3Options, mockDatastore));

    when(mockContext.element())
        .thenReturn(QUERY);
    doReturn(mockReader).when(spiedReadFn).createReader(QUERY);
    when(mockReader.start()).thenReturn(true);
    when(mockReader.getCurrent()).thenReturn(entity);
    doNothing().when(mockContext).output(entity);

    // Return {@code entityCount - 1} number of entities and then break
    when(mockReader.advance()).thenAnswer(new Answer<Boolean>() {
      int count = entityCount - 1;
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        if (count > 0) {
          count--;
          return true;
        }
        return false;
      }
    });

    spiedReadFn.processElement(mockContext);
    verify(mockContext, times(1)).element();
    verify(spiedReadFn, times(1)).createReader(QUERY);
    verify(mockReader, times(1)).start();
    verify(mockReader, times(entityCount)).getCurrent();
    verify(mockReader, times(entityCount)).advance();
    verify(mockContext, times(entityCount)).output(entity);
  }

  // Helper Methods

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

  // Dummy query splits
  private List<Query> splitQuery(Query query, int numSplits) {
    List<Query> queries = new LinkedList<>();
    for (int i = 0; i < numSplits; i++) {
      queries.add(query.toBuilder().clone().build());
    }
    return queries;
  }
}

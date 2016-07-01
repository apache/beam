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
package org.apache.beam.sdk.io.datastore;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;

import static com.google.datastore.v1beta3.client.DatastoreHelper.makeKey;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.io.datastore.V1Beta3.DatastoreReader;
import org.apache.beam.sdk.io.datastore.V1Beta3.DatastoreSource;
import org.apache.beam.sdk.io.datastore.V1Beta3.DatastoreWriter;
import org.apache.beam.sdk.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.TestCredential;

import com.google.common.collect.Lists;
import com.google.datastore.v1beta3.Entity;
import com.google.datastore.v1beta3.EntityResult;
import com.google.datastore.v1beta3.Key;
import com.google.datastore.v1beta3.KindExpression;
import com.google.datastore.v1beta3.PartitionId;
import com.google.datastore.v1beta3.PropertyFilter;
import com.google.datastore.v1beta3.Query;
import com.google.datastore.v1beta3.QueryResultBatch;
import com.google.datastore.v1beta3.RunQueryRequest;
import com.google.datastore.v1beta3.RunQueryResponse;
import com.google.datastore.v1beta3.Value;
import com.google.datastore.v1beta3.client.Datastore;
import com.google.datastore.v1beta3.client.DatastoreHelper;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for {@link V1Beta3}.
 */
@RunWith(JUnit4.class)
public class V1Beta3Test {
  private static final String PROJECT_ID = "testProject";
  private static final String NAMESPACE = "testNamespace";
  private static final String KIND = "testKind";
  private static final Query QUERY;
  static {
    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(KIND);
    QUERY = q.build();
  }
  private V1Beta3.Read initialRead;

  @Mock
  Datastore mockDatastore;

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Rule public final ExpectedLogs logged = ExpectedLogs.none(DatastoreSource.class);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    initialRead = DatastoreIO.v1beta3().read()
        .withProjectId(PROJECT_ID).withQuery(QUERY).withNamespace(NAMESPACE);
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
  public void testBuildRead() throws Exception {
    V1Beta3.Read read = DatastoreIO.v1beta3().read()
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
    V1Beta3.Read read =  DatastoreIO.v1beta3().read()
        .withProjectId(PROJECT_ID).withNamespace(NAMESPACE).withQuery(QUERY);
    assertEquals(QUERY, read.getQuery());
    assertEquals(PROJECT_ID, read.getProjectId());
    assertEquals(NAMESPACE, read.getNamespace());
  }

  @Test
  public void testReadValidationFailsProject() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withQuery(QUERY);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("project");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQuery() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withProjectId(PROJECT_ID);
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("query");
    read.validate(null);
  }

  @Test
  public void testReadValidationFailsQueryLimitZero() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(0)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit 0: must be positive");

    DatastoreIO.v1beta3().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationFailsQueryLimitNegative() throws Exception {
    Query invalidLimit = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(-5)).build();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid query limit -5: must be positive");

    DatastoreIO.v1beta3().read().withQuery(invalidLimit);
  }

  @Test
  public void testReadValidationSucceedsNamespace() throws Exception {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withProjectId(PROJECT_ID).withQuery(QUERY);
    /* Should succeed, as a null namespace is fine. */
    read.validate(null);
  }

  @Test
  public void testReadDisplayData() {
    V1Beta3.Read read =  DatastoreIO.v1beta3().read()
      .withProjectId(PROJECT_ID)
      .withQuery(QUERY)
      .withNamespace(NAMESPACE);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
    assertThat(displayData, hasDisplayItem("query", QUERY.toString()));
    assertThat(displayData, hasDisplayItem("namespace", NAMESPACE));
  }

  @Test
  public void testWriteDoesNotAllowNullProject() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    DatastoreIO.v1beta3().write().withProjectId(null);
  }

  @Test
  public void testWriteValidationFailsWithNoProject() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write();

    thrown.expect(NullPointerException.class);
    thrown.expectMessage("projectId");

    write.validate(null);
  }

  @Test
  public void testSinkValidationSucceedsWithProject() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write().withProjectId(PROJECT_ID);
    write.validate(null);
  }

  @Test
  public void testWriteDisplayData() {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write()
        .withProjectId(PROJECT_ID);

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("projectId", PROJECT_ID));
  }

  @Test
  public void testQuerySplitBasic() throws Exception {
    KindExpression mykind = KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    List<Query> mockSplits = new ArrayList<>();
    for (int i = 0; i < 8; ++i) {
      mockSplits.add(
          Query.newBuilder()
              .addKind(mykind)
              .setFilter(
                  DatastoreHelper.makeFilter("foo", PropertyFilter.Operator.EQUAL,
                      Value.newBuilder().setIntegerValue(i).build()))
              .build());
    }

    QuerySplitter splitter = mock(QuerySplitter.class);
    /* No namespace */
    PartitionId partition = PartitionId.newBuilder().build();
    when(splitter.getSplits(any(Query.class), eq(partition), eq(8), any(Datastore.class)))
        .thenReturn(mockSplits);

    DatastoreSource io = initialRead
        .withNamespace(null)
        .withQuery(query)
        .getSource()
        .withMockSplitter(splitter)
        .withMockEstimateSizeBytes(8 * 1024L);

    List<DatastoreSource> bundles = io.splitIntoBundles(1024, testPipelineOptions());
    assertEquals(8, bundles.size());
    for (int i = 0; i < 8; ++i) {
      DatastoreSource bundle = bundles.get(i);
      Query bundleQuery = bundle.getQuery();
      assertEquals("mykind", bundleQuery.getKind(0).getName());
      assertEquals(i, bundleQuery.getFilter().getPropertyFilter().getValue().getIntegerValue());
    }
  }

  /**
   * Verifies that when namespace is set in the source, the split request includes the namespace.
   */
  @Test
  public void testSourceWithNamespace() throws Exception {
    QuerySplitter splitter = mock(QuerySplitter.class);
    DatastoreSource io = initialRead
        .getSource()
        .withMockSplitter(splitter)
        .withMockEstimateSizeBytes(8 * 1024L);

    io.splitIntoBundles(1024, testPipelineOptions());

    PartitionId partition = PartitionId.newBuilder().setNamespaceId(NAMESPACE).build();
    verify(splitter).getSplits(eq(QUERY), eq(partition), eq(8), any(Datastore.class));
    verifyNoMoreInteractions(splitter);
  }

  @Test
  public void testQuerySplitWithZeroSize() throws Exception {
    KindExpression mykind = KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    List<Query> mockSplits = Lists.newArrayList(
        Query.newBuilder()
            .addKind(mykind)
            .build());

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), any(PartitionId.class), eq(1), any(Datastore.class)))
        .thenReturn(mockSplits);

    DatastoreSource io = initialRead
        .withQuery(query)
        .getSource()
        .withMockSplitter(splitter)
        .withMockEstimateSizeBytes(0L);

    List<DatastoreSource> bundles = io.splitIntoBundles(1024, testPipelineOptions());
    assertEquals(1, bundles.size());
    verify(splitter, never())
        .getSplits(any(Query.class), any(PartitionId.class), eq(1), any(Datastore.class));
    DatastoreSource bundle = bundles.get(0);
    Query bundleQuery = bundle.getQuery();
    assertEquals("mykind", bundleQuery.getKind(0).getName());
    assertFalse(bundleQuery.hasFilter());
  }

  /**
   * Tests that a query with a user-provided limit field does not split, and does not even
   * interact with a query splitter.
   */
  @Test
  public void testQueryDoesNotSplitWithLimitSet() throws Exception {
    // Minimal query with a limit
    Query query = Query.newBuilder().setLimit(Int32Value.newBuilder().setValue(5)).build();

    // Mock query splitter, should not be invoked.
    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), any(PartitionId.class), eq(2), any(Datastore.class)))
        .thenThrow(new AssertionError("Splitter should not be invoked"));

    List<DatastoreSource> bundles =
        initialRead
            .withQuery(query)
            .getSource()
            .withMockSplitter(splitter)
            .splitIntoBundles(1024, testPipelineOptions());

    assertEquals(1, bundles.size());
    assertEquals(query, bundles.get(0).getQuery());
    verifyNoMoreInteractions(splitter);
  }

  /**
   * Tests that when {@link QuerySplitter} cannot split a query, {@link V1Beta3} falls back to
   * a single split.
   */
  @Test
  public void testQuerySplitterThrows() throws Exception {
    // Mock query splitter that throws IllegalArgumentException
    IllegalArgumentException exception =
        new IllegalArgumentException("query not supported by splitter");
    QuerySplitter splitter = mock(QuerySplitter.class);
    when(
            splitter.getSplits(
                any(Query.class), any(PartitionId.class), any(Integer.class), any(Datastore.class)))
        .thenThrow(exception);

    Query query = Query.newBuilder().addKind(KindExpression.newBuilder().setName("myKind")).build();
    List<DatastoreSource> bundles =
        initialRead
            .withQuery(query)
            .getSource()
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(10240L)
            .splitIntoBundles(1024, testPipelineOptions());

    assertEquals(1, bundles.size());
    assertEquals(query, bundles.get(0).getQuery());
    verify(splitter, times(1))
        .getSplits(
            any(Query.class), any(PartitionId.class), any(Integer.class), any(Datastore.class));
    logged.verifyWarn("Unable to parallelize the given query", exception);
  }

  @Test
  public void testQuerySplitSizeUnavailable() throws Exception {
    KindExpression mykind = KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    List<Query> mockSplits = Lists.newArrayList(Query.newBuilder().addKind(mykind).build());

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), any(PartitionId.class), eq(12), any(Datastore.class)))
        .thenReturn(mockSplits);

    DatastoreSource io = initialRead
        .withQuery(query)
        .getSource()
        .withMockSplitter(splitter)
        .withMockEstimateSizeBytes(8 * 1024L);

    DatastoreSource spiedIo = spy(io);
    when(spiedIo.getEstimatedSizeBytes(any(PipelineOptions.class)))
        .thenThrow(new NoSuchElementException());

    List<DatastoreSource> bundles = spiedIo.splitIntoBundles(1024, testPipelineOptions());
    assertEquals(1, bundles.size());
    verify(splitter, never())
        .getSplits(any(Query.class), any(PartitionId.class), eq(1), any(Datastore.class));
    DatastoreSource bundle = bundles.get(0);
    Query bundleQuery = bundle.getQuery();
    assertEquals("mykind", bundleQuery.getKind(0).getName());
    assertFalse(bundleQuery.hasFilter());
  }

  /**
   * Test building a Write using builder methods.
   */
  @Test
  public void testBuildWrite() throws Exception {
    V1Beta3.Write write =  DatastoreIO.v1beta3().write().withProjectId(PROJECT_ID);
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
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id, no ancestor
    key = makeKey("bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete, no ancestor
    key = makeKey("bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    // Complete with name and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", "horned").build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id and ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete with ancestor
    key = makeKey("bird", "owl").build();
    key = makeKey(key, "bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    key = makeKey().build();
    assertFalse(DatastoreWriter.isValidKey(key));
  }

  /**
   * Test that entities with incomplete keys cannot be updated.
   */
  @Test
  public void testAddEntitiesWithIncompleteKeys() throws Exception {
    Key key = makeKey("bird").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    DatastoreWriter writer = new DatastoreWriter(null, mockDatastore);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Entities to be written to the Datastore must have complete keys");

    writer.write(entity);
  }

  /**
   * Test that entities are added to the batch to update.
   */
  @Test
  public void testAddingEntities() throws Exception {
    List<Entity> expected = Lists.newArrayList(
        Entity.newBuilder().setKey(makeKey("bird", "jay").build()).build(),
        Entity.newBuilder().setKey(makeKey("bird", "condor").build()).build(),
        Entity.newBuilder().setKey(makeKey("bird", "robin").build()).build());

    List<Entity> allEntities = Lists.newArrayList(expected);
    Collections.shuffle(allEntities);

    DatastoreWriter writer = new DatastoreWriter(null, mockDatastore);
    writer.open("test_id");
    for (Entity entity : allEntities) {
      writer.write(entity);
    }

    assertEquals(expected.size(), writer.entities.size());
    assertThat(writer.entities, containsInAnyOrder(expected.toArray()));
  }

  /** Datastore batch API limit in number of records per query. */
  private static final int DATASTORE_QUERY_BATCH_LIMIT = 500;

  /**
   * A helper function that creates mock {@link Entity} results in response to a query. Always
   * indicates that more results are available, unless the batch is limited to fewer than
   * {@link #DATASTORE_QUERY_BATCH_LIMIT} results.
   */
  private static RunQueryResponse mockResponseForQuery(Query q) {
    // Every query V1Beta3 sends should have a limit.
    assertTrue(q.hasLimit());

    // The limit should be in the range [1, DATASTORE_QUERY_BATCH_LIMIT]
    int limit = q.getLimit().getValue();
    assertThat(limit, greaterThanOrEqualTo(1));
    assertThat(limit, lessThanOrEqualTo(DATASTORE_QUERY_BATCH_LIMIT));

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
            limit == DATASTORE_QUERY_BATCH_LIMIT
                ? QueryResultBatch.MoreResultsType.NOT_FINISHED
                : QueryResultBatch.MoreResultsType.NO_MORE_RESULTS);

    return ret.build();
  }

  /** Helper function to run a test reading from a limited-result query. */
  private void runQueryLimitReadTest(int numEntities) throws Exception {
    // An empty query to read entities.
    Query query = Query.newBuilder().setLimit(
        Int32Value.newBuilder().setValue(numEntities)).build();
    V1Beta3.Read read =  DatastoreIO.v1beta3().read().withQuery(query).withProjectId("mockProject");

    // Use mockResponseForQuery to generate results.
    when(mockDatastore.runQuery(any(RunQueryRequest.class)))
        .thenAnswer(
            new Answer<RunQueryResponse>() {
              @Override
              public RunQueryResponse answer(InvocationOnMock invocation) throws Throwable {
                Query q = ((RunQueryRequest) invocation.getArguments()[0]).getQuery();
                return mockResponseForQuery(q);
              }
            });

    // Actually instantiate the reader.
    DatastoreReader reader = new DatastoreReader(read.getSource(), mockDatastore);

    // Simply count the number of results returned by the reader.
    assertTrue(reader.start());
    int resultCount = 1;
    while (reader.advance()) {
      resultCount++;
    }
    reader.close();

    // Validate the number of results.
    assertEquals(numEntities, resultCount);
  }

  /** Tests reading with a query limit less than one batch. */
  @Test
  public void testReadingWithLimitOneBatch() throws Exception {
    runQueryLimitReadTest(5);
  }

  /** Tests reading with a query limit more than one batch, and not a multiple. */
  @Test
  public void testReadingWithLimitMultipleBatches() throws Exception {
    runQueryLimitReadTest(DATASTORE_QUERY_BATCH_LIMIT + 5);
  }

  /** Tests reading several batches, using an exact multiple of batch size results. */
  @Test
  public void testReadingWithLimitMultipleBatchesExactMultiple() throws Exception {
    runQueryLimitReadTest(5 * DATASTORE_QUERY_BATCH_LIMIT);
  }
}

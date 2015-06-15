/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Key;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.QuerySplitter;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.io.DatastoreIO.DatastoreWriter;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.Write;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for DatastoreIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class DatastoreIOTest {
  private String host;
  private String datasetId;
  private Query query;

  @Mock
  Datastore mockDatastore;

  /**
   * Sets the default dataset ID as "shakespearedataset", which
   * contains two kinds of records: "food" and "shakespeare".
   * The "food" table contains 10 manually constructed entities,
   * The "shakespeare" table contains 172948 entities,
   * where each entity represents one line in one play in
   * Shakespeare collections (e.g. there are 172948 lines in
   * all Shakespeare files).
   *
   * <p> The function also sets up the datastore agent by creating
   * a Datastore object to access the dataset shakespeareddataset.
   *
   * <p> Note that the local server must be started to let the agent
   * be created normally.
   */
  @Before
  public void setUp() {
    this.host = "http://localhost:1234";
    this.datasetId = "shakespearedataset";

    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName("shakespeare");
    this.query = q.build();

    MockitoAnnotations.initMocks(this);
  }

  /**
   * Test for reading one entity from kind "food".
   */
  @Test
  public void testBuildRead() throws Exception {
    DatastoreIO.Source readQuery =
        DatastoreIO.read().withHost(this.host).withDataset(this.datasetId).withQuery(this.query);
    assertEquals(this.query, readQuery.query);
    assertEquals(this.datasetId, readQuery.datasetId);
    assertEquals(this.host, readQuery.host);
  }

  @Test
  public void testBuildReadAlt() throws Exception {
    DatastoreIO.Source readQuery =
        DatastoreIO.read().withDataset(this.datasetId).withQuery(this.query).withHost(this.host);
    assertEquals(this.query, readQuery.query);
    assertEquals(this.datasetId, readQuery.datasetId);
    assertEquals(this.host, readQuery.host);
  }

  @Test(expected = NullPointerException.class)
  public void testBuildReadWithoutDatastoreSettingToCatchException() throws Exception {
    // create pipeline and run the pipeline to get result
    Pipeline p = DirectPipeline.createForTest();
    p.apply(Read.from(DatastoreIO.read().withHost(null)));
  }

  @Test
  public void testQuerySplitWithMockSplitter() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    List<Query> mockSplits = new ArrayList<>();
    for (int i = 0; i < 8; ++i) {
      mockSplits.add(
          Query.newBuilder()
              .addKind(mykind)
              .setFilter(
                  DatastoreHelper.makeFilter("foo", DatastoreV1.PropertyFilter.Operator.EQUAL,
                      DatastoreV1.Value.newBuilder().setIntegerValue(i).build()))
              .build());
    }

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), eq(8), any(Datastore.class))).thenReturn(mockSplits);

    DatastoreIO.Source io =
        DatastoreIO.read()
            .withDataset(dataset)
            .withQuery(query)
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(8 * 1024L);

    List<DatastoreIO.Source> bundles = io.splitIntoBundles(1024, options);
    assertEquals(8, bundles.size());
    for (int i = 0; i < 8; ++i) {
      DatastoreIO.Source bundle = bundles.get(i);
      Query bundleQuery = bundle.query;
      assertEquals("mykind", bundleQuery.getKind(0).getName());
      assertEquals(i, bundleQuery.getFilter().getPropertyFilter().getValue().getIntegerValue());
    }
  }

  @Test
  public void testQuerySplitWithZeroSize() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    List<Query> mockSplits = Lists.newArrayList(
        Query.newBuilder()
            .addKind(mykind)
            .build());

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), eq(1), any(Datastore.class))).thenReturn(mockSplits);

    DatastoreIO.Source io =
        DatastoreIO.read()
            .withDataset(dataset)
            .withQuery(query)
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(0L);

    List<DatastoreIO.Source> bundles = io.splitIntoBundles(1024, options);
    assertEquals(1, bundles.size());
    DatastoreIO.Source bundle = bundles.get(0);
    Query bundleQuery = bundle.query;
    assertEquals("mykind", bundleQuery.getKind(0).getName());
  }

  @Test
  public void testQuerySplitSizeUnavailable() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setNumWorkers(2);

    List<Query> mockSplits = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      mockSplits.add(
          Query.newBuilder()
              .addKind(mykind)
              .setFilter(
                  DatastoreHelper.makeFilter("foo", DatastoreV1.PropertyFilter.Operator.EQUAL,
                      DatastoreV1.Value.newBuilder().setIntegerValue(i).build()))
              .build());
    }

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), eq(2), any(Datastore.class))).thenReturn(mockSplits);

    DatastoreIO.Source io =
        DatastoreIO.read()
            .withDataset(dataset)
            .withQuery(query)
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(8 * 1024L);

    DatastoreIO.Source spiedIo = spy(io);
    when(spiedIo.getEstimatedSizeBytes(any(PipelineOptions.class))).thenThrow(new IOException());

    List<DatastoreIO.Source> bundles = spiedIo.splitIntoBundles(1024, options);
    assertEquals(2, bundles.size());
    for (int i = 0; i < 2; ++i) {
      DatastoreIO.Source bundle = bundles.get(i);
      Query bundleQuery = bundle.query;
      assertEquals("mykind", bundleQuery.getKind(0).getName());
      assertEquals(i, bundleQuery.getFilter().getPropertyFilter().getValue().getIntegerValue());
    }
  }

  @Test
  public void testQuerySplitNoWorkers() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    options.setNumWorkers(0);

    List<Query> mockSplits = Lists.newArrayList(Query.newBuilder().addKind(mykind).build());

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), eq(1), any(Datastore.class))).thenReturn(mockSplits);

    DatastoreIO.Source io =
        DatastoreIO.read()
            .withDataset(dataset)
            .withQuery(query)
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(8 * 1024L);

    DatastoreIO.Source spiedIo = spy(io);
    when(spiedIo.getEstimatedSizeBytes(any(PipelineOptions.class)))
        .thenThrow(new NoSuchElementException());

    List<DatastoreIO.Source> bundles = spiedIo.splitIntoBundles(1024, options);
    assertEquals(1, bundles.size());
    DatastoreIO.Source bundle = bundles.get(0);
    Query bundleQuery = bundle.query;
    assertEquals("mykind", bundleQuery.getKind(0).getName());
  }

  @Test
  public void testQuerySplitWithSmallDataset() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options =
        PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    List<Query> mockSplits = Lists.newArrayList(
        Query.newBuilder()
            .addKind(mykind)
            .build());

    QuerySplitter splitter = mock(QuerySplitter.class);
    when(splitter.getSplits(any(Query.class), eq(1), any(Datastore.class))).thenReturn(mockSplits);

    DatastoreIO.Source io =
        DatastoreIO.read()
            .withDataset(dataset)
            .withQuery(query)
            .withMockSplitter(splitter)
            .withMockEstimateSizeBytes(1L);

    List<DatastoreIO.Source> bundles = io.splitIntoBundles(1024, options);
    assertEquals(1, bundles.size());
    DatastoreIO.Source bundle = bundles.get(0);
    Query bundleQuery = bundle.query;
    assertEquals("mykind", bundleQuery.getKind(0).getName());
  }

  /**
   * Test building a Sink using builder methods.
   */
  @Test
  public void testBuildWrite() throws Exception {
    DatastoreIO.Sink sink = DatastoreIO.sink().withDataset(this.datasetId).withHost(this.host);
    assertEquals(this.host, sink.host);
    assertEquals(this.datasetId, sink.datasetId);

    sink = DatastoreIO.sink().withHost(this.host).withDataset(this.datasetId);
    assertEquals(this.host, sink.host);
    assertEquals(this.datasetId, sink.datasetId);

    sink = DatastoreIO.sink().withDataset(this.datasetId).withHost(this.host);
    assertEquals(this.host, sink.host);
    assertEquals(this.datasetId, sink.datasetId);
  }

  /**
   * Test building a sink using the default host.
   */
  @Test
  public void testBuildWriteDefaults() throws Exception {
    DatastoreIO.Sink sink = DatastoreIO.sink().withDataset(this.datasetId);
    assertEquals(DatastoreIO.DEFAULT_HOST, sink.host);
    assertEquals(this.datasetId, sink.datasetId);

    sink = DatastoreIO.sink().withDataset(this.datasetId);
    assertEquals(DatastoreIO.DEFAULT_HOST, sink.host);
    assertEquals(this.datasetId, sink.datasetId);
  }

  /**
   * Test building an invalid sink.
   */
  @Test(expected = NullPointerException.class)
  public void testBuildWriteWithoutDatastoreToCatchException() throws Exception {
    // create pipeline and run the pipeline to get result
    Pipeline p = DirectPipeline.createForTest();
    p.apply(Create.<Entity>of().withCoder(EntityCoder.of())).apply(Write.to(DatastoreIO.sink()));
  }

  /**
   * Test the detection of complete and incomplete keys.
   */
  @Test
  public void testHasNameOrId() {
    Key key;
    // Complete with name, no ancestor
    key = DatastoreHelper.makeKey("bird", "finch").build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id, no ancestor
    key = DatastoreHelper.makeKey("bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete, no ancestor
    key = DatastoreHelper.makeKey("bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    // Complete with name and ancestor
    key = DatastoreHelper.makeKey("bird", "owl").build();
    key = DatastoreHelper.makeKey(key, "bird", "horned").build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Complete with id and ancestor
    key = DatastoreHelper.makeKey("bird", "owl").build();
    key = DatastoreHelper.makeKey(key, "bird", 123).build();
    assertTrue(DatastoreWriter.isValidKey(key));

    // Incomplete with ancestor
    key = DatastoreHelper.makeKey("bird", "owl").build();
    key = DatastoreHelper.makeKey(key, "bird").build();
    assertFalse(DatastoreWriter.isValidKey(key));

    key = DatastoreHelper.makeKey().build();
    assertFalse(DatastoreWriter.isValidKey(key));
  }

  /**
   * Test that entities with incomplete keys cannot be updated.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testAddEntitiesWithIncompleteKeys() throws Exception {
    Key key = DatastoreHelper.makeKey("bird").build();
    Entity entity = Entity.newBuilder().setKey(key).build();
    DatastoreWriter writer = new DatastoreIO.DatastoreWriter(null, mockDatastore);
    writer.write(entity);
  }

  /**
   * Test that entities are added to the batch to update.
   */
  @Test
  public void testAddingEntities() throws Exception {
    List<Entity> expected = Lists.newArrayList(
        Entity.newBuilder().setKey(DatastoreHelper.makeKey("bird", "jay").build()).build(),
        Entity.newBuilder().setKey(DatastoreHelper.makeKey("bird", "condor").build()).build(),
        Entity.newBuilder().setKey(DatastoreHelper.makeKey("bird", "robin").build()).build());

    List<Entity> allEntities = new ArrayList<>();
    allEntities.addAll(expected);
    Collections.shuffle(allEntities);

    DatastoreWriter writer = new DatastoreIO.DatastoreWriter(null, mockDatastore);
    writer.open("test_id");
    for (Entity entity : allEntities) {
      writer.write(entity);
    }

    assertEquals(expected.size(), writer.entities.size());
    assertThat(writer.entities, containsInAnyOrder(expected.toArray()));
  }
}

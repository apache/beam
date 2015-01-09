/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.datastore.DatastoreV1;
import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.api.services.datastore.client.QuerySplitter;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.common.base.Supplier;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for DatastoreIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
public class DatastoreIOTest {
  private String host;
  private String datasetId;
  private Query query;

  /**
   * Sets the default dataset ID as "shakespearedataset",
   * which contains two kinds of records: "food" and "shakespeare".
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
  }

  /**
   * Test for reading one entity from kind "food".
   */
  @Test
  @Category(com.google.cloud.dataflow.sdk.testing.RunnableOnService.class)
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
    p.apply(ReadSource.from(DatastoreIO.read().withHost(null)));
  }

  @Test
  public void testQuerySplitWithMockSplitter() throws Exception {
    String dataset = "mydataset";
    DatastoreV1.KindExpression mykind =
        DatastoreV1.KindExpression.newBuilder().setName("mykind").build();
    Query query = Query.newBuilder().addKind(mykind).build();

    DataflowPipelineOptions options = PipelineOptionsFactory.create()
        .as(DataflowPipelineOptions.class);
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
            .withMockEstimateSizeBytes(new Supplier<Long>() {
              @Override
              public Long get() {
                return 8 * 1024L;
              }
            });

    List<DatastoreIO.Source> shards = io.splitIntoShards(1024, options);
    assertEquals(8, shards.size());
    for (int i = 0; i < 8; ++i) {
      DatastoreIO.Source shard = shards.get(i);
      Query shardQuery = shard.query;
      assertEquals("mykind", shardQuery.getKind(0).getName());
      assertEquals(i, shardQuery.getFilter().getPropertyFilter().getValue().getIntegerValue());
    }
  }

  @Test
  public void testBuildWrite() throws Exception {
    DatastoreIO.Sink sink = DatastoreIO.write().to(this.datasetId).withHost(this.host);
    assertEquals(this.host, sink.host);
    assertEquals(this.datasetId, sink.datasetId);
  }

  @Test
  public void testBuildWriteAlt() throws Exception {
    DatastoreIO.Sink sink = DatastoreIO.write().withHost(this.host).to(this.datasetId);
    assertEquals(this.host, sink.host);
    assertEquals(this.datasetId, sink.datasetId);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWriteWithoutDatastoreToCatchException() throws Exception {
    // create pipeline and run the pipeline to get result
    Pipeline p = DirectPipeline.createForTest();
    p.apply(Create.<Entity>of())
        .setCoder(EntityCoder.of())
        .apply(DatastoreIO.write().named("WriteDatastore"));
  }
}

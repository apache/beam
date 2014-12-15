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

import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.EntityCoder;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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
    DatastoreIO.Read.Bound readQuery = DatastoreIO.Read
        .withHost(this.host)
        .from(this.datasetId, this.query);
    assertEquals(this.query, readQuery.query);
    assertEquals(this.datasetId, readQuery.datasetId);
    assertEquals(this.host, readQuery.host);
  }

  @Test
  public void testBuildReadAlt() throws Exception {
    DatastoreIO.Read.Bound readQuery = DatastoreIO.Read
        .from(this.datasetId, this.query)
        .withHost(this.host);
    assertEquals(this.query, readQuery.query);
    assertEquals(this.datasetId, readQuery.datasetId);
    assertEquals(this.host, readQuery.host);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildReadWithoutDatastoreSettingToCatchException()
      throws Exception {
    // create pipeline and run the pipeline to get result
    Pipeline p = DirectPipeline.createForTest();
    p.apply(DatastoreIO.Read.named("ReadDatastore"));
  }

  @Test
  public void testBuildWrite() throws Exception {
    DatastoreIO.Write.Bound write = DatastoreIO.Write
        .to(this.datasetId)
        .withHost(this.host);
    assertEquals(this.host, write.host);
    assertEquals(this.datasetId, write.datasetId);
  }

  @Test
  public void testBuildWriteAlt() throws Exception {
    DatastoreIO.Write.Bound write = DatastoreIO.Write
        .withHost(this.host)
        .to(this.datasetId);
    assertEquals(this.host, write.host);
    assertEquals(this.datasetId, write.datasetId);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildWriteWithoutDatastoreToCatchException() throws Exception {
    // create pipeline and run the pipeline to get result
    Pipeline p = DirectPipeline.createForTest();
    p.apply(Create.<Entity>of()).setCoder(EntityCoder.of())
        .apply(DatastoreIO.Write.named("WriteDatastore"));
  }
}

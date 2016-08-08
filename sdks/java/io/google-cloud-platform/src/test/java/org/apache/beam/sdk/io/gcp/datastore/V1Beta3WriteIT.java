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

import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.countEntities;
import static org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.deleteAllEntities;
import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.CountingInput;
import org.apache.beam.sdk.io.gcp.datastore.V1Beta3TestUtil.CreateEntityFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.UUID;

/**
 * End-to-end tests for Datastore V1Beta3.Write.
 */
@RunWith(JUnit4.class)
public class V1Beta3WriteIT {
  private V1Beta3TestOptions options;
  private String ancestor;
  private final long numEntities = 1000;

  @Before
  public void setup() {
    PipelineOptionsFactory.register(V1Beta3TestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(V1Beta3TestOptions.class);
    ancestor = UUID.randomUUID().toString();
  }

  /**
   * An end-to-end test for {@link V1Beta3.Write}.
   *
   * Write some test entities to datastore through a dataflow pipeline.
   * Read and count all the entities. Verify that the count matches the
   * number of entities written.
   */
  @Test
  public void testE2EV1Beta3Write() throws Exception {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(CountingInput.upTo(numEntities))
        .apply(ParDo.of(new CreateEntityFn(
            options.getKind(), options.getNamespace(), ancestor)))
        .apply(DatastoreIO.v1beta3().write().withProjectId(options.getProject()));

    p.run();

    // Count number of entities written to datastore.
    long numEntitiesWritten = countEntities(options, ancestor);

    assertEquals(numEntitiesWritten, numEntities);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllEntities(options, ancestor);
  }
}

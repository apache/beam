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

import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.countEntities;
import static org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.deleteAllEntities;
import static org.junit.Assert.assertEquals;

import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.CreateEntityFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end tests for Datastore DatastoreV1.Write. */
@RunWith(JUnit4.class)
public class V1WriteIT {
  private V1TestOptions options;
  private String project;
  private String ancestor;
  private final long numEntities = 1000;

  @Before
  public void setup() {
    PipelineOptionsFactory.register(V1TestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(V1TestOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    ancestor = UUID.randomUUID().toString();
  }

  /**
   * An end-to-end test for {@link DatastoreV1.Write}.
   *
   * <p>Write some test entities to Cloud Datastore. Read and count all the entities. Verify that
   * the count matches the number of entities written.
   */
  @Test
  public void testE2EV1Write() throws Exception {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numEntities))
        .apply(ParDo.of(new CreateEntityFn(options.getKind(), options.getNamespace(), ancestor, 0)))
        .apply(DatastoreIO.v1().write().withProjectId(project));

    p.run();

    // Count number of entities written to datastore.
    long numEntitiesWritten = countEntities(options, project, ancestor);

    assertEquals(numEntities, numEntitiesWritten);
  }

  /**
   * An end-to-end test for {@link DatastoreV1.Write}.
   *
   * <p>Write some large test entities to Cloud Datastore, to test that a batch is flushed when the
   * byte size limit is reached. Read and count all the entities. Verify that the count matches the
   * number of entities written.
   */
  @Test
  public void testE2EV1WriteWithLargeEntities() throws Exception {
    Pipeline p = Pipeline.create(options);

    /*
     * Datastore has a limit of 1MB per entity, and 10MB per write RPC. If each entity is around
     * 1MB in size, then we hit the limit on the size of the write long before we hit the limit on
     * the number of entities per writes.
     */
    final int rawPropertySize = 900_000;
    final int numLargeEntities = 100;

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numLargeEntities))
        .apply(
            ParDo.of(
                new CreateEntityFn(
                    options.getKind(), options.getNamespace(), ancestor, rawPropertySize)))
        .apply(DatastoreIO.v1().write().withProjectId(project));

    p.run();

    // Count number of entities written to datastore.
    long numEntitiesWritten = countEntities(options, project, ancestor);

    assertEquals(numLargeEntities, numEntitiesWritten);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllEntities(options, project, ancestor);
  }
}

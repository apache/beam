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

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.datastore.V1TestUtil.CreateEntityFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * End-to-end tests for Datastore DatastoreV1.Write.
 */
@RunWith(JUnit4.class)
public class V1BatchSizingIT {

  private static final Logger LOG = LoggerFactory.getLogger(V1BatchSizingIT.class);

  private V1TestOptions options;
  private String project;
  private final long numEntities = 1_000_000;

  @Before
  public void setup() {
    PipelineOptionsFactory.register(V1TestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(V1TestOptions.class);
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
  }

  @Test
  public void testE2EV1Write() {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numEntities))
        .apply(ParDo
            .of(new CreateEntityFn(options.getKind(), options.getNamespace(), null, 1, 0, 4)))
        .apply(DatastoreIO.v1().write().withProjectId(project).withHintNumWorkers(20));

    // Completed successfully.
    State state = p.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }

  @Test
  public void testE2EV1WriteMediumIndexFanout() {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numEntities))
        .apply(ParDo
            .of(new CreateEntityFn(options.getKind(), options.getNamespace(), null, 20, 0, 4)))
        .apply(DatastoreIO.v1().write().withProjectId(project).withHintNumWorkers(20));

    // Completed successfully.
    State state = p.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }

  @Test
  public void testE2EV1WriteHighIndexFanout() {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(10_000_000))
        .apply(ParDo
            .of(new CreateEntityFn(options.getKind(), options.getNamespace(), null, 100, 0, 4)))
        .apply(DatastoreIO.v1().write().withProjectId(project).withHintNumWorkers(20));

    // Completed successfully.
    State state = p.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }

  @Test
  public void testE2EV1WriteExtremeIndexFanout() {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numEntities))
        .apply(ParDo
            .of(new CreateEntityFn(options.getKind(), options.getNamespace(), null, 500, 0, 4)))
        .apply(DatastoreIO.v1().write().withProjectId(project).withHintNumWorkers(20));

    // Completed successfully.
    State state = p.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }

  @Test
  public void testE2EV1WriteLargeEntities() {
    Pipeline p = Pipeline.create(options);

    // Write to datastore
    p.apply(GenerateSequence.from(0).to(numEntities))
        .apply(ParDo
            .of(new CreateEntityFn(options.getKind(), options.getNamespace(), null, 0, 500,
                1_000)))
        .apply(DatastoreIO.v1().write().withProjectId(project).withHintNumWorkers(20));

    // Completed successfully.
    State state = p.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }

  // @After
  // public void tearDown() throws Exception {
  //   deleteAllEntities(options, project, ancestor);
  // }
}

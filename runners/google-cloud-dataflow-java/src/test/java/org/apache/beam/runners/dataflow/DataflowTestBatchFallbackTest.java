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
package org.apache.beam.runners.dataflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.dataflow.DataflowTestBatchCoordinator.ScopedDataflowPipelineJob;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.testing.BeamParallelJunit4Runner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ValidatesRunner tests that deliberately exercise {@link DataflowTestBatchCoordinator}'s fallback
 * path against the real service: one test fails only while it is part of a merged job, so the
 * merged job fails, the coordinator attributes the failure, that test's {@code run()} ends in
 * {@link TestPipeline.StandaloneRerunRequested}, and {@link BeamParallelJunit4Runner} executes the
 * test again from scratch as a job of its own (where it passes). The other tests are healthy
 * companions that give the merged job stages to attribute; they pass whether they are credited from
 * the merged job or executed again.
 *
 * <p>Keep this class small. Everything merged with the failing test may have to be executed again,
 * and since a test JVM runs one test class at a time a merged job only ever contains tests of a
 * single class, so the size of this class bounds that cost. All tests run concurrently under {@link
 * BeamParallelJunit4Runner} so that they do end up in the same batch.
 *
 * <p>When batching is disabled (streaming tasks, {@code -PtestBatching=false}) every test here runs
 * standalone and simply passes.
 */
@RunWith(BeamParallelJunit4Runner.class)
public class DataflowTestBatchFallbackTest {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowTestBatchFallbackTest.class);

  @Rule public final transient TestPipeline p = TestPipeline.create();

  /** Fails inside a merged job (recognised by its job name) and passes in a standalone one. */
  private static class FailWhenMergedFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      String jobName = c.getPipelineOptions().getJobName();
      if (jobName != null
          && jobName.startsWith(DataflowTestBatchCoordinator.BATCH_JOB_NAME_PREFIX)) {
        throw new IllegalStateException(
            "Deliberate failure inside merged Dataflow test job " + jobName);
      }
      c.output(c.element());
    }
  }

  private static class IdentityFn extends DoFn<Integer, Integer> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testMemberFailingOnlyWhenMergedFallsBackAndPasses() {
    PCollection<Integer> out =
        p.apply(Create.of(1, 2, 3)).apply("FailWhenMerged", ParDo.of(new FailWhenMergedFn()));
    PAssert.that(out).containsInAnyOrder(1, 2, 3);

    PipelineResult result = p.run();

    // This test can never be credited from a merged job: its DoFn throws before any PAssert runs
    // there, so the merged attempt has no success counters for it. Either batching was off, or this
    // is the standalone re-execution of the test.
    assertThat(result, not(instanceOf(ScopedDataflowPipelineJob.class)));
    assertEquals(State.DONE, result.getState());
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testHealthyCompanionA() {
    runHealthyCompanion("CompanionA");
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testHealthyCompanionB() {
    runHealthyCompanion("CompanionB");
  }

  private void runHealthyCompanion(String name) {
    PCollection<Integer> out = p.apply(Create.of(4, 5, 6)).apply(name, ParDo.of(new IdentityFn()));
    PAssert.that(out).containsInAnyOrder(4, 5, 6);

    PipelineResult result = p.run();

    // Whether the coordinator could credit this test from the failed merged job (stage states
    // attributable) or had to re-run it is informative for the postcommit log, but both are passes.
    LOG.info(
        "{} finished {} ({})",
        name,
        result.getState(),
        result instanceof ScopedDataflowPipelineJob
            ? "credited from merged job"
            : "standalone or re-run");
    assertEquals(State.DONE, result.getState());
  }
}

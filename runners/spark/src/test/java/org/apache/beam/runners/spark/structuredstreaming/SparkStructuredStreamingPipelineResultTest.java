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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.sdk.PipelineResult.State;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the cancel and wait semantics of {@link SparkStructuredStreamingPipelineResult}. */
@RunWith(JUnit4.class)
public class SparkStructuredStreamingPipelineResultTest {

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final AtomicInteger cancelSparkJobsCalls = new AtomicInteger();

  @After
  public void shutdownExecutor() {
    executor.shutdownNow();
  }

  private SparkStructuredStreamingPipelineResult result(Future<?> execution, Runnable onCancel) {
    Runnable cancelSparkJobs =
        () -> {
          cancelSparkJobsCalls.incrementAndGet();
          onCancel.run();
        };
    return new SparkStructuredStreamingPipelineResult(
        execution, () -> null, new MetricsAccumulator(), new AtomicBoolean(), cancelSparkJobs);
  }

  @Test
  public void testCancelJoinsExecutionThread() throws Exception {
    CountDownLatch jobsCancelled = new CountDownLatch(1);
    AtomicBoolean finished = new AtomicBoolean();
    Future<?> execution =
        executor.submit(
            () -> {
              jobsCancelled.await();
              finished.set(true);
              return null;
            });
    SparkStructuredStreamingPipelineResult result = result(execution, jobsCancelled::countDown);

    assertThat(result.cancel(), is(State.CANCELLED));
    assertTrue("cancel returned before the execution thread ended", finished.get());
    assertThat(result.getState(), is(State.CANCELLED));
    assertThat(cancelSparkJobsCalls.get(), is(1));

    assertThat(result.cancel(), is(State.CANCELLED));
    assertThat(cancelSparkJobsCalls.get(), is(1));
  }

  @Test
  public void testCancelAfterCompletionKeepsTerminalState() throws Exception {
    SparkStructuredStreamingPipelineResult result =
        result(CompletableFuture.completedFuture(null), () -> {});

    assertThat(result.waitUntilFinish(), is(State.DONE));
    assertThat(result.cancel(), is(State.DONE));
    assertThat(result.getState(), is(State.DONE));
    assertThat(cancelSparkJobsCalls.get(), is(0));
  }

  @Test
  public void testCancelOfUnobservedCompletionReportsDone() throws Exception {
    SparkStructuredStreamingPipelineResult result =
        result(CompletableFuture.completedFuture(null), () -> {});

    assertThat(result.cancel(), is(State.DONE));
    assertThat(cancelSparkJobsCalls.get(), is(0));
  }

  @Test
  public void testCancelOfUnobservedFailureReportsFailed() throws Exception {
    CompletableFuture<Void> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("boom"));
    SparkStructuredStreamingPipelineResult result = result(failed, () -> {});

    assertThat(result.cancel(), is(State.FAILED));
    assertThat(cancelSparkJobsCalls.get(), is(0));
    assertThrows(RuntimeException.class, result::waitUntilFinish);
  }

  @Test
  public void testInterruptedCancelKeepsStateAndInterruptFlag() throws Exception {
    CountDownLatch jobsCancelled = new CountDownLatch(1);
    Future<?> execution =
        executor.submit(
            () -> {
              jobsCancelled.await();
              return null;
            });
    SparkStructuredStreamingPipelineResult result = result(execution, () -> {});

    Thread.currentThread().interrupt();
    assertThat(result.cancel(), is(State.RUNNING));
    assertTrue("interrupt flag not restored", Thread.interrupted());

    jobsCancelled.countDown();
    assertThat(result.cancel(), is(State.CANCELLED));
  }

  @Test
  public void testFailureAfterCancelIsCancelled() throws Exception {
    CountDownLatch jobsCancelled = new CountDownLatch(1);
    Future<?> execution =
        executor.submit(
            () -> {
              jobsCancelled.await();
              throw new IllegalStateException("job cancelled");
            });
    SparkStructuredStreamingPipelineResult result = result(execution, jobsCancelled::countDown);

    assertThat(result.cancel(), is(State.CANCELLED));
    assertThat(result.waitUntilFinish(), is(State.CANCELLED));
  }
}

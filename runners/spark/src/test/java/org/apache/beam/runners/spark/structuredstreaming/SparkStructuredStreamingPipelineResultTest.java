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
import static org.junit.Assert.assertFalse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.runners.spark.structuredstreaming.metrics.MetricsAccumulator;
import org.apache.beam.sdk.PipelineResult.State;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the cancel and wait semantics of {@link SparkStructuredStreamingPipelineResult}. */
@RunWith(JUnit4.class)
public class SparkStructuredStreamingPipelineResultTest {

  private final AtomicInteger cancelSparkJobsCalls = new AtomicInteger();

  private SparkStructuredStreamingPipelineResult result(Future<?> execution) {
    return new SparkStructuredStreamingPipelineResult(
        execution,
        () -> null,
        new MetricsAccumulator(),
        new AtomicBoolean(),
        cancelSparkJobsCalls::incrementAndGet);
  }

  @Test
  public void testCancelRunsJobCancelHookOnce() throws Exception {
    SparkStructuredStreamingPipelineResult result = result(new CompletableFuture<>());
    assertThat(result.cancel(), is(State.CANCELLED));
    assertThat(result.cancel(), is(State.CANCELLED));
    assertThat(cancelSparkJobsCalls.get(), is(1));
  }

  @Test
  public void testCancelIsAsynchronous() throws Exception {
    CompletableFuture<Void> execution = new CompletableFuture<>();
    SparkStructuredStreamingPipelineResult result = result(execution);
    assertThat(result.cancel(), is(State.CANCELLED));
    assertFalse(execution.isDone());
    execution.completeExceptionally(new IllegalStateException("job cancelled"));
    assertThat(result.waitUntilFinish(), is(State.CANCELLED));
  }
}

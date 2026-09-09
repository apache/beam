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
package org.apache.beam.runners.flink;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import org.apache.beam.sdk.PipelineResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.joda.time.Duration;
import org.junit.Test;

/** Tests for {@link FlinkRunnerResult}. */
public class FlinkRunnerResultTest {

  @Test
  public void testPipelineResultReturnsDone() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    assertThat(result.getState(), is(PipelineResult.State.DONE));
  }

  @Test
  public void testWaitUntilFinishReturnsDone() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    assertThat(result.waitUntilFinish(), is(PipelineResult.State.DONE));
    assertThat(result.waitUntilFinish(Duration.millis(100)), is(PipelineResult.State.DONE));
  }

  @Test
  public void testCancelDoesNotThrowAnException() {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    result.cancel();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
  }

  @Test
  public void testDrainDoneResultDoesNotThrowAnException() throws Exception {
    FlinkRunnerResult result = new FlinkRunnerResult(Collections.emptyMap(), 100);
    assertThat(result.drain(), is(PipelineResult.State.DONE));
  }

  @Test
  public void testDetachedDrainReturnsDrainingThenDrained() throws Exception {
    JobClient jobClient = mock(JobClient.class);
    CompletableFuture<String> drainFuture = new CompletableFuture<>();
    when(jobClient.stopWithSavepoint(true, null, SavepointFormatType.DEFAULT))
        .thenReturn(drainFuture);
    FlinkDetachedRunnerResult result = new FlinkDetachedRunnerResult(jobClient, 1);

    assertThat(result.drain(), is(PipelineResult.State.DRAINING));
    assertThat(result.getState(), is(PipelineResult.State.DRAINING));

    drainFuture.complete("savepoint");
    assertThat(result.getState(), is(PipelineResult.State.DRAINED));
    verify(jobClient).stopWithSavepoint(true, null, SavepointFormatType.DEFAULT);
  }

  @Test
  public void testDetachedDrainFailureThrowsIOException() throws Exception {
    JobClient jobClient = mock(JobClient.class);
    CompletableFuture<String> drainFuture = new CompletableFuture<>();
    RuntimeException failure = new RuntimeException("savepoint failed");
    drainFuture.completeExceptionally(failure);
    when(jobClient.stopWithSavepoint(true, null, SavepointFormatType.DEFAULT))
        .thenReturn(drainFuture);
    FlinkDetachedRunnerResult result = new FlinkDetachedRunnerResult(jobClient, 1);

    try {
      result.drain();
      fail("Expected IOException");
    } catch (IOException e) {
      assertThat(e.getMessage(), is("Failed to drain Flink job"));
      assertSame(failure, e.getCause());
    }
  }

  @Test
  public void testDetachedGetStateFallsBackAfterDrainFailure() throws Exception {
    JobClient jobClient = mock(JobClient.class);
    CompletableFuture<String> drainFuture = new CompletableFuture<>();
    drainFuture.completeExceptionally(new RuntimeException("savepoint failed"));
    when(jobClient.stopWithSavepoint(true, null, SavepointFormatType.DEFAULT))
        .thenReturn(drainFuture);
    when(jobClient.getJobStatus()).thenReturn(CompletableFuture.completedFuture(JobStatus.RUNNING));
    FlinkDetachedRunnerResult result = new FlinkDetachedRunnerResult(jobClient, 1);

    try {
      result.drain();
      fail("Expected IOException");
    } catch (IOException expected) {
      assertThat(result.getState(), is(PipelineResult.State.RUNNING));
    }
  }

  @Test
  public void testDetachedDrainRetriesAfterFailure() throws Exception {
    JobClient jobClient = mock(JobClient.class);
    CompletableFuture<String> failedDrainFuture = new CompletableFuture<>();
    failedDrainFuture.completeExceptionally(new RuntimeException("savepoint failed"));
    CompletableFuture<String> retryDrainFuture = new CompletableFuture<>();
    when(jobClient.stopWithSavepoint(true, null, SavepointFormatType.DEFAULT))
        .thenReturn(failedDrainFuture, retryDrainFuture);
    FlinkDetachedRunnerResult result = new FlinkDetachedRunnerResult(jobClient, 1);

    try {
      result.drain();
      fail("Expected IOException");
    } catch (IOException expected) {
      assertThat(result.drain(), is(PipelineResult.State.DRAINING));
    }

    retryDrainFuture.complete("savepoint");
    assertThat(result.getState(), is(PipelineResult.State.DRAINED));
    verify(jobClient, times(2)).stopWithSavepoint(true, null, SavepointFormatType.DEFAULT);
  }
}

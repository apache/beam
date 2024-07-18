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
package org.apache.beam.runners.prism;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.runners.prism.PrismRunnerTest.getLocalPrismBuildOrIgnoreTest;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismPipelineResult}. */
@RunWith(JUnit4.class)
public class PrismPipelineResultTest {

  final PrismExecutor exec = executor();

  @Before
  public void setUp() throws IOException {
    exec.execute();
    assertThat(exec.isAlive()).isTrue();
  }

  @After
  public void tearDown() {
    assertThat(exec.isAlive()).isFalse();
  }

  @Test
  public void givenTerminated_reportsState() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.waitUntilFinish()).thenReturn(PipelineResult.State.FAILED);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    // Assigns terminal state.
    underTest.waitUntilFinish();
    assertThat(underTest.getState()).isEqualTo(PipelineResult.State.FAILED);
  }

  @Test
  public void givenNotTerminated_reportsState() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.getState()).thenReturn(PipelineResult.State.RUNNING);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    assertThat(underTest.getState()).isEqualTo(PipelineResult.State.RUNNING);
    exec.stop();
  }

  @Test
  public void cancelStopsExecutable_reportsTerminalState() throws IOException {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.cancel()).thenReturn(PipelineResult.State.CANCELLED);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    assertThat(underTest.cancel()).isEqualTo(PipelineResult.State.CANCELLED);
  }

  @Test
  public void givenTerminated_cancelIsNoop_reportsTerminalState() throws IOException {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.cancel()).thenReturn(PipelineResult.State.FAILED);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    assertThat(underTest.cancel()).isEqualTo(PipelineResult.State.FAILED);
  }

  @Test
  public void givenPipelineRunWithDuration_waitUntilFinish_reportsTerminalState() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.waitUntilFinish(Duration.millis(3000L)))
        .thenReturn(PipelineResult.State.CANCELLED);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    assertThat(underTest.waitUntilFinish(Duration.millis(3000L)))
        .isEqualTo(PipelineResult.State.CANCELLED);
  }

  @Test
  public void givenTerminated_waitUntilFinishIsNoop_reportsTerminalState() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.waitUntilFinish()).thenReturn(PipelineResult.State.DONE);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    // Terminate Job as setup for additional call.
    underTest.waitUntilFinish();
    assertThat(underTest.waitUntilFinish()).isEqualTo(PipelineResult.State.DONE);
  }

  @Test
  public void givenNotTerminated_reportsMetrics() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.metrics()).thenReturn(mock(MetricResults.class));
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    assertThat(underTest.metrics()).isNotNull();
    exec.stop();
  }

  @Test
  public void givenTerminated_reportsTerminatedMetrics() {
    PipelineResult delegate = mock(PipelineResult.class);
    when(delegate.metrics()).thenReturn(mock(MetricResults.class));
    when(delegate.waitUntilFinish()).thenReturn(PipelineResult.State.DONE);
    PrismPipelineResult underTest = new PrismPipelineResult(delegate, exec::stop);
    // Terminate Job as setup for additional call.
    underTest.waitUntilFinish();
    assertThat(underTest.metrics()).isNotNull();
  }

  private static PrismExecutor executor() {
    return PrismExecutor.builder().setCommand(getLocalPrismBuildOrIgnoreTest()).build();
  }
}

/*
 * Copyright (C) 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.it.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.it.dataflow.DataflowOperator.Config;
import com.google.cloud.teleport.it.dataflow.DataflowOperator.Result;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link DataflowOperator}. */
@RunWith(JUnit4.class)
public final class DataflowOperatorTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private DataflowTemplateClient client;

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String JOB_ID = "test-job-id";
  private static final Duration CHECK_AFTER = Duration.ofMillis(10);
  private static final Duration TIMEOUT_AFTER = Duration.ofMillis(100);

  private static final Config DEFAULT_CONFIG =
      Config.builder()
          .setProject(PROJECT)
          .setRegion(REGION)
          .setJobId(JOB_ID)
          .setCheckAfter(CHECK_AFTER)
          .setTimeoutAfter(TIMEOUT_AFTER)
          .build();

  @Captor private ArgumentCaptor<String> projectCaptor;
  @Captor private ArgumentCaptor<String> regionCaptor;
  @Captor private ArgumentCaptor<String> jobIdCaptor;

  @Test
  public void testWaitUntilDone() throws IOException {
    // Arrange
    when(client.getJobStatus(any(), any(), any()))
        .thenReturn(JobState.QUEUED)
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLING)
        .thenReturn(JobState.CANCELLED);

    // Act
    Result result = new DataflowOperator(client).waitUntilDone(DEFAULT_CONFIG);

    // Assert
    verify(client, times(4))
        .getJobStatus(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());

    Set<String> allProjects = new HashSet<>(projectCaptor.getAllValues());
    Set<String> allRegions = new HashSet<>(regionCaptor.getAllValues());
    Set<String> allJobIds = new HashSet<>(jobIdCaptor.getAllValues());

    assertThat(allProjects).containsExactly(PROJECT);
    assertThat(allRegions).containsExactly(REGION);
    assertThat(allJobIds).containsExactly(JOB_ID);
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testWaitUntilDoneTimeout() throws IOException {
    when(client.getJobStatus(any(), any(), any())).thenReturn(JobState.RUNNING);
    Result result = new DataflowOperator(client).waitUntilDone(DEFAULT_CONFIG);
    assertThat(result).isEqualTo(Result.TIMEOUT);
  }

  @Test
  public void testWaitForCondition() throws IOException {
    AtomicInteger callCount = new AtomicInteger();
    int totalCalls = 3;
    Supplier<Boolean> checker = () -> callCount.incrementAndGet() >= totalCalls;
    when(client.getJobStatus(any(), any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenThrow(new IOException())
        .thenReturn(JobState.RUNNING);

    Result result = new DataflowOperator(client).waitForCondition(DEFAULT_CONFIG, checker);

    verify(client, atMost(totalCalls))
        .getJobStatus(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testWaitForConditionJobFinished() throws IOException {
    when(client.getJobStatus(any(), any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLED);

    Result result = new DataflowOperator(client).waitForCondition(DEFAULT_CONFIG, () -> false);

    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testWaitForConditionTimeout() throws IOException {
    when(client.getJobStatus(any(), any(), any())).thenReturn(JobState.RUNNING);

    Result result = new DataflowOperator(client).waitForCondition(DEFAULT_CONFIG, () -> false);

    assertThat(result).isEqualTo(Result.TIMEOUT);
  }

  @Test
  public void testFinishAfterCondition() throws IOException {
    // Arrange
    AtomicInteger callCount = new AtomicInteger();
    int totalCalls = 3;
    Supplier<Boolean> checker = () -> callCount.incrementAndGet() >= totalCalls;

    when(client.getJobStatus(any(), any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenThrow(new IOException())
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLING)
        .thenReturn(JobState.CANCELLED);
    doAnswer(invocation -> null).when(client).cancelJob(any(), any(), any());

    // Act
    Result result = new DataflowOperator(client).waitForConditionAndFinish(DEFAULT_CONFIG, checker);

    // Assert
    verify(client, atLeast(totalCalls))
        .getJobStatus(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());
    verify(client)
        .cancelJob(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());

    Set<String> allProjects = new HashSet<>(projectCaptor.getAllValues());
    Set<String> allRegions = new HashSet<>(regionCaptor.getAllValues());
    Set<String> allJobIds = new HashSet<>(jobIdCaptor.getAllValues());

    assertThat(allProjects).containsExactly(PROJECT);
    assertThat(allRegions).containsExactly(REGION);
    assertThat(allJobIds).containsExactly(JOB_ID);
    assertThat(result).isEqualTo(Result.CONDITION_MET);
  }

  @Test
  public void testFinishAfterConditionJobStopped() throws IOException {
    when(client.getJobStatus(any(), any(), any()))
        .thenReturn(JobState.RUNNING)
        .thenReturn(JobState.CANCELLED);
    doAnswer(invocation -> null).when(client).cancelJob(any(), any(), any());

    Result result =
        new DataflowOperator(client).waitForConditionAndFinish(DEFAULT_CONFIG, () -> false);

    verify(client, never()).cancelJob(any(), any(), any());
    assertThat(result).isEqualTo(Result.JOB_FINISHED);
  }

  @Test
  public void testFinishAfterConditionTimeout() throws IOException {
    when(client.getJobStatus(any(), any(), any())).thenReturn(JobState.RUNNING);
    doAnswer(invocation -> null).when(client).cancelJob(any(), any(), any());

    Result result =
        new DataflowOperator(client).waitForConditionAndFinish(DEFAULT_CONFIG, () -> false);

    verify(client).cancelJob(any(), any(), any());
    assertThat(result).isEqualTo(Result.TIMEOUT);
  }
}

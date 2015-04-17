/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.dataflow.sdk.runners;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.V1b3.Projects.Jobs.Messages;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.dataflow.sdk.PipelineResult.State;
import com.google.cloud.dataflow.sdk.testing.FastNanoClockAndSleeper;
import com.google.cloud.dataflow.sdk.util.AttemptBoundedExponentialBackOff;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for DataflowPipelineJob.
 */
@RunWith(JUnit4.class)
public class DataflowPipelineJobTest {
  private static final String PROJECT_ID = "someProject";
  private static final String JOB_ID = "1234";

  @Mock
  private Dataflow mockWorkflowClient;
  @Mock
  private Dataflow.V1b3 mockV1b3;
  @Mock
  private Dataflow.V1b3.Projects mockProjects;
  @Mock
  private Dataflow.V1b3.Projects.Jobs mockJobs;
  @Rule
  public FastNanoClockAndSleeper fastClock = new FastNanoClockAndSleeper();


  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(mockWorkflowClient.v1b3()).thenReturn(mockV1b3);
    when(mockV1b3.projects()).thenReturn(mockProjects);
    when(mockProjects.jobs()).thenReturn(mockJobs);
  }

/**
   * Validates that a given time is valid for the total time slept by a
   * AttemptBoundedExponentialBackOff given the number of retries and
   * an initial polling interval.
   *
   * @param pollingIntervalMillis The initial polling interval given.
   * @param attempts The number of attempts made
   * @param timeSleptMillis The amount of time slept by the clock. This is checked
   * against the valid interval.
   */
  void checkValidInterval(long pollingIntervalMillis, int attempts, long timeSleptMillis){
    long highSum = 0;
    long lowSum = 0;
    for (int i = 1; i < attempts; i++) {
      double currentInterval = pollingIntervalMillis
          * Math.pow(AttemptBoundedExponentialBackOff.DEFAULT_MULTIPLIER, i - 1);
      double offset = AttemptBoundedExponentialBackOff.DEFAULT_RANDOMIZATION_FACTOR
          * currentInterval;
      highSum += Math.round(currentInterval + offset);
      lowSum += Math.round(currentInterval - offset);
    }
    assertThat(timeSleptMillis, allOf(greaterThanOrEqualTo(lowSum), lessThanOrEqualTo(highSum)));
  }

  @Test
  public void testWaitToFinishMessagesFail() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest = mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.DONE.name());
    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID)))
        .thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    MonitoringUtil.JobMessagesHandler jobHandler = mock(MonitoringUtil.JobMessagesHandler.class);
    Dataflow.V1b3.Projects.Jobs.Messages mockMessages =
        mock(Dataflow.V1b3.Projects.Jobs.Messages.class);
    Messages.List listRequest = mock(Dataflow.V1b3.Projects.Jobs.Messages.List.class);
    when(mockJobs.messages()).thenReturn(mockMessages);
    when(mockMessages.list(eq(PROJECT_ID), eq(JOB_ID))).thenReturn(listRequest);
    when(listRequest.execute()).thenThrow(SocketTimeoutException.class);

    DataflowPipelineJob job = new DataflowPipelineJob(
        PROJECT_ID, JOB_ID, mockWorkflowClient);

    State state = job.waitToFinish(5, TimeUnit.MINUTES, jobHandler, fastClock, fastClock);
    assertEquals(null, state);
  }

  @Test
  public void testWaitToFinish() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest = mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.DONE.name());

    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID)))
        .thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowPipelineJob job = new DataflowPipelineJob(
        PROJECT_ID, JOB_ID, mockWorkflowClient);

    State state = job.waitToFinish(1, TimeUnit.MINUTES, null, fastClock, fastClock);
    assertEquals(State.DONE, state);
  }

  @Test
  public void testWaitToFinishFail() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest = mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID)))
        .thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job = new DataflowPipelineJob(
        PROJECT_ID, JOB_ID, mockWorkflowClient);

    long startTime = fastClock.nanoTime();
    State state = job.waitToFinish(5, TimeUnit.MINUTES, null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(DataflowPipelineJob.MESSAGES_POLLING_INTERVAL,
        DataflowPipelineJob.MESSAGES_POLLING_ATTEMPTS, timeDiff);
  }

  @Test
  public void testWaitToFinishTimeFail() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest = mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID)))
        .thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job = new DataflowPipelineJob(
        PROJECT_ID, JOB_ID, mockWorkflowClient);
    long startTime = fastClock.nanoTime();
    State state = job.waitToFinish(4, TimeUnit.MILLISECONDS, null, fastClock, fastClock);
    assertEquals(null, state);
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    // Should only sleep for the 4 ms remaining.
    assertEquals(timeDiff, 4L);
  }

  @Test
  public void testGetStateReturnsServiceState() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest =
        mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState("JOB_STATE_" + State.RUNNING.name());

    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowPipelineJob job =
        new DataflowPipelineJob(PROJECT_ID, JOB_ID, mockWorkflowClient);

    assertEquals(State.RUNNING, job.getStateWithRetries(
        DataflowPipelineJob.STATUS_POLLING_ATTEMPTS, fastClock));
  }

  @Test
  public void testGetStateWithExceptionReturnsUnknown() throws Exception {
    Dataflow.V1b3.Projects.Jobs.Get statusRequest =
        mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID))).thenReturn(statusRequest);
    when(statusRequest.execute()).thenThrow(IOException.class);

    DataflowPipelineJob job =
        new DataflowPipelineJob(PROJECT_ID, JOB_ID, mockWorkflowClient);

    long startTime = fastClock.nanoTime();
    assertEquals(State.UNKNOWN, job.getStateWithRetries(
        DataflowPipelineJob.STATUS_POLLING_ATTEMPTS, fastClock));
    long timeDiff = TimeUnit.NANOSECONDS.toMillis(fastClock.nanoTime() - startTime);
    checkValidInterval(DataflowPipelineJob.STATUS_POLLING_INTERVAL,
        DataflowPipelineJob.STATUS_POLLING_ATTEMPTS, timeDiff);
  }
}

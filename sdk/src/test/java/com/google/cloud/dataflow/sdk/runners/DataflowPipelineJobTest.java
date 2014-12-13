/*
 * Copyright (C) 2014 Google Inc.
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil.JobState;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for DataflowPipelineJob.
 */
@RunWith(JUnit4.class)
public class DataflowPipelineJobTest {
  private static final String PROJECT_ID = "someProject";
  private static final String JOB_ID = "1234";

  @Test
  public void testWaitToFinish() throws IOException, InterruptedException {
    Dataflow mockWorkflowClient = mock(Dataflow.class);
    Dataflow.V1b3 mockV1b3 = mock(Dataflow.V1b3.class);
    Dataflow.V1b3.Projects mockProjects = mock(Dataflow.V1b3.Projects.class);
    Dataflow.V1b3.Projects.Jobs mockJobs = mock(Dataflow.V1b3.Projects.Jobs.class);
    Dataflow.V1b3.Projects.Jobs.Get statusRequest = mock(Dataflow.V1b3.Projects.Jobs.Get.class);

    Job statusResponse = new Job();
    statusResponse.setCurrentState(JobState.DONE.getStateName());

    when(mockWorkflowClient.v1b3()).thenReturn(mockV1b3);
    when(mockV1b3.projects()).thenReturn(mockProjects);
    when(mockProjects.jobs()).thenReturn(mockJobs);
    when(mockJobs.get(eq(PROJECT_ID), eq(JOB_ID)))
        .thenReturn(statusRequest);
    when(statusRequest.execute()).thenReturn(statusResponse);

    DataflowPipelineJob job = new DataflowPipelineJob(
        PROJECT_ID, JOB_ID, mockWorkflowClient);

    JobState state = job.waitToFinish(1, TimeUnit.MINUTES, null);
    assertEquals(JobState.DONE, state);
  }
}

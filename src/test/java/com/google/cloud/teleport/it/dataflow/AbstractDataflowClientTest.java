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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Get;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Update;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.teleport.it.dataflow.DataflowClient.JobState;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for {@link AbstractDataflowClient}. */
@RunWith(JUnit4.class)
public final class AbstractDataflowClientTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Dataflow client;

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String JOB_ID = "test-job-id";

  @Captor private ArgumentCaptor<String> projectCaptor;
  @Captor private ArgumentCaptor<String> regionCaptor;
  @Captor private ArgumentCaptor<String> jobIdCaptor;
  @Captor private ArgumentCaptor<Job> jobCaptor;

  @Test
  public void testGetJobStatus() throws IOException {
    Get get = mock(Get.class);
    Job job = new Job().setCurrentState(JobState.RUNNING.toString());
    when(getLocationJobs(client).get(any(), any(), any())).thenReturn(get);
    when(get.execute()).thenReturn(job);

    JobState actual = new FakeDataflowClient(client).getJobStatus(PROJECT, REGION, JOB_ID);

    verify(getLocationJobs(client))
        .get(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(actual).isEqualTo(JobState.RUNNING);
  }

  @Test
  public void testGetJobThrowsException() throws IOException {
    when(getLocationJobs(client).get(any(), any(), any())).thenThrow(new IOException());
    assertThrows(
        IOException.class,
        () -> new FakeDataflowClient(client).getJobStatus(PROJECT, REGION, JOB_ID));
  }

  @Test
  public void testCancelJob() throws IOException {
    Update update = mock(Update.class);
    when(getLocationJobs(client).update(any(), any(), any(), any())).thenReturn(update);
    when(update.execute()).thenReturn(new Job());

    new FakeDataflowClient(client).cancelJob(PROJECT, REGION, JOB_ID);

    verify(getLocationJobs(client))
        .update(
            projectCaptor.capture(),
            regionCaptor.capture(),
            jobIdCaptor.capture(),
            jobCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(jobCaptor.getValue().getRequestedState()).isEqualTo(JobState.CANCELLED.toString());
  }

  @Test
  public void testCancelJobThrowsException() throws IOException {
    when(getLocationJobs(client).update(any(), any(), any(), any())).thenThrow(new IOException());
    assertThrows(
        IOException.class, () -> new FakeDataflowClient(client).cancelJob(PROJECT, REGION, JOB_ID));
  }

  @Test
  public void testDrainJob() throws IOException {
    Update update = mock(Update.class);
    when(getLocationJobs(client).update(any(), any(), any(), any())).thenReturn(update);
    when(update.execute()).thenReturn(new Job());

    new FakeDataflowClient(client).drainJob(PROJECT, REGION, JOB_ID);

    verify(getLocationJobs(client))
        .update(
            projectCaptor.capture(),
            regionCaptor.capture(),
            jobIdCaptor.capture(),
            jobCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);
    assertThat(jobCaptor.getValue().getRequestedState()).isEqualTo(JobState.DRAINED.toString());
  }

  @Test
  public void testDrainJobThrowsException() throws IOException {
    when(getLocationJobs(client).update(any(), any(), any(), any())).thenThrow(new IOException());
    assertThrows(
        IOException.class, () -> new FakeDataflowClient(client).drainJob(PROJECT, REGION, JOB_ID));
  }

  private static Locations.Jobs getLocationJobs(Dataflow client) {
    return client.projects().locations().jobs();
  }

  /**
   * Fake implementation that simply throws {@link UnsupportedOperationException} for some methods.
   */
  private static final class FakeDataflowClient extends AbstractDataflowClient {
    FakeDataflowClient(Dataflow client) {
      super(client);
    }

    @Override
    public JobInfo launch(String project, String region, LaunchConfig options) {
      throw new UnsupportedOperationException();
    }
  }
}

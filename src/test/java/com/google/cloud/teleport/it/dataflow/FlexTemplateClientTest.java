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
import com.google.api.services.dataflow.Dataflow.Projects.Locations.FlexTemplates;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.FlexTemplates.Launch;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Get;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchFlexTemplateParameter;
import com.google.api.services.dataflow.model.LaunchFlexTemplateRequest;
import com.google.api.services.dataflow.model.LaunchFlexTemplateResponse;
import com.google.auth.Credentials;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobInfo;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.JobState;
import com.google.cloud.teleport.it.dataflow.DataflowTemplateClient.LaunchOptions;
import com.google.common.collect.ImmutableMap;
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

/** Unit test for {@link FlexTemplateClient}. */
@RunWith(JUnit4.class)
public final class FlexTemplateClientTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Dataflow client;

  private static final String PROJECT = "test-project";
  private static final String REGION = "us-east1";
  private static final String JOB_ID = "test-job-id";
  private static final String JOB_NAME = "test-job";
  private static final String SPEC_PATH = "gs://test-bucket/test-dir/test-spec.json";

  private static final String PARAM_KEY = "key";
  private static final String PARAM_VALUE = "value";

  @Captor private ArgumentCaptor<String> projectCaptor;
  @Captor private ArgumentCaptor<String> regionCaptor;
  @Captor private ArgumentCaptor<String> jobIdCaptor;
  @Captor private ArgumentCaptor<LaunchFlexTemplateRequest> requestCaptor;

  @Test
  public void testCreateWithCredentials() {
    Credentials credentials = mock(Credentials.class);
    FlexTemplateClient.builder().setCredentials(credentials).build();
    // Lack of exception is all we really can test
  }

  @Test
  public void testLaunchNewJob() throws IOException {
    // Arrange
    Launch launch = mock(Launch.class);
    Get get = mock(Get.class);
    Job launchJob = new Job().setId(JOB_ID);
    Job getJob = new Job().setId(JOB_ID).setCurrentState(JobState.QUEUED.toString());
    LaunchFlexTemplateResponse response = new LaunchFlexTemplateResponse().setJob(launchJob);

    LaunchOptions options =
        LaunchOptions.builder(JOB_NAME, SPEC_PATH).addParameter(PARAM_KEY, PARAM_VALUE).build();

    when(getFlexTemplates(client).launch(any(), any(), any())).thenReturn(launch);
    when(getLocationJobs(client).get(any(), any(), any())).thenReturn(get);
    when(launch.execute()).thenReturn(response);
    when(get.execute()).thenReturn(getJob);

    // Act
    JobInfo actual =
        FlexTemplateClient.withDataflowClient(client).launchTemplate(PROJECT, REGION, options);

    // Assert
    LaunchFlexTemplateRequest expectedRequest =
        new LaunchFlexTemplateRequest()
            .setLaunchParameter(
                new LaunchFlexTemplateParameter()
                    .setJobName(JOB_NAME)
                    .setContainerSpecGcsPath(SPEC_PATH)
                    .setParameters(ImmutableMap.of(PARAM_KEY, PARAM_VALUE)));
    verify(getFlexTemplates(client))
        .launch(projectCaptor.capture(), regionCaptor.capture(), requestCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(requestCaptor.getValue()).isEqualTo(expectedRequest);

    verify(getLocationJobs(client))
        .get(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);

    JobInfo expected = JobInfo.builder().setJobId(JOB_ID).setState(JobState.QUEUED).build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testLaunchNewJobThrowsException() throws IOException {
    when(getFlexTemplates(client).launch(any(), any(), any())).thenThrow(new IOException());
    assertThrows(
        IOException.class,
        () ->
            FlexTemplateClient.withDataflowClient(client)
                .launchTemplate(
                    PROJECT, REGION, LaunchOptions.builder(JOB_NAME, SPEC_PATH).build()));
  }

  private static Locations.Jobs getLocationJobs(Dataflow client) {
    return client.projects().locations().jobs();
  }

  private static FlexTemplates getFlexTemplates(Dataflow client) {
    return client.projects().locations().flexTemplates();
  }
}

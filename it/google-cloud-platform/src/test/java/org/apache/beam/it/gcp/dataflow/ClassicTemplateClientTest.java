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
package org.apache.beam.it.gcp.dataflow;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.LEGACY_RUNNER;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.PARAM_JOB_ID;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.PARAM_JOB_TYPE;
import static org.apache.beam.it.gcp.dataflow.AbstractPipelineLauncher.PARAM_RUNNER;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.Dataflow.Projects.Locations;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Jobs.Get;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Templates;
import com.google.api.services.dataflow.Dataflow.Projects.Locations.Templates.Create;
import com.google.api.services.dataflow.model.CreateJobFromTemplateRequest;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.JobMetadata;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.api.services.dataflow.model.SdkVersion;
import com.google.auth.Credentials;
import com.google.common.collect.ImmutableMap;
import dev.failsafe.FailsafeException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Collections;
import org.apache.beam.it.common.PipelineLauncher.JobState;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
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

/** Unit test for {@link ClassicTemplateClient}. */
@RunWith(JUnit4.class)
public final class ClassicTemplateClientTest {
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
  @Captor private ArgumentCaptor<CreateJobFromTemplateRequest> requestCaptor;

  @Test
  public void testCreateWithCredentials() {
    Credentials credentials = mock(Credentials.class);
    ClassicTemplateClient.builder(credentials).build();
    // Lack of exception is all we really can test
  }

  @Test
  public void testLaunchNewJob() throws IOException {
    // Arrange
    Create launch = mock(Create.class);
    Get get = mock(Get.class);
    Job launchJob = new Job().setId(JOB_ID);
    Job getJob =
        new Job()
            .setId(JOB_ID)
            .setProjectId(PROJECT)
            .setLocation(REGION)
            .setCurrentState(JobState.RUNNING.toString())
            .setCreateTime("")
            .setJobMetadata(
                new JobMetadata()
                    .setSdkVersion(
                        new SdkVersion()
                            .setVersionDisplayName("Apache Beam Java")
                            .setVersion("2.42.0")))
            .setType("JOB_TYPE_BATCH")
            .setEnvironment(new Environment().setExperiments(Collections.singletonList("")));

    LaunchConfig options =
        LaunchConfig.builderWithName(JOB_NAME, SPEC_PATH)
            .addParameter(PARAM_KEY, PARAM_VALUE)
            .build();

    when(getTemplates(client).create(any(), any(), any())).thenReturn(launch);
    when(getLocationJobs(client).get(any(), any(), any()).setView(any())).thenReturn(get);
    when(launch.execute())
        .thenThrow(new SocketTimeoutException("Read timed out"))
        .thenReturn(launchJob);
    when(get.execute()).thenReturn(getJob);

    // Act
    LaunchInfo actual =
        ClassicTemplateClient.withDataflowClient(client).launch(PROJECT, REGION, options);

    // Assert
    CreateJobFromTemplateRequest expectedRequest =
        new CreateJobFromTemplateRequest()
            .setJobName(JOB_NAME)
            .setGcsPath(SPEC_PATH)
            .setParameters(ImmutableMap.of(PARAM_KEY, PARAM_VALUE))
            .setLocation(REGION)
            .setEnvironment(new RuntimeEnvironment());
    verify(getTemplates(client), times(2))
        .create(projectCaptor.capture(), regionCaptor.capture(), requestCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(requestCaptor.getValue()).isEqualTo(expectedRequest);

    verify(getLocationJobs(client), times(3))
        .get(projectCaptor.capture(), regionCaptor.capture(), jobIdCaptor.capture());
    assertThat(projectCaptor.getValue()).isEqualTo(PROJECT);
    assertThat(regionCaptor.getValue()).isEqualTo(REGION);
    assertThat(jobIdCaptor.getValue()).isEqualTo(JOB_ID);

    LaunchInfo expected =
        LaunchInfo.builder()
            .setJobId(JOB_ID)
            .setProjectId(PROJECT)
            .setRegion(REGION)
            .setState(JobState.RUNNING)
            .setCreateTime("")
            .setSdk("Apache Beam Java")
            .setVersion("2.42.0")
            .setJobType("JOB_TYPE_BATCH")
            .setRunner(AbstractPipelineLauncher.LEGACY_RUNNER)
            .setParameters(
                ImmutableMap.<String, String>builder()
                    .put(PARAM_KEY, PARAM_VALUE)
                    .put(PARAM_JOB_ID, JOB_ID)
                    .put(PARAM_RUNNER, LEGACY_RUNNER)
                    .put(PARAM_JOB_TYPE, "JOB_TYPE_BATCH")
                    .build())
            .build();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testLaunchNewJobThrowsException() throws IOException {
    when(getTemplates(client).create(any(), any(), any())).thenThrow(new IOException());
    assertThrows(
        FailsafeException.class,
        () ->
            ClassicTemplateClient.withDataflowClient(client)
                .launch(PROJECT, REGION, LaunchConfig.builder(JOB_NAME, SPEC_PATH).build()));
  }

  private static Locations.Jobs getLocationJobs(Dataflow client) {
    return client.projects().locations().jobs();
  }

  private static Templates getTemplates(Dataflow client) {
    return client.projects().locations().templates();
  }
}

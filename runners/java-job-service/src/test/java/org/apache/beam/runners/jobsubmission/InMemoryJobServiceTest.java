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
package org.apache.beam.runners.jobsubmission;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.sdk.fn.server.GrpcFnServer;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.Struct;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.StatusException;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link InMemoryJobService}. */
@RunWith(JUnit4.class)
public class InMemoryJobServiceTest {

  private static final String TEST_JOB_NAME = "test-job";
  private static final String TEST_JOB_ID = "test-job-id";
  private static final String TEST_RETRIEVAL_TOKEN = "test-staging-token";
  private static final RunnerApi.Pipeline TEST_PIPELINE =
      RunnerApi.Pipeline.newBuilder()
          .setComponents(RunnerApi.Components.getDefaultInstance())
          .build();
  private static final Struct TEST_OPTIONS = Struct.getDefaultInstance();
  private static final JobApi.JobInfo TEST_JOB_INFO =
      JobApi.JobInfo.newBuilder()
          .setJobId(TEST_JOB_ID)
          .setJobName(TEST_JOB_NAME)
          .setPipelineOptions(TEST_OPTIONS)
          .build();

  Endpoints.ApiServiceDescriptor stagingServiceDescriptor;
  @Mock JobInvoker invoker;
  @Mock JobInvocation invocation;
  @Mock ArtifactStagingService stagingService;
  @Mock GrpcFnServer<ArtifactStagingService> stagingServer;

  InMemoryJobService service;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    stagingServiceDescriptor = Endpoints.ApiServiceDescriptor.getDefaultInstance();
    when(stagingServer.getApiServiceDescriptor()).thenReturn(stagingServiceDescriptor);
    when(stagingServer.getService()).thenReturn(stagingService);
    when(stagingService.getStagedArtifacts(any())).thenReturn(ImmutableMap.of());
    service = InMemoryJobService.create(stagingServer, session -> "token", null, invoker);
    when(invoker.invoke(TEST_PIPELINE, TEST_OPTIONS, TEST_RETRIEVAL_TOKEN)).thenReturn(invocation);
    when(invocation.getId()).thenReturn(TEST_JOB_ID);
    when(invocation.getPipeline()).thenReturn(TEST_PIPELINE);
    when(invocation.toProto()).thenReturn(TEST_JOB_INFO);
  }

  private JobApi.PrepareJobResponse prepareJob() {
    JobApi.PrepareJobRequest request =
        JobApi.PrepareJobRequest.newBuilder()
            .setJobName(TEST_JOB_NAME)
            .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
            .setPipelineOptions(Struct.getDefaultInstance())
            .build();
    RecordingObserver<JobApi.PrepareJobResponse> recorder = new RecordingObserver<>();
    service.prepare(request, recorder);
    return recorder.values.get(0);
  }

  private JobApi.RunJobResponse runJob(String preparationId) {
    JobApi.RunJobRequest runRequest =
        JobApi.RunJobRequest.newBuilder()
            .setPreparationId(preparationId)
            .setRetrievalToken(TEST_RETRIEVAL_TOKEN)
            .build();
    RecordingObserver<JobApi.RunJobResponse> recorder = new RecordingObserver<>();
    service.run(runRequest, recorder);
    return recorder.values.get(0);
  }

  private JobApi.RunJobResponse prepareAndRunJob() {
    JobApi.PrepareJobResponse prepareResponse = prepareJob();
    return runJob(prepareResponse.getPreparationId());
  }

  @Test
  public void testPrepareIsSuccessful() {
    JobApi.PrepareJobRequest request =
        JobApi.PrepareJobRequest.newBuilder()
            .setJobName(TEST_JOB_NAME)
            .setPipeline(RunnerApi.Pipeline.getDefaultInstance())
            .setPipelineOptions(Struct.getDefaultInstance())
            .build();
    RecordingObserver<JobApi.PrepareJobResponse> recorder = new RecordingObserver<>();
    service.prepare(request, recorder);
    assertThat(recorder.isSuccessful(), is(true));
    assertThat(recorder.values, hasSize(1));
    JobApi.PrepareJobResponse response = recorder.values.get(0);
    assertThat(response.getArtifactStagingEndpoint(), notNullValue());
    assertThat(response.getPreparationId(), notNullValue());
  }

  @Test
  public void testGetJobsIsSuccessful() throws Exception {
    prepareAndRunJob();

    JobApi.GetJobsRequest request = JobApi.GetJobsRequest.newBuilder().build();
    RecordingObserver<JobApi.GetJobsResponse> recorder = new RecordingObserver<>();
    service.getJobs(request, recorder);
    assertThat(recorder.isSuccessful(), is(true));
    assertThat(recorder.values, hasSize(1));
    JobApi.GetJobsResponse response = recorder.values.get(0);
    List<JobApi.JobInfo> jobs = response.getJobInfoList();
    assertThat(jobs, hasSize(1));
    JobApi.JobInfo job = jobs.get(0);
    assertThat(job.getJobId(), is(TEST_JOB_ID));
    assertThat(job.getJobName(), is(TEST_JOB_NAME));
  }

  @Test
  public void testGetPipelineFailure() {
    prepareJob();

    JobApi.GetJobPipelineRequest request =
        JobApi.GetJobPipelineRequest.newBuilder().setJobId(TEST_JOB_ID).build();
    RecordingObserver<JobApi.GetJobPipelineResponse> recorder = new RecordingObserver<>();
    service.getPipeline(request, recorder);
    // job has not been run yet
    assertThat(recorder.isSuccessful(), is(false));
    assertThat(recorder.error, isA(StatusException.class));
  }

  @Test
  public void testGetPipelineIsSuccessful() throws Exception {
    prepareAndRunJob();

    JobApi.GetJobPipelineRequest request =
        JobApi.GetJobPipelineRequest.newBuilder().setJobId(TEST_JOB_ID).build();
    RecordingObserver<JobApi.GetJobPipelineResponse> recorder = new RecordingObserver<>();
    service.getPipeline(request, recorder);
    assertThat(recorder.isSuccessful(), is(true));
    assertThat(recorder.values, hasSize(1));
    JobApi.GetJobPipelineResponse response = recorder.values.get(0);
    assertThat(response.getPipeline(), is(TEST_PIPELINE));
  }

  @Test
  public void testJobSubmissionUsesJobInvokerAndIsSuccess() throws Exception {
    JobApi.PrepareJobResponse prepareResponse = prepareJob();

    // run job
    JobApi.RunJobRequest runRequest =
        JobApi.RunJobRequest.newBuilder()
            .setPreparationId(prepareResponse.getPreparationId())
            .setRetrievalToken(TEST_RETRIEVAL_TOKEN)
            .build();
    RecordingObserver<JobApi.RunJobResponse> runRecorder = new RecordingObserver<>();
    service.run(runRequest, runRecorder);
    verify(invoker, times(1)).invoke(TEST_PIPELINE, TEST_OPTIONS, TEST_RETRIEVAL_TOKEN);
    assertThat(runRecorder.isSuccessful(), is(true));
    assertThat(runRecorder.values, hasSize(1));
    JobApi.RunJobResponse runResponse = runRecorder.values.get(0);
    assertThat(runResponse.getJobId(), is(TEST_JOB_ID));

    verify(invocation, times(1)).addStateListener(any());
    verify(invocation, times(1)).start();
  }

  @Test
  public void testInvocationCleanup() {
    final int maxInvocationHistory = 3;
    service =
        InMemoryJobService.create(
            stagingServer, session -> "token", null, invoker, maxInvocationHistory);

    assertThat(getNumberOfInvocations(), is(0));

    Job job1 = runJob();
    assertThat(getNumberOfInvocations(), is(1));
    Job job2 = runJob();
    assertThat(getNumberOfInvocations(), is(2));
    Job job3 = runJob();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory));

    // All running invocations must be available and never be discarded
    // even if they exceed the max history size
    Job job4 = runJob();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory + 1));

    // We need to have more than maxInvocationHistory completed jobs for the cleanup to trigger
    job1.finish();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory + 1));
    job2.finish();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory + 1));
    job3.finish();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory + 1));

    // The fourth finished job exceeds maxInvocationHistory and triggers the cleanup
    job4.finish();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory));

    // Run a new job after the cleanup
    Job job5 = runJob();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory + 1));
    job5.finish();
    assertThat(getNumberOfInvocations(), is(maxInvocationHistory));
  }

  private Job runJob() {
    when(invocation.getId()).thenReturn(UUID.randomUUID().toString());
    prepareAndRunJob();
    // Retrieve the state listener for this invocation
    ArgumentCaptor<Consumer<JobApi.JobStateEvent>> stateListener =
        ArgumentCaptor.forClass(Consumer.class);
    verify(invocation, atLeastOnce()).addStateListener(stateListener.capture());
    return new Job(stateListener.getValue());
  }

  private int getNumberOfInvocations() {
    RecordingObserver<JobApi.GetJobsResponse> recorder = new RecordingObserver<>();
    final JobApi.GetJobsRequest getJobsRequest = JobApi.GetJobsRequest.newBuilder().build();
    service.getJobs(getJobsRequest, recorder);
    return recorder.getValue().getJobInfoCount();
  }

  private static class Job {
    private Consumer<JobApi.JobStateEvent> stateListener;

    private Job(Consumer<JobApi.JobStateEvent> stateListener) {
      this.stateListener = stateListener;
    }

    void finish() {
      JobApi.JobStateEvent terminalEvent =
          JobApi.JobStateEvent.newBuilder().setState(JobApi.JobState.Enum.DONE).build();
      stateListener.accept(terminalEvent);
    }
  }

  private static class RecordingObserver<T> implements StreamObserver<T> {

    ArrayList<T> values = new ArrayList<>();
    Throwable error = null;
    boolean isCompleted = false;

    @Override
    public void onNext(T t) {
      values.add(t);
    }

    @Override
    public void onError(Throwable throwable) {
      error = throwable;
    }

    @Override
    public void onCompleted() {
      isCompleted = true;
    }

    T getValue() {
      assert values.size() == 1;
      return values.get(0);
    }

    boolean isSuccessful() {
      return isCompleted && error == null;
    }
  }
}

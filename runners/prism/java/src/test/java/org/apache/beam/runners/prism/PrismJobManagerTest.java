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
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.Optional;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessServerBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PrismJobManager}. */
@RunWith(JUnit4.class)
public class PrismJobManagerTest {
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @Rule public TestName testName = new TestName();

  @Test
  public void givenPrepareError_forwardsException_canGracefulShutdown() {
    TestJobService service =
        new TestJobService().withErrorResponse(new RuntimeException(testName.getMethodName()));
    PrismJobManager underTest = prismJobManager(service);
    assertThat(underTest.isShutdown()).isFalse();
    assertThrows(
        RuntimeException.class,
        () ->
            underTest.prepare(
                JobApi.PrepareJobRequest.newBuilder().setPipeline(pipelineOf()).build()));
    assertThat(underTest.isShutdown()).isFalse();
    underTest.close();
    assertThat(underTest.isShutdown()).isTrue();
  }

  @Test
  public void givenPrepareSuccess_forwardsResponse_canGracefulShutdown() {
    TestJobService service =
        new TestJobService()
            .withPrepareJobResponse(
                JobApi.PrepareJobResponse.newBuilder()
                    .setStagingSessionToken("token")
                    .setPreparationId("preparationId")
                    .setArtifactStagingEndpoint(
                        Endpoints.ApiServiceDescriptor.newBuilder()
                            .setUrl("localhost:1234")
                            .build())
                    .build());
    PrismJobManager underTest = prismJobManager(service);
    assertThat(underTest.isShutdown()).isFalse();
    JobApi.PrepareJobResponse response =
        underTest.prepare(JobApi.PrepareJobRequest.newBuilder().setPipeline(pipelineOf()).build());
    assertThat(underTest.isShutdown()).isFalse();
    assertThat(response.getStagingSessionToken()).isEqualTo("token");
    assertThat(response.getPreparationId()).isEqualTo("preparationId");
    underTest.close();
    assertThat(underTest.isShutdown()).isTrue();
  }

  @Test
  public void givenRunError_forwardsException_canGracefulShutdown() {
    TestJobService service =
        new TestJobService().withErrorResponse(new RuntimeException(testName.getMethodName()));
    PrismJobManager underTest = prismJobManager(service);
    assertThat(underTest.isShutdown()).isFalse();
    assertThrows(
        RuntimeException.class,
        () ->
            underTest.run(JobApi.RunJobRequest.newBuilder().setPreparationId("prepareId").build()));
    assertThat(underTest.isShutdown()).isFalse();
    underTest.close();
    assertThat(underTest.isShutdown()).isTrue();
  }

  @Test
  public void givenRunSuccess_forwardsResponse_canGracefulShutdown() {
    TestJobService service =
        new TestJobService()
            .withRunJobResponse(JobApi.RunJobResponse.newBuilder().setJobId("jobId").build());
    PrismJobManager underTest = prismJobManager(service);
    assertThat(underTest.isShutdown()).isFalse();
    JobApi.RunJobResponse runJobResponse =
        underTest.run(JobApi.RunJobRequest.newBuilder().setPreparationId("preparationId").build());
    assertThat(underTest.isShutdown()).isFalse();
    assertThat(runJobResponse.getJobId()).isEqualTo("jobId");
    underTest.close();
    assertThat(underTest.isShutdown()).isTrue();
  }

  @Test
  public void givenTerminalState_closes() {
    PrismJobManager underTest = prismJobManager(new TestJobService());
    assertThat(underTest.isShutdown()).isFalse();
    underTest.onStateChanged(PipelineResult.State.RUNNING);
    assertThat(underTest.isShutdown()).isFalse();
    underTest.onStateChanged(PipelineResult.State.RUNNING);
    assertThat(underTest.isShutdown()).isFalse();
    underTest.onStateChanged(PipelineResult.State.CANCELLED);
    assertThat(underTest.isShutdown()).isTrue();

    underTest.close();
  }

  private PrismJobManager prismJobManager(TestJobService service) {
    String serverName = InProcessServerBuilder.generateName();
    try {
      grpcCleanup.register(
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(service)
              .build()
              .start());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ManagedChannel channel =
        grpcCleanup.register(InProcessChannelBuilder.forName(serverName).build());

    return PrismJobManager.builder()
        .setTimeout(Duration.millis(3000L))
        .setEndpoint("ignore")
        .setManagedChannel(channel)
        .build();
  }

  private static class TestJobService extends JobServiceGrpc.JobServiceImplBase {

    private Optional<JobApi.PrepareJobResponse> prepareJobResponse = Optional.empty();
    private Optional<JobApi.RunJobResponse> runJobResponse = Optional.empty();
    private Optional<RuntimeException> error = Optional.empty();

    TestJobService withPrepareJobResponse(JobApi.PrepareJobResponse prepareJobResponse) {
      this.prepareJobResponse = Optional.of(prepareJobResponse);
      return this;
    }

    TestJobService withRunJobResponse(JobApi.RunJobResponse runJobResponse) {
      this.runJobResponse = Optional.of(runJobResponse);
      return this;
    }

    TestJobService withErrorResponse(RuntimeException error) {
      this.error = Optional.of(error);
      return this;
    }

    @Override
    public void prepare(
        JobApi.PrepareJobRequest request,
        StreamObserver<JobApi.PrepareJobResponse> responseObserver) {
      if (prepareJobResponse.isPresent()) {
        responseObserver.onNext(prepareJobResponse.get());
        responseObserver.onCompleted();
      }
      if (error.isPresent()) {
        responseObserver.onError(error.get());
      }
    }

    @Override
    public void run(
        JobApi.RunJobRequest request, StreamObserver<JobApi.RunJobResponse> responseObserver) {
      if (runJobResponse.isPresent()) {
        responseObserver.onNext(runJobResponse.get());
        responseObserver.onCompleted();
      }
      if (error.isPresent()) {
        responseObserver.onError(error.get());
      }
    }
  }

  private static RunnerApi.Pipeline pipelineOf() {
    Pipeline pipeline = Pipeline.create();
    pipeline.apply(Impulse.create());
    return PipelineTranslation.toProto(pipeline);
  }
}

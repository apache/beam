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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.ArtifactStagingServiceGrpc;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.artifact.ArtifactStagingService;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stages {@link org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline} artifacts of prepared jobs.
 */
@AutoValue
abstract class PrismArtifactStager implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PrismArtifactStager.class);

  /**
   * Instantiate a {@link PrismArtifactStager} via call to {@link #of(String, String)}, assigning
   * {@link Builder#setStagingEndpoint} using {@param prepareJobResponse} {@link
   * JobApi.PrepareJobResponse#getArtifactStagingEndpoint} and {@link
   * JobApi.PrepareJobResponse#getStagingSessionToken}.
   */
  static PrismArtifactStager of(JobApi.PrepareJobResponse prepareJobResponse) {
    return of(
        prepareJobResponse.getArtifactStagingEndpoint().getUrl(),
        prepareJobResponse.getStagingSessionToken());
  }

  /**
   * Instantiates a {@link PrismArtifactStager} from the {@param stagingEndpoint} URL and {@param
   * stagingSessionToken} to instantiate the {@link #getRetrievalService}, {@link
   * #getManagedChannel}, and {@link #getStagingServiceStub} defaults. See the referenced getters
   * for more details.
   */
  static PrismArtifactStager of(String stagingEndpoint, String stagingSessionToken) {
    return PrismArtifactStager.builder()
        .setStagingEndpoint(stagingEndpoint)
        .setStagingSessionToken(stagingSessionToken)
        .build();
  }

  static Builder builder() {
    return new AutoValue_PrismArtifactStager.Builder();
  }

  /**
   * Stage the {@link org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline} artifacts via {@link
   * ArtifactStagingService#offer} supplying {@link #getRetrievalService}, {@link
   * #getStagingServiceStub}, and {@link #getStagingSessionToken}.
   */
  void stage() throws ExecutionException, InterruptedException {
    LOG.info("staging artifacts at {}", getStagingEndpoint());
    ArtifactStagingService.offer(
        getRetrievalService(), getStagingServiceStub(), getStagingSessionToken());
  }

  /** The URL of the {@link ArtifactStagingService}. */
  abstract String getStagingEndpoint();

  /**
   * Token associated with a staging session and acquired from a {@link
   * JobServiceGrpc.JobServiceStub#prepare}'s {@link JobApi.PrepareJobResponse}.
   */
  abstract String getStagingSessionToken();

  /**
   * The service that retrieves artifacts; defaults to instantiating from the default {@link
   * ArtifactRetrievalService#ArtifactRetrievalService()} constructor.
   */
  abstract ArtifactRetrievalService getRetrievalService();

  /**
   * Used to instantiate the {@link #getStagingServiceStub}. By default, instantiates using {@link
   * ManagedChannelFactory#forDescriptor(Endpoints.ApiServiceDescriptor)}, where {@link
   * Endpoints.ApiServiceDescriptor} is instantiated via {@link
   * Endpoints.ApiServiceDescriptor.Builder#setUrl(String)} and the URL provided by {@link
   * #getStagingEndpoint}.
   */
  abstract ManagedChannel getManagedChannel();

  /**
   * Required by {@link ArtifactStagingService#offer}. By default, instantiates using {@link
   * ArtifactStagingServiceGrpc#newStub} and {@link #getManagedChannel}.
   */
  abstract ArtifactStagingServiceGrpc.ArtifactStagingServiceStub getStagingServiceStub();

  @Override
  public void close() {
    LOG.info("shutting down {}", PrismArtifactStager.class);
    getRetrievalService().close();
    getManagedChannel().shutdown();
    try {
      getManagedChannel().awaitTermination(3000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setStagingEndpoint(String stagingEndpoint);

    abstract Optional<String> getStagingEndpoint();

    abstract Builder setStagingSessionToken(String stagingSessionToken);

    abstract Builder setRetrievalService(ArtifactRetrievalService retrievalService);

    abstract Optional<ArtifactRetrievalService> getRetrievalService();

    abstract Builder setManagedChannel(ManagedChannel managedChannel);

    abstract Optional<ManagedChannel> getManagedChannel();

    abstract Builder setStagingServiceStub(
        ArtifactStagingServiceGrpc.ArtifactStagingServiceStub stub);

    abstract Optional<ArtifactStagingServiceGrpc.ArtifactStagingServiceStub>
        getStagingServiceStub();

    abstract PrismArtifactStager autoBuild();

    final PrismArtifactStager build() {

      checkState(getStagingEndpoint().isPresent(), "missing staging endpoint");
      ManagedChannelFactory channelFactory = ManagedChannelFactory.createDefault();

      if (!getManagedChannel().isPresent()) {
        Endpoints.ApiServiceDescriptor descriptor =
            Endpoints.ApiServiceDescriptor.newBuilder().setUrl(getStagingEndpoint().get()).build();
        setManagedChannel(channelFactory.forDescriptor(descriptor));
      }

      if (!getStagingServiceStub().isPresent()) {
        setStagingServiceStub(ArtifactStagingServiceGrpc.newStub(getManagedChannel().get()));
      }

      if (!getRetrievalService().isPresent()) {
        setRetrievalService(new ArtifactRetrievalService());
      }

      return autoBuild();
    }
  }
}

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
import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.model.pipeline.v1.Endpoints;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.fn.channel.ManagedChannelFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.joda.time.Duration;

/**
 * A wrapper for {@link JobServiceGrpc.JobServiceBlockingStub} that {@link #close}es when {@link
 * StateListener#onStateChanged} is invoked with a {@link PipelineResult.State} that is {@link
 * PipelineResult.State#isTerminal}.
 */
@AutoValue
abstract class PrismJobManager implements StateListener, Closeable {

  /**
   * Instantiate a {@link PrismJobManager} with {@param options}, assigning {@link #getEndpoint}
   * from {@link PortablePipelineOptions#getJobEndpoint} and {@link #getTimeout} from {@link
   * PortablePipelineOptions#getJobServerTimeout}. Defaults the instantiations of {@link
   * #getManagedChannel} and {@link #getBlockingStub}. See respective getters for more details.
   */
  static PrismJobManager of(PortablePipelineOptions options) {
    return builder()
        .setEndpoint(options.getJobEndpoint())
        .setTimeout(Duration.standardSeconds(options.getJobServerTimeout()))
        .build();
  }

  static Builder builder() {
    return new AutoValue_PrismJobManager.Builder();
  }

  /**
   * Executes {@link #getBlockingStub()}'s {@link JobServiceGrpc.JobServiceBlockingStub#prepare}
   * method.
   */
  JobApi.PrepareJobResponse prepare(JobApi.PrepareJobRequest request) {
    return getBlockingStub().prepare(request);
  }

  /**
   * Executes {@link #getBlockingStub()}'s {@link JobServiceGrpc.JobServiceBlockingStub#run} method.
   */
  JobApi.RunJobResponse run(JobApi.RunJobRequest request) {
    return getBlockingStub().run(request);
  }

  /** The {@link JobServiceGrpc} endpoint. */
  abstract String getEndpoint();

  /** The {@link JobServiceGrpc} timeout. */
  abstract Duration getTimeout();

  /** The {@link #getBlockingStub}'s channel. Defaulted from the {@link #getEndpoint()}. */
  abstract ManagedChannel getManagedChannel();

  /** The wrapped service defaulted using the {@link #getManagedChannel}. */
  abstract JobServiceGrpc.JobServiceBlockingStub getBlockingStub();

  /** Shuts down {@link #getManagedChannel}, if not {@link #isShutdown}. */
  @Override
  public void close() {
    if (isShutdown()) {
      return;
    }
    getManagedChannel().shutdown();
    try {
      getManagedChannel().awaitTermination(3000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  /** Queries whether {@link #getManagedChannel} {@link ManagedChannel#isShutdown}. */
  boolean isShutdown() {
    return getManagedChannel().isShutdown();
  }

  /**
   * Override of {@link StateListener#onStateChanged}. Invokes {@link #close} when {@link
   * PipelineResult.State} {@link PipelineResult.State#isTerminal}.
   */
  @Override
  public void onStateChanged(PipelineResult.State state) {
    if (state.isTerminal()) {
      close();
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setEndpoint(String endpoint);

    abstract Optional<String> getEndpoint();

    abstract Builder setTimeout(Duration timeout);

    abstract Optional<Duration> getTimeout();

    abstract Builder setManagedChannel(ManagedChannel managedChannel);

    abstract Optional<ManagedChannel> getManagedChannel();

    abstract Builder setBlockingStub(JobServiceGrpc.JobServiceBlockingStub blockingStub);

    abstract Optional<JobServiceGrpc.JobServiceBlockingStub> getBlockingStub();

    abstract PrismJobManager autoBuild();

    final PrismJobManager build() {

      checkState(getEndpoint().isPresent(), "endpoint is not set");
      checkState(getTimeout().isPresent(), "timeout is not set");

      if (!getManagedChannel().isPresent()) {
        ManagedChannelFactory channelFactory = ManagedChannelFactory.createDefault();

        setManagedChannel(
            channelFactory.forDescriptor(
                Endpoints.ApiServiceDescriptor.newBuilder().setUrl(getEndpoint().get()).build()));
      }

      if (!getBlockingStub().isPresent()) {
        setBlockingStub(
            JobServiceGrpc.newBlockingStub(getManagedChannel().get())
                .withDeadlineAfter(getTimeout().get().getMillis(), TimeUnit.MILLISECONDS)
                .withWaitForReady());
      }

      return autoBuild();
    }
  }
}

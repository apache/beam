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

import com.google.auto.value.AutoValue;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ChannelCredentials;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.InsecureChannelCredentials;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.netty.NettyChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;

/**
 * {@link StateWatcher} {@link #watch}es for and reports {@link PipelineResult.State} changes to
 * {@link StateListener}s.
 */
@AutoValue
abstract class StateWatcher implements AutoCloseable {

  private Optional<PipelineResult.State> latestState = Optional.empty();

  /**
   * Instantiates a {@link StateWatcher} with {@link InsecureChannelCredentials}. {@link
   * StateWatcher} will report to each {@link StateListener} of {@param listeners} of any changed
   * {@link PipelineResult.State}.
   */
  static StateWatcher insecure(String endpoint, StateListener... listeners) {
    return StateWatcher.builder()
        .setEndpoint(HostAndPort.fromString(endpoint))
        .setCredentials(InsecureChannelCredentials.create())
        .setListeners(Arrays.asList(listeners))
        .build();
  }

  /**
   * Watch for a Job's {@link PipelineResult.State} change. A {@link
   * org.apache.beam.model.jobmanagement.v1.JobApi.GetJobStateRequest} identifies a Job to watch via
   * its {@link JobApi.GetJobStateRequest#getJobId()}. The method is blocking until the {@link
   * JobApi.JobStateEvent} {@link StreamObserver#onCompleted()}.
   */
  void watch(String jobId) {
    JobApi.GetJobStateRequest request =
        JobApi.GetJobStateRequest.newBuilder().setJobId(jobId).build();
    Iterator<JobApi.JobStateEvent> iterator = getJobServiceBlockingStub().getStateStream(request);
    while (iterator.hasNext()) {
      JobApi.JobStateEvent event = iterator.next();
      PipelineResult.State state = PipelineResult.State.valueOf(event.getState().name());
      publish(state);
    }
  }

  private void publish(PipelineResult.State state) {
    if (latestState.isPresent() && latestState.get().equals(state)) {
      return;
    }
    latestState = Optional.of(state);
    for (StateListener listener : getListeners()) {
      listener.onStateChanged(state);
    }
  }

  static Builder builder() {
    return new AutoValue_StateWatcher.Builder();
  }

  abstract HostAndPort getEndpoint();

  abstract ChannelCredentials getCredentials();

  abstract List<StateListener> getListeners();

  abstract ManagedChannel getManagedChannel();

  abstract JobServiceGrpc.JobServiceBlockingStub getJobServiceBlockingStub();

  @Override
  public void close() {
    getManagedChannel().shutdown();
    try {
      getManagedChannel().awaitTermination(3000L, TimeUnit.MILLISECONDS);
    } catch (InterruptedException ignored) {
    }
  }

  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setEndpoint(HostAndPort endpoint);

    abstract Optional<HostAndPort> getEndpoint();

    abstract Builder setCredentials(ChannelCredentials credentials);

    abstract Optional<ChannelCredentials> getCredentials();

    abstract Builder setListeners(List<StateListener> listeners);

    abstract Builder setManagedChannel(ManagedChannel managedChannel);

    abstract Builder setJobServiceBlockingStub(
        JobServiceGrpc.JobServiceBlockingStub jobServiceBlockingStub);

    abstract StateWatcher autoBuild();

    final StateWatcher build() {
      if (!getEndpoint().isPresent()) {
        throw new IllegalStateException("missing endpoint");
      }
      if (!getCredentials().isPresent()) {
        throw new IllegalStateException("missing credentials");
      }
      HostAndPort endpoint = getEndpoint().get();
      ManagedChannel channel =
          NettyChannelBuilder.forAddress(
                  endpoint.getHost(), endpoint.getPort(), getCredentials().get())
              .build();
      setManagedChannel(channel);
      setJobServiceBlockingStub(JobServiceGrpc.newBlockingStub(channel));

      return autoBuild();
    }
  }
}

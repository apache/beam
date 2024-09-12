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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.jobmanagement.v1.JobServiceGrpc;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Grpc;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.InsecureServerCredentials;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.Server;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.stub.StreamObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StateWatcherTest {

  @Test
  public void givenSingleListener_watches() {
    Server server = serverOf(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
    TestStateListener listener = new TestStateListener();
    try (StateWatcher underTest = StateWatcher.insecure("0.0.0.0:" + server.getPort(), listener)) {
      underTest.watch("job-001");
      assertThat(listener.states)
          .containsExactly(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
      shutdown(server);
    }
  }

  @Test
  public void givenMultipleListeners_watches() {
    Server server = serverOf(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
    TestStateListener listenerA = new TestStateListener();
    TestStateListener listenerB = new TestStateListener();
    try (StateWatcher underTest =
        StateWatcher.insecure("0.0.0.0:" + server.getPort(), listenerA, listenerB)) {
      underTest.watch("job-001");
      assertThat(listenerA.states)
          .containsExactly(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
      assertThat(listenerB.states)
          .containsExactly(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
      shutdown(server);
    }
  }

  @Test
  public void publishesOnlyChangedState() {
    Server server =
        serverOf(
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.RUNNING,
            PipelineResult.State.DONE);
    TestStateListener listener = new TestStateListener();
    try (StateWatcher underTest = StateWatcher.insecure("0.0.0.0:" + server.getPort(), listener)) {
      underTest.watch("job-001");
      assertThat(listener.states)
          .containsExactly(PipelineResult.State.RUNNING, PipelineResult.State.DONE);
      shutdown(server);
    }
  }

  private static class TestStateListener implements StateListener {
    private final List<PipelineResult.State> states = new ArrayList<>();

    @Override
    public void onStateChanged(PipelineResult.State state) {
      states.add(state);
    }
  }

  private static class TestJobServiceStateStream extends JobServiceGrpc.JobServiceImplBase {
    private final List<PipelineResult.State> states;

    TestJobServiceStateStream(PipelineResult.State... states) {
      this.states = Arrays.asList(states);
    }

    @Override
    public void getStateStream(
        JobApi.GetJobStateRequest request, StreamObserver<JobApi.JobStateEvent> responseObserver) {
      for (PipelineResult.State state : states) {
        responseObserver.onNext(
            JobApi.JobStateEvent.newBuilder()
                .setState(JobApi.JobState.Enum.valueOf(state.name()))
                .build());
      }
      responseObserver.onCompleted();
    }
  }

  private static Server serverOf(PipelineResult.State... states) {
    try {
      return Grpc.newServerBuilderForPort(0, InsecureServerCredentials.create())
          .addService(new TestJobServiceStateStream(states))
          .build()
          .start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void shutdown(Server server) {
    server.shutdownNow();
    try {
      server.awaitTermination();
    } catch (InterruptedException ignored) {
    }
  }
}

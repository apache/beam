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
package org.apache.beam.runners.dataflow.worker.streaming.harness;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.GetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.processing.StreamingWorkScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillStreamPoolSenderTest {
  private static final long ITEM_BUDGET = 1L;
  private static final long BYTE_BUDGET = 1L;

  private static final GetWorkRequest GET_WORK_REQUEST =
      GetWorkRequest.newBuilder().setClientId(1L).setJobId("job").setProjectId("project").build();
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private final GrpcWindmillStreamFactory streamFactory =
      spy(
          GrpcWindmillStreamFactory.of(
                  JobHeader.newBuilder()
                      .setJobId("job")
                      .setProjectId("project")
                      .setWorkerId("worker")
                      .build())
              .build());
  private ManagedChannel inProcessChannel;
  private WindmillConnection connection;
  private final GetDataClient mockGetDataClient = mock(GetDataClient.class);
  private final HeartbeatSender mockHeartbeatSender = mock(HeartbeatSender.class);
  private final StreamingWorkScheduler mockStreamingWorkScheduler =
      mock(StreamingWorkScheduler.class);
  private final Runnable mockWaitForResources = mock(Runnable.class);
  private final Function<String, Optional<ComputationState>> mockComputationStateFetcher =
      mock(Function.class);
  private final WorkCommitter mockWorkCommitter = mock(WorkCommitter.class);

  @Before
  public void setUp() {
    inProcessChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName("WindmillStreamPoolSenderTest")
                .directExecutor()
                .build());
    grpcCleanup.register(inProcessChannel);
    connection =
        WindmillConnection.builder()
            .setStubSupplier(() -> CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel))
            .build();
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testStartStream_startsGetWorkStreamAndCommitter() {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(BYTE_BUDGET).setItems(ITEM_BUDGET).build());

    windmillStreamPoolSender.start();
    waitForDispatchLoopToStart(windmillStreamPoolSender);

    verify(streamFactory)
        .createGetWorkStream(
            any(CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub.class),
            eq(GET_WORK_REQUEST),
            any());
    verify(mockWorkCommitter).start();
  }

  @Test
  public void testStartStream_onlyStartsStreamsOnce() {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(BYTE_BUDGET).setItems(ITEM_BUDGET).build());

    windmillStreamPoolSender.start();
    windmillStreamPoolSender.start();
    windmillStreamPoolSender.start();
    waitForDispatchLoopToStart(windmillStreamPoolSender);

    verify(streamFactory, times(1))
        .createGetWorkStream(
            any(CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub.class),
            eq(GET_WORK_REQUEST),
            any());
    verify(mockWorkCommitter, times(1)).start();
  }

  @Test
  public void testCloseAllStreams_closesGetWorkStreamAndCommitter() {
    GrpcWindmillStreamFactory mockStreamFactory = mock(GrpcWindmillStreamFactory.class);
    GetWorkStream mockGetWorkStream = mock(GetWorkStream.class);

    when(mockStreamFactory.createGetWorkStream(
            any(CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub.class),
            any(GetWorkRequest.class),
            any()))
        .thenReturn(mockGetWorkStream);

    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(1L).setItems(1L).build(), mockStreamFactory);

    windmillStreamPoolSender.start();
    waitForDispatchLoopToStart(windmillStreamPoolSender);
    windmillStreamPoolSender.close();

    verify(mockGetWorkStream).shutdown();
    verify(mockWorkCommitter).stop();
  }

  private WindmillStreamPoolSender newWindmillStreamPoolSender(GetWorkBudget budget) {
    return newWindmillStreamPoolSender(budget, streamFactory);
  }

  private WindmillStreamPoolSender newWindmillStreamPoolSender(
      GetWorkBudget budget, GrpcWindmillStreamFactory streamFactory) {
    return WindmillStreamPoolSender.create(
        connection,
        GET_WORK_REQUEST,
        budget,
        streamFactory,
        mockWorkCommitter,
        mockGetDataClient,
        mockHeartbeatSender,
        mockStreamingWorkScheduler,
        mockWaitForResources,
        mockComputationStateFetcher);
  }

  private void waitForDispatchLoopToStart(WindmillStreamPoolSender windmillStreamSender) {
    while (true) {
      if (windmillStreamSender.hasGetWorkStreamStarted()) {
        break;
      }
      try {
        Thread.sleep(1000); // Wait for 1 second
      } catch (InterruptedException e) {
        break;
      }
    }
  }
}

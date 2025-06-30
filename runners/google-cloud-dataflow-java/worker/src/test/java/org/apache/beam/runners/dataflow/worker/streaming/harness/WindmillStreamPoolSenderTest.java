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

import static org.junit.Assert.assertEquals;
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
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemReceiver;
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
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class WindmillStreamPoolSenderTest {
  private static final long ITEM_BUDGET = 1L;
  private static final long BYTE_BUDGET = 1L;

  private static final GetWorkRequest GET_WORK_REQUEST =
      GetWorkRequest.newBuilder().setClientId(1L).setJobId("job").setProjectId("project").build();

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private GrpcWindmillStreamFactory mockStreamFactory;
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
  private GetWorkStream mockGetWorkStream = mock(GetWorkStream.class);

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

    mockStreamFactory =
        spy(
            GrpcWindmillStreamFactory.of(
                    JobHeader.newBuilder()
                        .setJobId("job")
                        .setProjectId("project")
                        .setWorkerId("worker")
                        .build())
                .build());

    when(mockStreamFactory.createGetWorkStream(
            any(CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub.class),
            any(GetWorkRequest.class),
            any(WorkItemReceiver.class)))
        .thenReturn(mockGetWorkStream);
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testStartStream_startsGetWorkStreamAndCommitter() throws Exception {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(BYTE_BUDGET).setItems(ITEM_BUDGET).build());

    windmillStreamPoolSender.start();

    verify(mockStreamFactory, timeout(1000).atLeastOnce())
        .createGetWorkStream(
            any(CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub.class),
            eq(GET_WORK_REQUEST),
            any(WorkItemReceiver.class));
    verify(mockGetWorkStream, timeout(1000).atLeastOnce()).start();
    verify(mockWorkCommitter, timeout(1000).atLeastOnce()).start();
    windmillStreamPoolSender.close();
  }

  @Test
  public void testCloseAllStreams_closesCommitter() throws Exception {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(GetWorkBudget.builder().setBytes(1L).setItems(1L).build());

    windmillStreamPoolSender.start();
    verify(mockGetWorkStream, timeout(1000).atLeastOnce()).start();

    windmillStreamPoolSender.close();

    verify(mockWorkCommitter, times(1)).stop();
  }

  @Test
  public void testSetBudgetUpdatesActiveStream() throws Exception {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(BYTE_BUDGET).setItems(ITEM_BUDGET).build());

    windmillStreamPoolSender.start();
    verify(mockGetWorkStream, timeout(1000).atLeastOnce()).start();

    long newItems = 200;
    long newBytes = 2000;
    windmillStreamPoolSender.setBudget(newItems, newBytes);
    ArgumentCaptor<GetWorkBudget> budgetCaptor = ArgumentCaptor.forClass(GetWorkBudget.class);
    verify(mockGetWorkStream, times(1)).setBudget(budgetCaptor.capture());

    GetWorkBudget capturedBudget = budgetCaptor.getValue();
    assertEquals(newItems, capturedBudget.items());
    assertEquals(newBytes, capturedBudget.bytes());
  }

  @Test
  public void testSetBudgetDoesNothingIfNoActiveStream() {
    WindmillStreamPoolSender windmillStreamPoolSender =
        newWindmillStreamPoolSender(
            GetWorkBudget.builder().setBytes(BYTE_BUDGET).setItems(ITEM_BUDGET).build());

    windmillStreamPoolSender.setBudget(200, 2000);

    verify(mockGetWorkStream, times(0)).setBudget(any(GetWorkBudget.class));
  }

  private WindmillStreamPoolSender newWindmillStreamPoolSender(GetWorkBudget budget) {
    return WindmillStreamPoolSender.create(
        connection,
        GET_WORK_REQUEST,
        budget,
        mockStreamFactory,
        mockWorkCommitter,
        mockGetDataClient,
        mockHeartbeatSender,
        mockStreamingWorkScheduler,
        mockWaitForResources,
        mockComputationStateFetcher);
  }
}

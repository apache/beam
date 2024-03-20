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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.GetWorkRequest;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.throttling.ThrottleTimer;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemProcessor;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindmillStreamSenderTest {
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
  private final WorkItemProcessor workItemProcessor =
      (computation,
          inputDataWatermark,
          synchronizedProcessingTime,
          workItem,
          ackQueuedWorkItem,
          getWorkStreamLatencies) -> {};
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private ManagedChannel inProcessChannel;
  private CloudWindmillServiceV1Alpha1Stub stub;

  @Before
  public void setUp() {
    inProcessChannel =
        grpcCleanup.register(
            InProcessChannelBuilder.forName("WindmillStreamSenderTest").directExecutor().build());
    grpcCleanup.register(inProcessChannel);
    stub = CloudWindmillServiceV1Alpha1Grpc.newStub(inProcessChannel);
  }

  @After
  public void cleanUp() {
    inProcessChannel.shutdownNow();
  }

  @Test
  public void testStartStream_startsAllStreams() {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    windmillStreamSender.startStreams();

    verify(streamFactory)
        .createDirectGetWorkStream(
            eq(stub),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(ThrottleTimer.class),
            any(),
            any(),
            eq(workItemProcessor));

    verify(streamFactory).createGetDataStream(eq(stub), any(ThrottleTimer.class));
    verify(streamFactory).createCommitWorkStream(eq(stub), any(ThrottleTimer.class));
  }

  @Test
  public void testStartStream_onlyStartsStreamsOnce() {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    windmillStreamSender.startStreams();
    windmillStreamSender.startStreams();
    windmillStreamSender.startStreams();

    verify(streamFactory, times(1))
        .createDirectGetWorkStream(
            eq(stub),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(ThrottleTimer.class),
            any(),
            any(),
            eq(workItemProcessor));

    verify(streamFactory, times(1)).createGetDataStream(eq(stub), any(ThrottleTimer.class));
    verify(streamFactory, times(1)).createCommitWorkStream(eq(stub), any(ThrottleTimer.class));
  }

  @Test
  public void testStartStream_onlyStartsStreamsOnceConcurrent() throws InterruptedException {
    long itemBudget = 1L;
    long byteBudget = 1L;

    WindmillStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build());

    Thread startStreamThread = new Thread(windmillStreamSender::startStreams);
    startStreamThread.start();

    windmillStreamSender.startStreams();

    startStreamThread.join();

    verify(streamFactory, times(1))
        .createDirectGetWorkStream(
            eq(stub),
            eq(
                GET_WORK_REQUEST
                    .toBuilder()
                    .setMaxItems(itemBudget)
                    .setMaxBytes(byteBudget)
                    .build()),
            any(ThrottleTimer.class),
            any(),
            any(),
            eq(workItemProcessor));

    verify(streamFactory, times(1)).createGetDataStream(eq(stub), any(ThrottleTimer.class));
    verify(streamFactory, times(1)).createCommitWorkStream(eq(stub), any(ThrottleTimer.class));
  }

  @Test
  public void testCloseAllStreams_doesNotCloseUnstartedStreams() {
    WindmillStreamSender windmillStreamSender =
        newWindmillStreamSender(GetWorkBudget.builder().setBytes(1L).setItems(1L).build());

    windmillStreamSender.closeAllStreams();

    verifyNoInteractions(streamFactory);
  }

  @Test
  public void testCloseAllStreams_closesAllStreams() {
    long itemBudget = 1L;
    long byteBudget = 1L;
    GetWorkRequest getWorkRequestWithBudget =
        GET_WORK_REQUEST.toBuilder().setMaxItems(itemBudget).setMaxBytes(byteBudget).build();
    GrpcWindmillStreamFactory mockStreamFactory = mock(GrpcWindmillStreamFactory.class);
    GetWorkStream mockGetWorkStream = mock(GetWorkStream.class);
    GetDataStream mockGetDataStream = mock(GetDataStream.class);
    CommitWorkStream mockCommitWorkStream = mock(CommitWorkStream.class);

    when(mockStreamFactory.createDirectGetWorkStream(
            eq(stub),
            eq(getWorkRequestWithBudget),
            any(ThrottleTimer.class),
            any(),
            any(),
            eq(workItemProcessor)))
        .thenReturn(mockGetWorkStream);

    when(mockStreamFactory.createGetDataStream(eq(stub), any(ThrottleTimer.class)))
        .thenReturn(mockGetDataStream);
    when(mockStreamFactory.createCommitWorkStream(eq(stub), any(ThrottleTimer.class)))
        .thenReturn(mockCommitWorkStream);

    WindmillStreamSender windmillStreamSender =
        newWindmillStreamSender(
            GetWorkBudget.builder().setBytes(byteBudget).setItems(itemBudget).build(),
            mockStreamFactory);

    windmillStreamSender.startStreams();
    windmillStreamSender.closeAllStreams();

    verify(mockGetWorkStream).close();
    verify(mockGetDataStream).close();
    verify(mockCommitWorkStream).close();
  }

  private WindmillStreamSender newWindmillStreamSender(GetWorkBudget budget) {
    return newWindmillStreamSender(budget, streamFactory);
  }

  private WindmillStreamSender newWindmillStreamSender(
      GetWorkBudget budget, GrpcWindmillStreamFactory streamFactory) {
    return WindmillStreamSender.create(
        stub, GET_WORK_REQUEST, budget, streamFactory, workItemProcessor);
  }
}

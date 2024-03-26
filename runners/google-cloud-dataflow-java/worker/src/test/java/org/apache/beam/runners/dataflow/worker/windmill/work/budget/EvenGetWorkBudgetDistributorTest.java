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
package org.apache.beam.runners.dataflow.worker.windmill.work.budget;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.WindmillStreamSender;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.ManagedChannel;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.inprocess.InProcessChannelBuilder;
import org.apache.beam.vendor.grpc.v1p60p1.io.grpc.testing.GrpcCleanupRule;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EvenGetWorkBudgetDistributorTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private ManagedChannel inProcessChannel;
  private CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub stub;

  private static GetWorkBudgetDistributor createBudgetDistributor(GetWorkBudget activeWorkBudget) {
    return GetWorkBudgetDistributors.distributeEvenly(() -> activeWorkBudget);
  }

  private static GetWorkBudgetDistributor createBudgetDistributor(long activeWorkItemsAndBytes) {
    return createBudgetDistributor(
        GetWorkBudget.builder()
            .setItems(activeWorkItemsAndBytes)
            .setBytes(activeWorkItemsAndBytes)
            .build());
  }

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
  public void testDistributeBudget_doesNothingWhenPassedInStreamsEmpty() {
    createBudgetDistributor(1L)
        .distributeBudget(
            ImmutableList.of(), GetWorkBudget.builder().setItems(10L).setBytes(10L).build());
  }

  @Test
  public void testDistributeBudget_doesNothingWithNoBudget() {
    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(GetWorkBudget.noBudget()));
    createBudgetDistributor(1L)
        .distributeBudget(ImmutableList.of(windmillStreamSender), GetWorkBudget.noBudget());
    verifyNoInteractions(windmillStreamSender);
  }

  @Test
  public void testDistributeBudget_doesNotAdjustStreamBudgetWhenRemainingBudgetHighNoActiveWork() {
    WindmillStreamSender windmillStreamSender =
        spy(
            createWindmillStreamSender(
                GetWorkBudget.builder().setItems(10L).setBytes(10L).build()));
    createBudgetDistributor(0L)
        .distributeBudget(
            ImmutableList.of(windmillStreamSender),
            GetWorkBudget.builder().setItems(10L).setBytes(10L).build());

    verify(windmillStreamSender, never()).adjustBudget(anyLong(), anyLong());
  }

  @Test
  public void
      testDistributeBudget_doesNotAdjustStreamBudgetWhenRemainingBudgetHighWithActiveWork() {
    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(GetWorkBudget.builder().setItems(5L).setBytes(5L).build()));
    createBudgetDistributor(10L)
        .distributeBudget(
            ImmutableList.of(windmillStreamSender),
            GetWorkBudget.builder().setItems(20L).setBytes(20L).build());

    verify(windmillStreamSender, never()).adjustBudget(anyLong(), anyLong());
  }

  @Test
  public void
      testDistributeBudget_adjustsStreamBudgetWhenRemainingItemBudgetTooLowWithNoActiveWork() {
    GetWorkBudget streamRemainingBudget =
        GetWorkBudget.builder().setItems(1L).setBytes(10L).build();
    GetWorkBudget totalGetWorkBudget = GetWorkBudget.builder().setItems(10L).setBytes(10L).build();
    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(streamRemainingBudget));
    createBudgetDistributor(0L)
        .distributeBudget(ImmutableList.of(windmillStreamSender), totalGetWorkBudget);

    verify(windmillStreamSender, times(1))
        .adjustBudget(
            eq(totalGetWorkBudget.items() - streamRemainingBudget.items()),
            eq(totalGetWorkBudget.bytes() - streamRemainingBudget.bytes()));
  }

  @Test
  public void
      testDistributeBudget_adjustsStreamBudgetWhenRemainingItemBudgetTooLowWithActiveWork() {
    GetWorkBudget streamRemainingBudget =
        GetWorkBudget.builder().setItems(1L).setBytes(10L).build();
    GetWorkBudget totalGetWorkBudget = GetWorkBudget.builder().setItems(10L).setBytes(10L).build();
    long activeWorkItemsAndBytes = 2L;
    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(streamRemainingBudget));
    createBudgetDistributor(activeWorkItemsAndBytes)
        .distributeBudget(ImmutableList.of(windmillStreamSender), totalGetWorkBudget);

    verify(windmillStreamSender, times(1))
        .adjustBudget(
            eq(
                totalGetWorkBudget.items()
                    - streamRemainingBudget.items()
                    - activeWorkItemsAndBytes),
            eq(totalGetWorkBudget.bytes() - streamRemainingBudget.bytes()));
  }

  @Test
  public void testDistributeBudget_adjustsStreamBudgetWhenRemainingByteBudgetTooLowNoActiveWork() {
    GetWorkBudget streamRemainingBudget =
        GetWorkBudget.builder().setItems(10L).setBytes(1L).build();
    GetWorkBudget totalGetWorkBudget = GetWorkBudget.builder().setItems(10L).setBytes(10L).build();
    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(streamRemainingBudget));
    createBudgetDistributor(0L)
        .distributeBudget(ImmutableList.of(windmillStreamSender), totalGetWorkBudget);

    verify(windmillStreamSender, times(1))
        .adjustBudget(
            eq(totalGetWorkBudget.items() - streamRemainingBudget.items()),
            eq(totalGetWorkBudget.bytes() - streamRemainingBudget.bytes()));
  }

  @Test
  public void
      testDistributeBudget_adjustsStreamBudgetWhenRemainingByteBudgetTooLowWithActiveWork() {
    GetWorkBudget streamRemainingBudget =
        GetWorkBudget.builder().setItems(10L).setBytes(1L).build();
    GetWorkBudget totalGetWorkBudget = GetWorkBudget.builder().setItems(10L).setBytes(10L).build();
    long activeWorkItemsAndBytes = 2L;

    WindmillStreamSender windmillStreamSender =
        spy(createWindmillStreamSender(streamRemainingBudget));
    createBudgetDistributor(activeWorkItemsAndBytes)
        .distributeBudget(ImmutableList.of(windmillStreamSender), totalGetWorkBudget);

    verify(windmillStreamSender, times(1))
        .adjustBudget(
            eq(totalGetWorkBudget.items() - streamRemainingBudget.items()),
            eq(
                totalGetWorkBudget.bytes()
                    - streamRemainingBudget.bytes()
                    - activeWorkItemsAndBytes));
  }

  @Test
  public void testDistributeBudget_distributesBudgetEvenlyIfPossible() {
    long totalItemsAndBytes = 10L;
    List<WindmillStreamSender> streams = new ArrayList<>();
    for (int i = 0; i < totalItemsAndBytes; i++) {
      streams.add(spy(createWindmillStreamSender(GetWorkBudget.noBudget())));
    }
    createBudgetDistributor(0L)
        .distributeBudget(
            ImmutableList.copyOf(streams),
            GetWorkBudget.builder()
                .setItems(totalItemsAndBytes)
                .setBytes(totalItemsAndBytes)
                .build());

    long itemsAndBytesPerStream = totalItemsAndBytes / streams.size();
    streams.forEach(
        stream ->
            verify(stream, times(1))
                .adjustBudget(eq(itemsAndBytesPerStream), eq(itemsAndBytesPerStream)));
  }

  @Test
  public void testDistributeBudget_distributesFairlyWhenNotEven() {
    long totalItemsAndBytes = 10L;
    List<WindmillStreamSender> streams = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      streams.add(spy(createWindmillStreamSender(GetWorkBudget.noBudget())));
    }
    createBudgetDistributor(0L)
        .distributeBudget(
            ImmutableList.copyOf(streams),
            GetWorkBudget.builder()
                .setItems(totalItemsAndBytes)
                .setBytes(totalItemsAndBytes)
                .build());

    long itemsAndBytesPerStream = (long) Math.ceil(totalItemsAndBytes / (streams.size() * 1.0));
    streams.forEach(
        stream ->
            verify(stream, times(1))
                .adjustBudget(eq(itemsAndBytesPerStream), eq(itemsAndBytesPerStream)));
  }

  private WindmillStreamSender createWindmillStreamSender(GetWorkBudget getWorkBudget) {
    return WindmillStreamSender.create(
        stub,
        Windmill.GetWorkRequest.newBuilder()
            .setClientId(1L)
            .setJobId("job")
            .setProjectId("project")
            .build(),
        getWorkBudget,
        GrpcWindmillStreamFactory.of(
                JobHeader.newBuilder()
                    .setJobId("job")
                    .setProjectId("project")
                    .setWorkerId("worker")
                    .build())
            .build(),
        (computation,
            inputDataWatermark,
            synchronizedProcessingTime,
            workItem,
            ackQueuedWorkItem,
            getWorkStreamLatencies) -> {});
  }
}

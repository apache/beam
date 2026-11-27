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

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.util.MemoryMonitor;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc;
import org.apache.beam.runners.dataflow.worker.windmill.CloudWindmillServiceV1Alpha1Grpc.CloudWindmillServiceV1Alpha1Stub;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.JobHeader;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillConnection;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillEndpoints.Endpoint;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServiceAddress.AuthenticatedGcpServiceAddress;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.CommitWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetDataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream.GetWorkerMetadataStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.ThrottlingGetDataMetricTracker;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcDispatcherClient;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.GrpcWindmillStreamFactory;
import org.apache.beam.runners.dataflow.worker.windmill.client.grpc.stubs.ChannelCachingStubFactory;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkItemScheduler;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudgetDistributor;
import org.apache.beam.vendor.grpc.v1p69p0.io.grpc.Channel;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class FanOutStreamingEngineWorkerHarnessLifecycleTest {
  private static final JobHeader JOB_HEADER =
      JobHeader.newBuilder()
          .setJobId("jobId")
          .setProjectId("projectId")
          .setWorkerId("workerId")
          .setClientId(1L)
          .build();
  private static final WindmillServiceAddress DISPATCHER_ADDRESS =
      WindmillServiceAddress.create(
          HostAndPort.fromParts("us-central1-dataflowstreaming-pa.googleapis.com", 443));
  private static final WindmillServiceAddress DIRECT_IPV6_ADDRESS_1 =
      WindmillServiceAddress.create(
          AuthenticatedGcpServiceAddress.create(
              "us-central1-dataflowstreaming-pa.googleapis.com",
              HostAndPort.fromParts("2001:db8:0:bac5:0:0:fed0:81a2", 12345)));
  private static final WindmillServiceAddress DIRECT_IPV6_ADDRESS_2 =
      WindmillServiceAddress.create(
          AuthenticatedGcpServiceAddress.create(
              "us-central1-dataflowstreaming-pa.googleapis.com",
              HostAndPort.fromParts("2001:db8:0:bac5:0:0:fed0:82a3", 12345)));

  private GrpcWindmillStreamFactory mockStreamFactory;
  private ChannelCachingStubFactory mockStubFactory;
  private AtomicReference<Consumer<WindmillEndpoints>> metadataConsumerRef;
  private FanOutStreamingEngineWorkerHarness harness;

  @Before
  public void setUp() {
    mockStreamFactory = mock(GrpcWindmillStreamFactory.class);
    mockStubFactory = mock(ChannelCachingStubFactory.class);
    metadataConsumerRef = new AtomicReference<>();

    GetWorkerMetadataStream mockMetadataStream = mock(GetWorkerMetadataStream.class);
    doAnswer(
            invocation -> {
              metadataConsumerRef.set(invocation.getArgument(1));
              return mockMetadataStream;
            })
        .when(mockStreamFactory)
        .createGetWorkerMetadataStream(any(), any());
    when(mockStreamFactory.createDirectGetWorkStream(any(), any(), any(), any(), any(), any()))
        .thenAnswer(inv -> mock(GetWorkStream.class));
    when(mockStreamFactory.createDirectGetDataStream(any()))
        .thenAnswer(inv -> mock(GetDataStream.class));
    when(mockStreamFactory.createDirectCommitWorkStream(any()))
        .thenAnswer(inv -> mock(CommitWorkStream.class));

    WorkItemScheduler noOpScheduler =
        (workItem, size, watermarks, context, drainMode, finalizeIds, latencies) -> {};
    harness =
        FanOutStreamingEngineWorkerHarness.forTesting(
            JOB_HEADER,
            GetWorkBudget.builder().setItems(10).setBytes(100).build(),
            mockStreamFactory,
            noOpScheduler,
            mockStubFactory,
            mock(GetWorkBudgetDistributor.class),
            mock(GrpcDispatcherClient.class),
            ignored -> mock(WorkCommitter.class),
            new ThrottlingGetDataMetricTracker(mock(MemoryMonitor.class)));
  }

  @After
  public void tearDown() {
    if (harness != null) {
      harness.shutdown();
    }
  }

  @Test
  public void testCloudPathToDirectPathTransitionDoesNotEvictSharedGlobalDataChannel() {
    Endpoint cloudWorkEndpoint =
        Endpoint.builder()
            .setDirectEndpoint(DISPATCHER_ADDRESS)
            .setWorkerToken("cloud_token")
            .build();
    Endpoint globalDataEndpoint = Endpoint.builder().setDirectEndpoint(DISPATCHER_ADDRESS).build();

    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(1L)
                .setType(WindmillEndpoints.Type.CLOUDPATH)
                .setWindmillEndpoints(ImmutableSet.of(cloudWorkEndpoint))
                .setGlobalDataEndpoints(ImmutableMap.of("side_input_tag", globalDataEndpoint))
                .build());

    GetDataStream initialGlobalDataStream =
        harness.currentBackends().globalDataStreams().get("side_input_tag").stream();

    // Transition work endpoints to DirectPath IPv6 while global_data_endpoints continues pointing
    // to DISPATCHER_ADDRESS.
    Endpoint directWorkEndpoint =
        Endpoint.builder()
            .setDirectEndpoint(DIRECT_IPV6_ADDRESS_1)
            .setWorkerToken("direct_token_1")
            .build();
    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(1L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of(directWorkEndpoint))
                .setGlobalDataEndpoints(ImmutableMap.of("side_input_tag", globalDataEndpoint))
                .build());

    // DISPATCHER_ADDRESS is still referenced by global_data_endpoints and must NOT be evicted.
    verify(mockStubFactory, never()).remove(eq(DISPATCHER_ADDRESS));
    assertThat(harness.currentBackends().globalDataStreams().get("side_input_tag").stream())
        .isSameInstanceAs(initialGlobalDataStream);
    verify(initialGlobalDataStream, never()).shutdown();
  }

  @Test
  public void testGlobalDataEndpointReassignmentAndTagRemovalClosesStaleStreams() {
    Endpoint endpoint1 = Endpoint.builder().setDirectEndpoint(DIRECT_IPV6_ADDRESS_1).build();
    Endpoint endpoint2 = Endpoint.builder().setDirectEndpoint(DIRECT_IPV6_ADDRESS_2).build();
    Endpoint endpointDispatcher = Endpoint.builder().setDirectEndpoint(DISPATCHER_ADDRESS).build();

    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(1L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of())
                .setGlobalDataEndpoints(
                    ImmutableMap.of(
                        "tag_reassigned", endpoint1,
                        "tag_removed_sharing_endpoint", endpoint1,
                        "tag_unchanged", endpointDispatcher))
                .build());

    GetDataStream oldReassignedStream =
        harness.currentBackends().globalDataStreams().get("tag_reassigned").stream();
    GetDataStream removedTagStream =
        harness.currentBackends().globalDataStreams().get("tag_removed_sharing_endpoint").stream();
    GetDataStream unchangedStream =
        harness.currentBackends().globalDataStreams().get("tag_unchanged").stream();

    // Update: tag_reassigned moves from endpoint1 -> endpoint2, tag_removed_sharing_endpoint is
    // removed, tag_unchanged stays on endpointDispatcher.
    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(2L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of())
                .setGlobalDataEndpoints(
                    ImmutableMap.of(
                        "tag_reassigned", endpoint2,
                        "tag_unchanged", endpointDispatcher))
                .build());

    verify(oldReassignedStream, times(1)).shutdown();
    verify(removedTagStream, times(1)).shutdown();
    verify(unchangedStream, never()).shutdown();
    verify(mockStubFactory, times(1)).remove(eq(DIRECT_IPV6_ADDRESS_1));
    verify(mockStubFactory, never()).remove(eq(DISPATCHER_ADDRESS));

    GetDataStream newReassignedStream =
        harness.currentBackends().globalDataStreams().get("tag_reassigned").stream();
    assertThat(newReassignedStream).isNotSameInstanceAs(oldReassignedStream);
    assertThat(harness.currentBackends().globalDataStreams().get("tag_unchanged").stream())
        .isSameInstanceAs(unchangedStream);
    assertThat(harness.currentBackends().globalDataStreams())
        .doesNotContainKey("tag_removed_sharing_endpoint");
  }

  @Test
  public void testRemovingOneGlobalDataTagSharingEndpointPreservesRemainingTagAndChannel() {
    Endpoint endpointDispatcher = Endpoint.builder().setDirectEndpoint(DISPATCHER_ADDRESS).build();

    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(1L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of())
                .setGlobalDataEndpoints(
                    ImmutableMap.of(
                        "tag_kept", endpointDispatcher,
                        "tag_removed", endpointDispatcher))
                .build());

    GetDataStream keptStream =
        harness.currentBackends().globalDataStreams().get("tag_kept").stream();
    GetDataStream removedStream =
        harness.currentBackends().globalDataStreams().get("tag_removed").stream();

    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(2L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of())
                .setGlobalDataEndpoints(ImmutableMap.of("tag_kept", endpointDispatcher))
                .build());

    verify(removedStream, times(1)).shutdown();
    verify(keptStream, never()).shutdown();
    verify(mockStubFactory, never()).remove(eq(DISPATCHER_ADDRESS));
    assertThat(harness.currentBackends().globalDataStreams().get("tag_kept").stream())
        .isSameInstanceAs(keptStream);
    assertThat(harness.currentBackends().globalDataStreams()).doesNotContainKey("tag_removed");
  }

  @Test
  public void testGlobalDataStreamConnectionRefetchesStubDynamically() {
    CloudWindmillServiceV1Alpha1Stub initialStub =
        CloudWindmillServiceV1Alpha1Grpc.newStub(mock(Channel.class));
    CloudWindmillServiceV1Alpha1Stub refreshedStub =
        CloudWindmillServiceV1Alpha1Grpc.newStub(mock(Channel.class));
    when(mockStubFactory.createWindmillServiceStub(DISPATCHER_ADDRESS))
        .thenReturn(initialStub, refreshedStub);

    Endpoint globalDataEndpoint = Endpoint.builder().setDirectEndpoint(DISPATCHER_ADDRESS).build();
    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(1L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of())
                .setGlobalDataEndpoints(ImmutableMap.of("side_input_tag", globalDataEndpoint))
                .build());

    ArgumentCaptor<WindmillConnection> connectionCaptor =
        ArgumentCaptor.forClass(WindmillConnection.class);
    verify(mockStreamFactory, times(1)).createDirectGetDataStream(connectionCaptor.capture());

    WindmillConnection connection = connectionCaptor.getValue();
    assertThat(connection.currentStub()).isSameInstanceAs(initialStub);
    assertThat(connection.currentStub()).isSameInstanceAs(refreshedStub);
    verify(mockStubFactory, times(2)).createWindmillServiceStub(eq(DISPATCHER_ADDRESS));
  }

  @Test
  public void testMetadataDeliveredAfterShutdownIsIgnoredWithoutException() {
    harness.shutdown();

    Endpoint endpoint =
        Endpoint.builder()
            .setDirectEndpoint(DIRECT_IPV6_ADDRESS_1)
            .setWorkerToken("post_shutdown_worker")
            .build();
    metadataConsumerRef
        .get()
        .accept(
            WindmillEndpoints.builder()
                .setVersion(10L)
                .setType(WindmillEndpoints.Type.DIRECTPATH)
                .setWindmillEndpoints(ImmutableSet.of(endpoint))
                .setGlobalDataEndpoints(ImmutableMap.of())
                .build());

    assertThat(harness.currentBackends().windmillStreams()).isEmpty();
  }
}

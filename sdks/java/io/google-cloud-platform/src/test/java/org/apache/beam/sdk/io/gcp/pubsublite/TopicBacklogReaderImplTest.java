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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.internal.ExtractStatus;
import com.google.cloud.pubsublite.internal.TopicStatsClient;
import com.google.cloud.pubsublite.proto.ComputeMessageStatsResponse;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import java.util.concurrent.ExecutionException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public final class TopicBacklogReaderImplTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock TopicStatsClient mockClient;

  private TopicPath topicPath;
  private TopicBacklogReader reader;

  @Before
  public void setUp() throws Exception {
    this.topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectNumber.of(4))
            .setName(TopicName.of("test"))
            .setLocation(CloudZone.parse("us-central1-b"))
            .build();
    this.reader = new TopicBacklogReaderImpl(mockClient, topicPath);
  }

  @Test
  public void computeMessageStats_partialFailure() throws Exception {
    ComputeMessageStatsResponse partition1 = ComputeMessageStatsResponse.getDefaultInstance();

    when(mockClient.computeMessageStats(
            topicPath, Partition.of(1), Offset.of(10), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition1));
    when(mockClient.computeMessageStats(
            topicPath, Partition.of(2), Offset.of(20), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFailedFuture(new StatusException(Status.UNAVAILABLE)));

    ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
    ApiFuture<ComputeMessageStatsResponse> future =
        reader.computeMessageStats(
            ImmutableMap.of(Partition.of(1), Offset.of(10), Partition.of(2), Offset.of(20)));

    ExecutionException ex = assertThrows(ExecutionException.class, future::get);
    assertEquals(ExtractStatus.extract(ex.getCause()).get().getCode(), Code.UNAVAILABLE);
  }

  @Test
  public void computeMessageStats_aggregatesEmptyMessages() throws Exception {
    ComputeMessageStatsResponse partition1 = ComputeMessageStatsResponse.getDefaultInstance();
    ComputeMessageStatsResponse partition2 = ComputeMessageStatsResponse.getDefaultInstance();
    ComputeMessageStatsResponse aggregate = ComputeMessageStatsResponse.getDefaultInstance();

    when(mockClient.computeMessageStats(
            topicPath, Partition.of(1), Offset.of(10), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition1));
    when(mockClient.computeMessageStats(
            topicPath, Partition.of(2), Offset.of(20), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition2));

    ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
    ApiFuture<ComputeMessageStatsResponse> future =
        reader.computeMessageStats(
            ImmutableMap.of(Partition.of(1), Offset.of(10), Partition.of(2), Offset.of(20)));

    assertEquals(future.get(), aggregate);
  }

  @Test
  public void computeMessageStats_timestampsAggregatedWhenPresent() throws Exception {
    Timestamp minEventTime = Timestamp.newBuilder().setSeconds(1000).setNanos(10).build();
    Timestamp minPublishTime = Timestamp.newBuilder().setSeconds(1001).setNanos(11).build();
    ComputeMessageStatsResponse partition1 =
        ComputeMessageStatsResponse.newBuilder().setMinimumPublishTime(minPublishTime).build();
    ComputeMessageStatsResponse partition2 =
        ComputeMessageStatsResponse.newBuilder().setMinimumEventTime(minEventTime).build();
    ComputeMessageStatsResponse aggregate =
        ComputeMessageStatsResponse.newBuilder()
            .setMinimumEventTime(minEventTime)
            .setMinimumPublishTime(minPublishTime)
            .build();

    when(mockClient.computeMessageStats(
            topicPath, Partition.of(1), Offset.of(10), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition1));
    when(mockClient.computeMessageStats(
            topicPath, Partition.of(2), Offset.of(20), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition2));

    ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
    ApiFuture<ComputeMessageStatsResponse> future =
        reader.computeMessageStats(
            ImmutableMap.of(Partition.of(1), Offset.of(10), Partition.of(2), Offset.of(20)));

    assertEquals(future.get(), aggregate);
  }

  @Test
  public void computeMessageStats_resultsAggregated() throws Exception {
    Timestamp minEventTime = Timestamp.newBuilder().setSeconds(1000).setNanos(10).build();
    Timestamp minPublishTime = Timestamp.newBuilder().setSeconds(1001).setNanos(11).build();
    ComputeMessageStatsResponse partition1 =
        ComputeMessageStatsResponse.newBuilder()
            .setMessageCount(10)
            .setMessageBytes(100)
            .setMinimumEventTime(minEventTime.toBuilder().setSeconds(1002).build())
            .setMinimumPublishTime(minPublishTime)
            .build();
    ComputeMessageStatsResponse partition2 =
        ComputeMessageStatsResponse.newBuilder()
            .setMessageCount(20)
            .setMessageBytes(200)
            .setMinimumEventTime(minEventTime)
            .setMinimumPublishTime(minPublishTime.toBuilder().setNanos(12).build())
            .build();
    ComputeMessageStatsResponse aggregate =
        ComputeMessageStatsResponse.newBuilder()
            .setMessageCount(30)
            .setMessageBytes(300)
            .setMinimumEventTime(minEventTime)
            .setMinimumPublishTime(minPublishTime)
            .build();

    when(mockClient.computeMessageStats(
            topicPath, Partition.of(1), Offset.of(10), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition1));
    when(mockClient.computeMessageStats(
            topicPath, Partition.of(2), Offset.of(20), Offset.of(Integer.MAX_VALUE)))
        .thenReturn(ApiFutures.immediateFuture(partition2));

    ImmutableMap.Builder<Partition, Offset> builder = ImmutableMap.builder();
    ApiFuture<ComputeMessageStatsResponse> future =
        reader.computeMessageStats(
            ImmutableMap.of(Partition.of(1), Offset.of(10), Partition.of(2), Offset.of(20)));

    assertEquals(future.get(), aggregate);
  }
}

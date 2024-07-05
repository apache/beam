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
package org.apache.beam.runners.dataflow.worker.windmill.work.refresh;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.auto.value.AutoValue;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.RefreshableWork;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HeartbeatRequestsTest {

  private Map<ShardedKey, Deque<Work>> activeWork;

  private static Work createWork(Windmill.WorkItem workItem, HeartbeatSender heartbeatSender) {
    return Work.create(
        workItem,
        Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
        createProcessingContext(heartbeatSender),
        Instant::now,
        Collections.emptyList());
  }

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private static Work.ProcessingContext createProcessingContext(HeartbeatSender heartbeatSender) {
    return Work.createProcessingContext(
        "computationId",
        (computationId, request) -> Windmill.KeyedGetDataResponse.getDefaultInstance(),
        ignored -> {},
        heartbeatSender);
  }

  private static Work expiredWork(Windmill.WorkItem workItem, HeartbeatSender heartbeatSender) {
    return Work.create(
        workItem,
        Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
        createProcessingContext(heartbeatSender),
        () -> Instant.EPOCH,
        Collections.emptyList());
  }

  private static Windmill.WorkItem createWorkItem(long workToken, long cacheToken) {
    return Windmill.WorkItem.newBuilder()
        .setKey(ByteString.copyFromUtf8(""))
        .setShardingKey(1)
        .setWorkToken(workToken)
        .setCacheToken(cacheToken)
        .build();
  }

  @Before
  public void setUp() {
    activeWork = new HashMap<>();
  }

  @Test
  public void testGetRefreshableFanoutKeyHeartbeats() {
    Instant refreshDeadline = Instant.now();
    HeartbeatSender sender1 = ignored -> {};
    HeartbeatSender sender2 = ignored -> {};

    Work freshWork = createWork(createWorkItem(3L, 3L), sender1);
    Work refreshableWork1 = expiredWork(createWorkItem(1L, 1L), sender1);
    refreshableWork1.setState(Work.State.COMMITTING);
    Work refreshableWork2 = expiredWork(createWorkItem(2L, 2L), sender2);
    refreshableWork2.setState(Work.State.COMMITTING);
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    activateWorkForKey(shardedKey1, refreshableWork1);
    activateWorkForKey(shardedKey1, freshWork);
    activateWorkForKey(shardedKey2, refreshableWork2);

    Table<HeartbeatSender, RefreshableWork, Windmill.HeartbeatRequest> requests =
        HeartbeatRequests.getRefreshableKeyHeartbeats(
            currentActiveWork(), refreshDeadline, DataflowExecutionStateSampler.instance());

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> expected =
        ImmutableList.of(
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey1, refreshableWork1),
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey2, refreshableWork2));

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> actual =
        requests.cellSet().stream()
            .map(
                entry ->
                    HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(
                        entry.getValue(), entry.getRowKey()))
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(expected);
  }

  private void activateWorkForKey(ShardedKey shardedKey, Work work) {
    Deque<Work> workQueue = activeWork.computeIfAbsent(shardedKey, ignored -> new ArrayDeque<>());
    workQueue.addLast(work);
  }

  private ImmutableListMultimap<ShardedKey, RefreshableWork> currentActiveWork() {
    ImmutableListMultimap.Builder<ShardedKey, RefreshableWork> currentActiveWork =
        ImmutableListMultimap.builder();

    for (Map.Entry<ShardedKey, Deque<Work>> keyedWorkQueues : activeWork.entrySet()) {
      currentActiveWork.putAll(
          keyedWorkQueues.getKey(),
          keyedWorkQueues.getValue().stream()
              .map(Work::refreshableView)
              .collect(Collectors.toList()));
    }

    return currentActiveWork.build();
  }

  @AutoValue
  abstract static class HeartbeatRequestShardingKeyWorkTokenAndCacheToken {

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken create(
        long shardingKey, long workToken, long cacheToken, HeartbeatSender sender) {
      return new AutoValue_HeartbeatRequestsTest_HeartbeatRequestShardingKeyWorkTokenAndCacheToken(
          shardingKey, workToken, cacheToken, sender);
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        ShardedKey shardedKey, Work work) {
      return create(
          shardedKey.shardingKey(),
          work.getWorkItem().getWorkToken(),
          work.getWorkItem().getCacheToken(),
          work.heartbeatSender());
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        Windmill.HeartbeatRequest heartbeatRequest, HeartbeatSender sender) {
      return create(
          heartbeatRequest.getShardingKey(),
          heartbeatRequest.getWorkToken(),
          heartbeatRequest.getCacheToken(),
          sender);
    }

    abstract long shardingKey();

    abstract long workToken();

    abstract long cacheToken();

    abstract HeartbeatSender heartbeatSender();

    @Override
    public final boolean equals(Object obj) {
      if (!(obj instanceof HeartbeatRequestShardingKeyWorkTokenAndCacheToken)) {
        return false;
      }
      HeartbeatRequestShardingKeyWorkTokenAndCacheToken other =
          (HeartbeatRequestShardingKeyWorkTokenAndCacheToken) obj;
      return shardingKey() == other.shardingKey()
          && workToken() == other.workToken()
          && cacheToken() == other.cacheToken();
    }

    @Override
    public final int hashCode() {
      return Objects.hash(shardingKey(), workToken(), cacheToken());
    }
  }
}

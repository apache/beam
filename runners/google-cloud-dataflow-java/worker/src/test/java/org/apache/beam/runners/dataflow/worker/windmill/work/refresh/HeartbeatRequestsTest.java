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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.auto.value.AutoValue;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkProcessingContext;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableListMultimap;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HeartbeatRequestsTest {

  private Map<ShardedKey, Deque<Work>> activeWork;

  private static Work createWork(
      Windmill.WorkItem workItem, WindmillStream.GetDataStream getDataStream) {
    return Work.create(
        createProcessingContext(workItem, getDataStream),
        Instant::now,
        Collections.emptyList(),
        unused -> {});
  }

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private static Work createWork(Windmill.WorkItem workItem) {
    return Work.create(
        createProcessingContext(workItem), Instant::now, Collections.emptyList(), unused -> {});
  }

  private static Work expiredWork(Windmill.WorkItem workItem) {
    return Work.create(
        createProcessingContext(workItem),
        () -> Instant.EPOCH,
        Collections.emptyList(),
        unused -> {});
  }

  private static WorkProcessingContext createProcessingContext(Windmill.WorkItem workItem) {
    WindmillStream.GetDataStream getDataStream = mock(WindmillStream.GetDataStream.class);
    when(getDataStream.requestKeyedData(anyString(), any()))
        .thenReturn(Windmill.KeyedGetDataResponse.getDefaultInstance());
    return createProcessingContext(workItem, getDataStream);
  }

  private static WorkProcessingContext createProcessingContext(
      Windmill.WorkItem workItem, WindmillStream.GetDataStream getDataStream) {
    WorkCommitter workCommitter = mock(WorkCommitter.class);
    doNothing().when(workCommitter).commit(any(Commit.class));
    return WorkProcessingContext.builder("computationId", getDataStream)
        .setWorkItem(workItem)
        .setWorkCommitter(workCommitter::commit)
        .setInputDataWatermark(Instant.EPOCH)
        .build();
  }

  private static Work expiredWork(
      Windmill.WorkItem workItem, WindmillStream.GetDataStream getDataStream) {
    return Work.create(
        createProcessingContext(workItem, getDataStream),
        () -> Instant.EPOCH,
        Collections.emptyList(),
        unused -> {});
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
  public void testGetRefreshableKeyHeartbeats() {
    Instant refreshDeadline = Instant.now();

    Work freshWork = createWork(createWorkItem(3L, 3L));
    Work refreshableWork1 = expiredWork(createWorkItem(1L, 1L));
    refreshableWork1.setState(Work.State.COMMITTING);
    Work refreshableWork2 = expiredWork(createWorkItem(2L, 2L));
    refreshableWork2.setState(Work.State.COMMITTING);
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    activateWorkForKey(shardedKey1, refreshableWork1);
    activateWorkForKey(shardedKey1, freshWork);
    activateWorkForKey(shardedKey2, refreshableWork2);

    ImmutableList<Windmill.HeartbeatRequest> requests =
        HeartbeatRequests.getRefreshableKeyHeartbeats(
            activeWork, refreshDeadline, DataflowExecutionStateSampler.instance());

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> expected =
        ImmutableList.of(
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey1, refreshableWork1),
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey2, refreshableWork2));

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> actual =
        requests.stream()
            .map(HeartbeatRequestShardingKeyWorkTokenAndCacheToken::from)
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @Test
  public void testGetRefreshableFanoutKeyHeartbeats() {
    Instant refreshDeadline = Instant.now();
    WindmillStream.GetDataStream getDataStream1 = mock(WindmillStream.GetDataStream.class);
    when(getDataStream1.isClosed()).thenReturn(false);
    WindmillStream.GetDataStream getDataStream2 = mock(WindmillStream.GetDataStream.class);
    when(getDataStream2.isClosed()).thenReturn(false);

    Work freshWork = createWork(createWorkItem(3L, 3L), getDataStream1);
    Work refreshableWork1 = expiredWork(createWorkItem(1L, 1L), getDataStream1);
    refreshableWork1.setState(Work.State.COMMITTING);
    Work refreshableWork2 = expiredWork(createWorkItem(2L, 2L), getDataStream2);
    refreshableWork2.setState(Work.State.COMMITTING);
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    activateWorkForKey(shardedKey1, refreshableWork1);
    activateWorkForKey(shardedKey1, freshWork);
    activateWorkForKey(shardedKey2, refreshableWork2);

    ImmutableListMultimap<WindmillStream.GetDataStream, Windmill.HeartbeatRequest> requests =
        HeartbeatRequests.getRefreshableDirectKeyHeartbeats(
            activeWork, refreshDeadline, DataflowExecutionStateSampler.instance());

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> expected =
        ImmutableList.of(
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey1, refreshableWork1),
            HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(shardedKey2, refreshableWork2));

    ImmutableList<HeartbeatRequestShardingKeyWorkTokenAndCacheToken> actual =
        requests.entries().stream()
            .map(
                entry ->
                    HeartbeatRequestShardingKeyWorkTokenAndCacheToken.from(
                        entry.getValue(), entry.getKey()))
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(expected);
  }

  private void activateWorkForKey(ShardedKey shardedKey, Work work) {
    Deque<Work> workQueue = activeWork.computeIfAbsent(shardedKey, ignored -> new ArrayDeque<>());
    workQueue.addLast(work);
  }

  @AutoValue
  abstract static class HeartbeatRequestShardingKeyWorkTokenAndCacheToken {

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken create(
        long shardingKey, long workToken, long cacheToken) {
      return new AutoValue_HeartbeatRequestsTest_HeartbeatRequestShardingKeyWorkTokenAndCacheToken(
          shardingKey, workToken, cacheToken, null);
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken create(
        long shardingKey,
        long workToken,
        long cacheToken,
        WindmillStream.GetDataStream getDataStream) {
      return new AutoValue_HeartbeatRequestsTest_HeartbeatRequestShardingKeyWorkTokenAndCacheToken(
          shardingKey, workToken, cacheToken, Objects.requireNonNull(getDataStream));
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        Windmill.HeartbeatRequest heartbeatRequest) {
      return create(
          heartbeatRequest.getShardingKey(),
          heartbeatRequest.getWorkToken(),
          heartbeatRequest.getCacheToken());
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        ShardedKey shardedKey, Work work) {
      @Nullable
      WindmillStream.GetDataStream getDataStream = work.getProcessingContext().getDataStream();
      return getDataStream == null
          ? create(
              shardedKey.shardingKey(),
              work.getWorkItem().getWorkToken(),
              work.getWorkItem().getCacheToken())
          : create(
              shardedKey.shardingKey(),
              work.getWorkItem().getWorkToken(),
              work.getWorkItem().getCacheToken(),
              work.getProcessingContext().getDataStream());
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        Windmill.HeartbeatRequest heartbeatRequest, WindmillStream.GetDataStream getDataStream) {
      return create(
          heartbeatRequest.getShardingKey(),
          heartbeatRequest.getWorkToken(),
          heartbeatRequest.getCacheToken(),
          getDataStream);
    }

    abstract long shardingKey();

    abstract long workToken();

    abstract long cacheToken();

    abstract @Nullable WindmillStream.GetDataStream getDataStream();

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

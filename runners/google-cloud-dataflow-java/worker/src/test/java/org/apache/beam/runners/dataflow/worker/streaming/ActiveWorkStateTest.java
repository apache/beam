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
package org.apache.beam.runners.dataflow.worker.streaming;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.auto.value.AutoValue;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ActiveWorkState.ActivateWorkResult;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActiveWorkStateTest {
  private final WindmillStateCache.ForComputation computationStateCache =
      mock(WindmillStateCache.ForComputation.class);
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);
  private Map<ShardedKey, Deque<Work>> readOnlyActiveWork;

  private ActiveWorkState activeWorkState;

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private static Work createWork(Windmill.WorkItem workItem) {
    return Work.create(workItem, Instant::now, Collections.emptyList(), unused -> {});
  }

  private static Work expiredWork(Windmill.WorkItem workItem) {
    return Work.create(workItem, () -> Instant.EPOCH, Collections.emptyList(), unused -> {});
  }

  private static Windmill.WorkItem createWorkItem(long workToken) {
    return Windmill.WorkItem.newBuilder()
        .setKey(ByteString.copyFromUtf8(""))
        .setShardingKey(1)
        .setWorkToken(workToken)
        .build();
  }

  @Before
  public void setup() {
    Map<ShardedKey, Deque<Work>> readWriteActiveWorkMap = new HashMap<>();
    // Only use readOnlyActiveWork to verify internal behavior in reaction to exposed API calls.
    readOnlyActiveWork = Collections.unmodifiableMap(readWriteActiveWorkMap);
    activeWorkState = ActiveWorkState.forTesting(readWriteActiveWorkMap, computationStateCache);
  }

  @Test
  public void testActivateWorkForKey_EXECUTE_unknownKey() {
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(
            shardedKey("someKey", 1L), createWork(createWorkItem(1L)));

    assertEquals(ActivateWorkResult.EXECUTE, activateWorkResult);
  }

  @Test
  public void testActivateWorkForKey_EXECUTE_emptyWorkQueueForKey() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);
    long workToken = 1L;

    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(shardedKey, createWork(createWorkItem(workToken)));

    Optional<Work> nextWorkForKey =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workToken);

    assertEquals(ActivateWorkResult.EXECUTE, activateWorkResult);
    assertEquals(Optional.empty(), nextWorkForKey);
    assertThat(readOnlyActiveWork).doesNotContainKey(shardedKey);
  }

  @Test
  public void testActivateWorkForKey_DUPLICATE() {
    long workToken = 10L;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    // ActivateWork with the same shardedKey, and the same workTokens.
    activeWorkState.activateWorkForKey(shardedKey, createWork(createWorkItem(workToken)));
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(shardedKey, createWork(createWorkItem(workToken)));

    assertEquals(ActivateWorkResult.DUPLICATE, activateWorkResult);
  }

  @Test
  public void testActivateWorkForKey_QUEUED() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    // ActivateWork with the same shardedKey, but different workTokens.
    activeWorkState.activateWorkForKey(shardedKey, createWork(createWorkItem(1L)));
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(shardedKey, createWork(createWorkItem(2L)));

    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_noWorkQueueForKey() {
    assertEquals(
        Optional.empty(),
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey("someKey", 1L), 10L));
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_currentWorkInQueueDoesNotMatchWorkToComplete() {
    long workTokenToComplete = 1L;

    Work workInQueue = createWork(createWorkItem(2L));
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    activeWorkState.activateWorkForKey(shardedKey, workInQueue);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workTokenToComplete);

    assertEquals(1, readOnlyActiveWork.get(shardedKey).size());
    assertEquals(workInQueue, readOnlyActiveWork.get(shardedKey).peek());
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_removesWorkFromQueueWhenComplete() {
    long workTokenToComplete = 1L;

    Work activeWork = createWork(createWorkItem(workTokenToComplete));
    Work nextWork = createWork(createWorkItem(2L));
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    activeWorkState.activateWorkForKey(shardedKey, activeWork);
    activeWorkState.activateWorkForKey(shardedKey, nextWork);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workTokenToComplete);

    assertEquals(nextWork, readOnlyActiveWork.get(shardedKey).peek());
    assertEquals(1, readOnlyActiveWork.get(shardedKey).size());
    assertFalse(readOnlyActiveWork.get(shardedKey).contains(activeWork));
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_removesQueueIfNoWorkPresent() {
    Work workInQueue = createWork(createWorkItem(1L));
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    activeWorkState.activateWorkForKey(shardedKey, workInQueue);
    activeWorkState.completeWorkAndGetNextWorkForKey(
        shardedKey, workInQueue.getWorkItem().getWorkToken());

    assertFalse(readOnlyActiveWork.containsKey(shardedKey));
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_returnsWorkIfPresent() {
    Work workToBeCompleted = createWork(createWorkItem(1L));
    Work nextWork = createWork(createWorkItem(2L));
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    activeWorkState.activateWorkForKey(shardedKey, workToBeCompleted);
    activeWorkState.activateWorkForKey(shardedKey, nextWork);
    activeWorkState.completeWorkAndGetNextWorkForKey(
        shardedKey, workToBeCompleted.getWorkItem().getWorkToken());

    Optional<Work> nextWorkOpt =
        activeWorkState.completeWorkAndGetNextWorkForKey(
            shardedKey, workToBeCompleted.getWorkItem().getWorkToken());

    assertTrue(nextWorkOpt.isPresent());
    assertSame(nextWork, nextWorkOpt.get());

    Optional<Work> endOfWorkQueue =
        activeWorkState.completeWorkAndGetNextWorkForKey(
            shardedKey, nextWork.getWorkItem().getWorkToken());

    assertFalse(endOfWorkQueue.isPresent());
    assertFalse(readOnlyActiveWork.containsKey(shardedKey));
  }

  @Test
  public void testCurrentActiveWorkBudget_correctlyAggregatesActiveWorkBudget_oneShardKey() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);
    Work work1 = createWork(createWorkItem(1L));
    Work work2 = createWork(createWorkItem(2L));

    activeWorkState.activateWorkForKey(shardedKey, work1);
    activeWorkState.activateWorkForKey(shardedKey, work2);

    GetWorkBudget expectedActiveBudget1 =
        GetWorkBudget.builder()
            .setItems(2)
            .setBytes(
                work1.getWorkItem().getSerializedSize() + work2.getWorkItem().getSerializedSize())
            .build();

    assertThat(activeWorkState.currentActiveWorkBudget()).isEqualTo(expectedActiveBudget1);

    activeWorkState.completeWorkAndGetNextWorkForKey(
        shardedKey, work1.getWorkItem().getWorkToken());

    GetWorkBudget expectedActiveBudget2 =
        GetWorkBudget.builder()
            .setItems(1)
            .setBytes(work1.getWorkItem().getSerializedSize())
            .build();

    assertThat(activeWorkState.currentActiveWorkBudget()).isEqualTo(expectedActiveBudget2);
  }

  @Test
  public void testCurrentActiveWorkBudget_correctlyAggregatesActiveWorkBudget_whenWorkCompleted() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);
    Work work1 = createWork(createWorkItem(1L));
    Work work2 = createWork(createWorkItem(2L));

    activeWorkState.activateWorkForKey(shardedKey, work1);
    activeWorkState.activateWorkForKey(shardedKey, work2);
    activeWorkState.completeWorkAndGetNextWorkForKey(
        shardedKey, work1.getWorkItem().getWorkToken());

    GetWorkBudget expectedActiveBudget =
        GetWorkBudget.builder()
            .setItems(1)
            .setBytes(work1.getWorkItem().getSerializedSize())
            .build();

    assertThat(activeWorkState.currentActiveWorkBudget()).isEqualTo(expectedActiveBudget);
  }

  @Test
  public void testCurrentActiveWorkBudget_correctlyAggregatesActiveWorkBudget_multipleShardKeys() {
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("someKey", 2L);
    Work work1 = createWork(createWorkItem(1L));
    Work work2 = createWork(createWorkItem(2L));

    activeWorkState.activateWorkForKey(shardedKey1, work1);
    activeWorkState.activateWorkForKey(shardedKey2, work2);

    GetWorkBudget expectedActiveBudget =
        GetWorkBudget.builder()
            .setItems(2)
            .setBytes(
                work1.getWorkItem().getSerializedSize() + work2.getWorkItem().getSerializedSize())
            .build();

    assertThat(activeWorkState.currentActiveWorkBudget()).isEqualTo(expectedActiveBudget);
  }

  @Test
  public void testInvalidateStuckCommits() {
    Map<ShardedKey, Long> invalidatedCommits = new HashMap<>();

    Work stuckWork1 = expiredWork(createWorkItem(1L));
    stuckWork1.setState(Work.State.COMMITTING);
    Work stuckWork2 = expiredWork(createWorkItem(2L));
    stuckWork2.setState(Work.State.COMMITTING);
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    activeWorkState.activateWorkForKey(shardedKey1, stuckWork1);
    activeWorkState.activateWorkForKey(shardedKey2, stuckWork2);

    activeWorkState.invalidateStuckCommits(Instant.now(), invalidatedCommits::put);

    assertThat(invalidatedCommits)
        .containsEntry(shardedKey1, stuckWork1.getWorkItem().getWorkToken());
    assertThat(invalidatedCommits)
        .containsEntry(shardedKey2, stuckWork2.getWorkItem().getWorkToken());
    verify(computationStateCache).invalidate(shardedKey1.key(), shardedKey1.shardingKey());
    verify(computationStateCache).invalidate(shardedKey2.key(), shardedKey2.shardingKey());
  }

  @Test
  public void testGetKeyHeartbeats() {
    Instant refreshDeadline = Instant.now();

    Work freshWork = createWork(createWorkItem(3L));
    Work refreshableWork1 = expiredWork(createWorkItem(1L));
    refreshableWork1.setState(Work.State.COMMITTING);
    Work refreshableWork2 = expiredWork(createWorkItem(2L));
    refreshableWork2.setState(Work.State.COMMITTING);
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    activeWorkState.activateWorkForKey(shardedKey1, refreshableWork1);
    activeWorkState.activateWorkForKey(shardedKey1, freshWork);
    activeWorkState.activateWorkForKey(shardedKey2, refreshableWork2);

    ImmutableList<HeartbeatRequest> requests =
        activeWorkState.getKeyHeartbeats(refreshDeadline, DataflowExecutionStateSampler.instance());

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

  @AutoValue
  abstract static class HeartbeatRequestShardingKeyWorkTokenAndCacheToken {

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken create(
        long shardingKey, long workToken, long cacheToken) {
      return new AutoValue_ActiveWorkStateTest_HeartbeatRequestShardingKeyWorkTokenAndCacheToken(
          shardingKey, workToken, cacheToken);
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        HeartbeatRequest heartbeatRequest) {
      return create(
          heartbeatRequest.getShardingKey(),
          heartbeatRequest.getWorkToken(),
          heartbeatRequest.getCacheToken());
    }

    private static HeartbeatRequestShardingKeyWorkTokenAndCacheToken from(
        ShardedKey shardedKey, Work work) {
      return create(
          shardedKey.shardingKey(),
          work.getWorkItem().getWorkToken(),
          work.getWorkItem().getCacheToken());
    }

    abstract long shardingKey();

    abstract long workToken();

    abstract long cacheToken();
  }
}

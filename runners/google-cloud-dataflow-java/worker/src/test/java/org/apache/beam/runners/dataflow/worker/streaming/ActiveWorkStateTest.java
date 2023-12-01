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
import javax.annotation.Nullable;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ActiveWorkState.ActivateWorkResult;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.KeyedGetDataRequest;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ActiveWorkStateTest {

  private final WindmillStateCache.ForComputation computationStateCache =
      mock(WindmillStateCache.ForComputation.class);
  private Map<ShardedKey, Deque<Work>> readOnlyActiveWork;

  private ActiveWorkState activeWorkState;

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private static Work emptyWork() {
    return createWork(null);
  }

  private static Work createWork(@Nullable Windmill.WorkItem workItem) {
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
        activeWorkState.activateWorkForKey(shardedKey("someKey", 1L), emptyWork());

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
  public void testGetKeysToRefresh() {
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

    ImmutableList<KeyedGetDataRequest> requests =
        activeWorkState.getKeysToRefresh(refreshDeadline, DataflowExecutionStateSampler.instance());

    ImmutableList<GetDataRequestKeyShardingKeyAndWorkToken> expected =
        ImmutableList.of(
            GetDataRequestKeyShardingKeyAndWorkToken.from(shardedKey1, refreshableWork1),
            GetDataRequestKeyShardingKeyAndWorkToken.from(shardedKey2, refreshableWork2));

    ImmutableList<GetDataRequestKeyShardingKeyAndWorkToken> actual =
        requests.stream()
            .map(GetDataRequestKeyShardingKeyAndWorkToken::from)
            .collect(toImmutableList());

    assertThat(actual).containsExactlyElementsIn(expected);
  }

  @AutoValue
  abstract static class GetDataRequestKeyShardingKeyAndWorkToken {

    private static GetDataRequestKeyShardingKeyAndWorkToken create(
        ByteString key, long shardingKey, long workToken) {
      return new AutoValue_ActiveWorkStateTest_GetDataRequestKeyShardingKeyAndWorkToken(
          key, shardingKey, workToken);
    }

    private static GetDataRequestKeyShardingKeyAndWorkToken from(
        KeyedGetDataRequest keyedGetDataRequest) {
      return create(
          keyedGetDataRequest.getKey(),
          keyedGetDataRequest.getShardingKey(),
          keyedGetDataRequest.getWorkToken());
    }

    private static GetDataRequestKeyShardingKeyAndWorkToken from(ShardedKey shardedKey, Work work) {
      return create(shardedKey.key(), shardedKey.shardingKey(), work.getWorkItem().getWorkToken());
    }

    abstract ByteString key();

    abstract long shardingKey();

    abstract long workToken();
  }
}

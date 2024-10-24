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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.runners.dataflow.worker.streaming.ActiveWorkState.ActivateWorkResult;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
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
  private Map<ShardedKey, Deque<ExecutableWork>> readOnlyActiveWork;

  private ActiveWorkState activeWorkState;

  private static ShardedKey shardedKey(String str, long shardKey) {
    return ShardedKey.create(ByteString.copyFromUtf8(str), shardKey);
  }

  private static ExecutableWork createWork(Windmill.WorkItem workItem) {
    return ExecutableWork.create(
        Work.create(
            workItem,
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            createWorkProcessingContext(),
            Instant::now,
            Collections.emptyList()),
        ignored -> {});
  }

  private static ExecutableWork expiredWork(Windmill.WorkItem workItem) {
    return ExecutableWork.create(
        Work.create(
            workItem,
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            createWorkProcessingContext(),
            () -> Instant.EPOCH,
            Collections.emptyList()),
        ignored -> {});
  }

  private static Work.ProcessingContext createWorkProcessingContext() {
    return Work.createProcessingContext(
        "computationId", new FakeGetDataClient(), ignored -> {}, mock(HeartbeatSender.class));
  }

  private static WorkId workId(long workToken, long cacheToken) {
    return WorkId.builder().setCacheToken(cacheToken).setWorkToken(workToken).build();
  }

  private static Windmill.WorkItem createWorkItem(
      long workToken, long cacheToken, ShardedKey shardedKey) {
    return Windmill.WorkItem.newBuilder()
        .setShardingKey(shardedKey.shardingKey())
        .setKey(shardedKey.key())
        .setWorkToken(workToken)
        .setCacheToken(cacheToken)
        .build();
  }

  @Before
  public void setup() {
    Map<ShardedKey, Deque<ExecutableWork>> readWriteActiveWorkMap = new HashMap<>();
    // Only use readOnlyActiveWork to verify internal behavior in reaction to exposed API calls.
    readOnlyActiveWork = Collections.unmodifiableMap(readWriteActiveWorkMap);
    activeWorkState = ActiveWorkState.forTesting(readWriteActiveWorkMap, computationStateCache);
  }

  @Test
  public void testActivateWorkForKey_EXECUTE_unknownKey() {
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(
            createWork(createWorkItem(1L, 1L, shardedKey("someKey", 1L))));

    assertEquals(ActivateWorkResult.EXECUTE, activateWorkResult);
  }

  @Test
  public void testActivateWorkForKey_EXECUTE_emptyWorkQueueForKey() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);
    long workToken = 1L;
    long cacheToken = 2L;

    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(
            createWork(createWorkItem(workToken, cacheToken, shardedKey)));

    Optional<ExecutableWork> nextWorkForKey =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workId(workToken, cacheToken));

    assertEquals(ActivateWorkResult.EXECUTE, activateWorkResult);
    assertEquals(Optional.empty(), nextWorkForKey);
    assertThat(readOnlyActiveWork).doesNotContainKey(shardedKey);
  }

  @Test
  public void testActivateWorkForKey_DUPLICATE() {
    long workToken = 10L;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    // ActivateWork with the same shardedKey, and the same workTokens.
    activeWorkState.activateWorkForKey(createWork(createWorkItem(workToken, 1L, shardedKey)));
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(createWork(createWorkItem(workToken, 1L, shardedKey)));

    assertEquals(ActivateWorkResult.DUPLICATE, activateWorkResult);
  }

  @Test
  public void testActivateWorkForKey_QUEUED() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    // ActivateWork with the same shardedKey, but different workTokens.
    activeWorkState.activateWorkForKey(createWork(createWorkItem(1L, 1L, shardedKey)));
    ActivateWorkResult activateWorkResult =
        activeWorkState.activateWorkForKey(createWork(createWorkItem(2L, 1L, shardedKey)));

    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_noWorkQueueForKey() {
    assertEquals(
        Optional.empty(),
        activeWorkState.completeWorkAndGetNextWorkForKey(
            shardedKey("someKey", 1L), workId(1L, 1L)));
  }

  @Test
  public void
      testCompleteWorkAndGetNextWorkForKey_currentWorkInQueueWorkTokenDoesNotMatchWorkToComplete() {
    long workTokenInQueue = 2L;
    long otherWorkToken = 1L;
    long cacheToken = 1L;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork workInQueue =
        createWork(createWorkItem(workTokenInQueue, cacheToken, shardedKey));

    activeWorkState.activateWorkForKey(workInQueue);
    activeWorkState.completeWorkAndGetNextWorkForKey(
        shardedKey, workId(otherWorkToken, cacheToken));

    assertEquals(1, readOnlyActiveWork.get(shardedKey).size());
    assertEquals(workInQueue, readOnlyActiveWork.get(shardedKey).peek());
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_removesWorkFromQueueWhenComplete() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork activeWork = createWork(createWorkItem(1L, 1L, shardedKey));
    ExecutableWork nextWork = createWork(createWorkItem(2L, 2L, shardedKey));

    activeWorkState.activateWorkForKey(activeWork);
    activeWorkState.activateWorkForKey(nextWork);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, activeWork.id());

    assertEquals(nextWork, readOnlyActiveWork.get(shardedKey).peek());
    assertEquals(1, readOnlyActiveWork.get(shardedKey).size());
    assertFalse(readOnlyActiveWork.get(shardedKey).contains(activeWork));
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_removesQueueIfNoWorkPresent() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork workInQueue = createWork(createWorkItem(1L, 1L, shardedKey));

    activeWorkState.activateWorkForKey(workInQueue);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workInQueue.id());

    assertFalse(readOnlyActiveWork.containsKey(shardedKey));
  }

  @Test
  public void testCompleteWorkAndGetNextWorkForKey_returnsWorkIfPresent() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork workToBeCompleted = createWork(createWorkItem(1L, 1L, shardedKey));
    ExecutableWork nextWork = createWork(createWorkItem(2L, 2L, shardedKey));

    activeWorkState.activateWorkForKey(workToBeCompleted);
    activeWorkState.activateWorkForKey(nextWork);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workToBeCompleted.id());

    Optional<ExecutableWork> nextWorkOpt =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, workToBeCompleted.id());

    assertTrue(nextWorkOpt.isPresent());
    assertSame(nextWork, nextWorkOpt.get());

    Optional<ExecutableWork> endOfWorkQueue =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, nextWork.id());

    assertFalse(endOfWorkQueue.isPresent());
    assertFalse(readOnlyActiveWork.containsKey(shardedKey));
  }

  @Test
  public void testCurrentActiveWorkBudget_correctlyAggregatesActiveWorkBudget_oneShardKey() {
    ShardedKey shardedKey = shardedKey("someKey", 1L);
    ExecutableWork work1 = createWork(createWorkItem(1L, 1L, shardedKey));
    ExecutableWork work2 = createWork(createWorkItem(2L, 2L, shardedKey));

    activeWorkState.activateWorkForKey(work1);
    activeWorkState.activateWorkForKey(work2);

    GetWorkBudget expectedActiveBudget1 =
        GetWorkBudget.builder()
            .setItems(2)
            .setBytes(
                work1.getWorkItem().getSerializedSize() + work2.getWorkItem().getSerializedSize())
            .build();

    assertThat(activeWorkState.currentActiveWorkBudget()).isEqualTo(expectedActiveBudget1);

    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, work1.id());

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
    ExecutableWork work1 = createWork(createWorkItem(1L, 1L, shardedKey));
    ExecutableWork work2 = createWork(createWorkItem(2L, 2L, shardedKey));

    activeWorkState.activateWorkForKey(work1);
    activeWorkState.activateWorkForKey(work2);
    activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, work1.id());

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
    ExecutableWork work1 = createWork(createWorkItem(1L, 1L, shardedKey1));
    ExecutableWork work2 = createWork(createWorkItem(2L, 2L, shardedKey2));

    activeWorkState.activateWorkForKey(work1);
    activeWorkState.activateWorkForKey(work2);

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
    Map<ShardedKey, WorkId> invalidatedCommits = new HashMap<>();
    ShardedKey shardedKey1 = shardedKey("someKey", 1L);
    ShardedKey shardedKey2 = shardedKey("anotherKey", 2L);

    ExecutableWork stuckWork1 = expiredWork(createWorkItem(1L, 1L, shardedKey1));
    stuckWork1.work().setState(Work.State.COMMITTING);
    ExecutableWork stuckWork2 = expiredWork(createWorkItem(2L, 1L, shardedKey2));
    stuckWork2.work().setState(Work.State.COMMITTING);

    activeWorkState.activateWorkForKey(stuckWork1);
    activeWorkState.activateWorkForKey(stuckWork2);

    activeWorkState.invalidateStuckCommits(Instant.now(), invalidatedCommits::put);

    assertThat(invalidatedCommits).containsEntry(shardedKey1, stuckWork1.id());
    assertThat(invalidatedCommits).containsEntry(shardedKey2, stuckWork2.id());
    verify(computationStateCache).invalidate(shardedKey1.key(), shardedKey1.shardingKey());
    verify(computationStateCache).invalidate(shardedKey2.key(), shardedKey2.shardingKey());
  }

  @Test
  public void
      testActivateWorkForKey_withMatchingWorkTokenAndDifferentCacheToken_queuedWorkIsNotActive_QUEUED() {
    long workToken = 10L;
    long cacheToken1 = 5L;
    long cacheToken2 = cacheToken1 + 2L;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork firstWork = createWork(createWorkItem(workToken, cacheToken1, shardedKey));
    ExecutableWork secondWork = createWork(createWorkItem(workToken, cacheToken2, shardedKey));
    ExecutableWork differentWorkTokenWork = createWork(createWorkItem(1L, 1L, shardedKey));

    activeWorkState.activateWorkForKey(differentWorkTokenWork);
    // ActivateWork with the same shardedKey, and the same workTokens, but different cacheTokens.
    activeWorkState.activateWorkForKey(firstWork);
    ActivateWorkResult activateWorkResult = activeWorkState.activateWorkForKey(secondWork);

    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
    assertTrue(readOnlyActiveWork.get(shardedKey).contains(secondWork));

    Optional<ExecutableWork> nextWork =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, differentWorkTokenWork.id());
    assertTrue(nextWork.isPresent());
    assertSame(firstWork, nextWork.get());
    nextWork = activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, firstWork.id());
    assertTrue(nextWork.isPresent());
    assertSame(secondWork, nextWork.get());
  }

  @Test
  public void
      testActivateWorkForKey_withMatchingWorkTokenAndDifferentCacheToken_queuedWorkIsActive_QUEUED() {
    long workToken = 10L;
    long cacheToken1 = 5L;
    long cacheToken2 = 7L;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork firstWork = createWork(createWorkItem(workToken, cacheToken1, shardedKey));
    ExecutableWork secondWork = createWork(createWorkItem(workToken, cacheToken2, shardedKey));

    // ActivateWork with the same shardedKey, and the same workTokens, but different cacheTokens.
    activeWorkState.activateWorkForKey(firstWork);
    ActivateWorkResult activateWorkResult = activeWorkState.activateWorkForKey(secondWork);

    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
    assertEquals(firstWork, readOnlyActiveWork.get(shardedKey).peek());
    assertTrue(readOnlyActiveWork.get(shardedKey).contains(secondWork));
    Optional<ExecutableWork> nextWork =
        activeWorkState.completeWorkAndGetNextWorkForKey(shardedKey, firstWork.id());
    assertTrue(nextWork.isPresent());
    assertSame(secondWork, nextWork.get());
  }

  @Test
  public void
      testActivateWorkForKey_matchingCacheTokens_newWorkTokenGreater_queuedWorkIsActive_QUEUED() {
    long cacheToken = 1L;
    long newWorkToken = 10L;
    long queuedWorkToken = newWorkToken / 2;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork queuedWork = createWork(createWorkItem(queuedWorkToken, cacheToken, shardedKey));
    ExecutableWork newWork = createWork(createWorkItem(newWorkToken, cacheToken, shardedKey));

    activeWorkState.activateWorkForKey(queuedWork);
    ActivateWorkResult activateWorkResult = activeWorkState.activateWorkForKey(newWork);

    // newWork should be queued and queuedWork should not be removed since it is currently active.
    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
    assertTrue(readOnlyActiveWork.get(shardedKey).contains(newWork));
    assertEquals(queuedWork, readOnlyActiveWork.get(shardedKey).peek());
  }

  @Test
  public void
      testActivateWorkForKey_matchingCacheTokens_newWorkTokenGreater_queuedWorkNotActive_QUEUED() {
    long matchingCacheToken = 1L;
    long newWorkToken = 10L;
    long queuedWorkToken = newWorkToken / 2;

    ShardedKey shardedKey = shardedKey("someKey", 1L);
    ExecutableWork differentWorkTokenWork = createWork(createWorkItem(100L, 100L, shardedKey));
    ExecutableWork queuedWork =
        createWork(createWorkItem(queuedWorkToken, matchingCacheToken, shardedKey));
    ExecutableWork newWork =
        createWork(createWorkItem(newWorkToken, matchingCacheToken, shardedKey));

    activeWorkState.activateWorkForKey(differentWorkTokenWork);
    activeWorkState.activateWorkForKey(queuedWork);
    ActivateWorkResult activateWorkResult = activeWorkState.activateWorkForKey(newWork);

    assertEquals(ActivateWorkResult.QUEUED, activateWorkResult);
    assertTrue(readOnlyActiveWork.get(shardedKey).contains(newWork));
    assertFalse(readOnlyActiveWork.get(shardedKey).contains(queuedWork));
    assertEquals(differentWorkTokenWork, readOnlyActiveWork.get(shardedKey).peek());
  }

  @Test
  public void testActivateWorkForKey_matchingCacheTokens_newWorkTokenLesser_STALE() {
    long cacheToken = 1L;
    long queuedWorkToken = 10L;
    long newWorkToken = queuedWorkToken / 2;
    ShardedKey shardedKey = shardedKey("someKey", 1L);

    ExecutableWork queuedWork = createWork(createWorkItem(queuedWorkToken, cacheToken, shardedKey));
    ExecutableWork newWork = createWork(createWorkItem(newWorkToken, cacheToken, shardedKey));

    activeWorkState.activateWorkForKey(queuedWork);
    ActivateWorkResult activateWorkResult = activeWorkState.activateWorkForKey(newWork);

    assertEquals(ActivateWorkResult.STALE, activateWorkResult);
    assertFalse(readOnlyActiveWork.get(shardedKey).contains(newWork));
    assertEquals(queuedWork, readOnlyActiveWork.get(shardedKey).peek());
  }
}

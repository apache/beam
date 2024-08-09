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
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.api.services.dataflow.model.MapTask;
import com.google.common.truth.Correspondence;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ExecutableWork;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Watermarks;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.direct.Clock;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

@RunWith(JUnit4.class)
public class ActiveWorkRefresherTest {
  private static final Supplier<Instant> A_LONG_TIME_AGO =
      () -> Instant.parse("1998-09-04T00:00:00Z");
  private static final String COMPUTATION_ID_PREFIX = "ComputationId-";
  private final HeartbeatSender heartbeatSender = mock(HeartbeatSender.class);

  private static BoundedQueueExecutor workExecutor() {
    return new BoundedQueueExecutor(
        1,
        60,
        TimeUnit.SECONDS,
        1,
        10000000,
        new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
  }

  private static ComputationState createComputationState(int computationIdSuffix) {
    return createComputationState(
        computationIdSuffix, mock(WindmillStateCache.ForComputation.class));
  }

  private static ComputationState createComputationState(
      int computationIdSuffix, WindmillStateCache.ForComputation stateCache) {
    return new ComputationState(
        COMPUTATION_ID_PREFIX + computationIdSuffix,
        new MapTask().setStageName("stageName").setSystemName("systemName"),
        workExecutor(),
        new HashMap<>(),
        stateCache);
  }

  private ActiveWorkRefresher createActiveWorkRefresher(
      Supplier<Instant> clock,
      int activeWorkRefreshPeriodMillis,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations,
      ActiveWorkRefresher.HeartbeatTracker heartbeatTracker) {
    return new ActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        DataflowExecutionStateSampler.instance(),
        Executors.newSingleThreadScheduledExecutor(),
        heartbeatTracker);
  }

  private ExecutableWork createOldWork(int workIds, Consumer<Work> processWork) {
    ShardedKey shardedKey = ShardedKey.create(ByteString.EMPTY, workIds);
    return createOldWork(shardedKey, workIds, processWork);
  }

  private ExecutableWork createOldWork(
      ShardedKey shardedKey, int workIds, Consumer<Work> processWork) {
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder()
                .setKey(shardedKey.key())
                .setShardingKey(shardedKey.shardingKey())
                .setWorkToken(workIds)
                .setCacheToken(workIds)
                .build(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createProcessingContext(
                "computationId", new FakeGetDataClient(), ignored -> {}, heartbeatSender),
            A_LONG_TIME_AGO,
            ImmutableList.of()),
        processWork);
  }

  @Test
  public void testActiveWorkRefresh() throws InterruptedException {
    int activeWorkRefreshPeriodMillis = 100;

    // Block work processing to queue up the work.
    CountDownLatch workIsProcessed = new CountDownLatch(1);
    Consumer<Work> processWork =
        ignored -> {
          try {
            workIsProcessed.await();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        };

    List<ComputationState> computations = new ArrayList<>();
    Map<String, List<ExecutableWork>> computationsAndWork = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      ComputationState computationState = createComputationState(i);
      ExecutableWork fakeWork = createOldWork(i, processWork);
      computationState.activateWork(fakeWork);

      computations.add(computationState);
      List<ExecutableWork> activeWorkForComputation =
          computationsAndWork.computeIfAbsent(
              computationState.getComputationId(), ignored -> new ArrayList<>());
      activeWorkForComputation.add(fakeWork);
    }

    CountDownLatch heartbeatsSent = new CountDownLatch(1);
    TestClock fakeClock = new TestClock(Instant.now());
    ActiveWorkRefresher activeWorkRefresher =
        createActiveWorkRefresher(
            fakeClock::now,
            activeWorkRefreshPeriodMillis,
            0,
            () -> computations,
            heartbeats -> heartbeatsSent::countDown);

    ArgumentCaptor<Heartbeats> heartbeatsCaptor = ArgumentCaptor.forClass(Heartbeats.class);
    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(activeWorkRefreshPeriodMillis * 2));
    heartbeatsSent.await();
    activeWorkRefresher.stop();
    verify(heartbeatSender).sendHeartbeats(heartbeatsCaptor.capture());
    Heartbeats fanoutExpectedHeartbeats = heartbeatsCaptor.getValue();
    assertThat(computationsAndWork.size())
        .isEqualTo(fanoutExpectedHeartbeats.heartbeatRequests().size());

    for (Map.Entry<String, Collection<Windmill.HeartbeatRequest>> expectedHeartbeat :
        fanoutExpectedHeartbeats.heartbeatRequests().asMap().entrySet()) {
      String computationId = expectedHeartbeat.getKey();
      Collection<Windmill.HeartbeatRequest> heartbeatRequests = expectedHeartbeat.getValue();
      List<Work> work =
          computationsAndWork.get(computationId).stream()
              .map(ExecutableWork::work)
              .collect(Collectors.toList());
      // Compare the heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys.
      assertThat(heartbeatRequests)
          .comparingElementsUsing(
              Correspondence.from(
                  (Windmill.HeartbeatRequest h, Work w) -> {
                    assert h != null;
                    assert w != null;
                    return h.getWorkToken() == w.getWorkItem().getWorkToken()
                        && h.getCacheToken() == w.getWorkItem().getWorkToken()
                        && h.getShardingKey() == w.getWorkItem().getShardingKey();
                  },
                  "heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys should be equal."))
          .containsExactlyElementsIn(work);
    }

    activeWorkRefresher.stop();
    // Free the work processing threads.
    workIsProcessed.countDown();
  }

  @Test
  public void testEmptyActiveWorkRefresh() throws InterruptedException {
    int activeWorkRefreshPeriodMillis = 100;

    List<ComputationState> computations = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ComputationState computationState = createComputationState(i);
      computations.add(computationState);
    }

    CountDownLatch heartbeatsSent = new CountDownLatch(1);
    TestClock fakeClock = new TestClock(Instant.now());
    ActiveWorkRefresher activeWorkRefresher =
        createActiveWorkRefresher(
            fakeClock::now,
            activeWorkRefreshPeriodMillis,
            0,
            () -> computations,
            heartbeats -> heartbeatsSent::countDown);

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(activeWorkRefreshPeriodMillis * 2));
    assertFalse(heartbeatsSent.await(500, TimeUnit.MILLISECONDS));
    activeWorkRefresher.stop();
  }

  @Test
  public void testInvalidateStuckCommits() throws InterruptedException {
    int stuckCommitDurationMillis = 100;
    Table<ComputationState, ExecutableWork, WindmillStateCache.ForComputation> computations =
        HashBasedTable.create();
    WindmillStateCache stateCache = WindmillStateCache.builder().setSizeMb(100).build();
    ByteString key = ByteString.EMPTY;
    for (int i = 0; i < 5; i++) {
      WindmillStateCache.ForComputation perComputationStateCache =
          spy(stateCache.forComputation(COMPUTATION_ID_PREFIX + i));
      ComputationState computationState = spy(createComputationState(i, perComputationStateCache));
      ExecutableWork fakeWork = createOldWork(ShardedKey.create(key, i), i, ignored -> {});
      fakeWork.work().setState(Work.State.COMMITTING);
      computationState.activateWork(fakeWork);
      computations.put(computationState, fakeWork, perComputationStateCache);
    }

    TestClock fakeClock = new TestClock(Instant.now());
    CountDownLatch invalidateStuckCommitRan = new CountDownLatch(computations.size());

    // Count down the latch every time to avoid waiting/sleeping arbitrarily.
    for (ComputationState computation : computations.rowKeySet()) {
      doAnswer(
              invocation -> {
                invocation.callRealMethod();
                invalidateStuckCommitRan.countDown();
                return null;
              })
          .when(computation)
          .invalidateStuckCommits(any(Instant.class));
    }

    ActiveWorkRefresher activeWorkRefresher =
        createActiveWorkRefresher(
            fakeClock::now,
            0,
            stuckCommitDurationMillis,
            computations.rowMap()::keySet,
            ignored -> () -> {});

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(stuckCommitDurationMillis));
    invalidateStuckCommitRan.await();
    activeWorkRefresher.stop();

    for (Table.Cell<ComputationState, ExecutableWork, WindmillStateCache.ForComputation> cell :
        computations.cellSet()) {
      ComputationState computation = cell.getRowKey();
      ExecutableWork work = cell.getColumnKey();
      WindmillStateCache.ForComputation perComputationStateCache = cell.getValue();
      verify(perComputationStateCache, times(1))
          .invalidate(eq(key), eq(work.getWorkItem().getShardingKey()));
      verify(computation, times(1))
          .completeWorkAndScheduleNextWorkForKey(
              eq(ShardedKey.create(key, work.getWorkItem().getShardingKey())), eq(work.id()));
    }
  }

  static class TestClock implements Clock {
    private Instant time;

    private TestClock(Instant startTime) {
      this.time = startTime;
    }

    private synchronized void advance(Duration amount) {
      time = time.plus(amount);
    }

    @Override
    public synchronized Instant now() {
      return time;
    }
  }
}

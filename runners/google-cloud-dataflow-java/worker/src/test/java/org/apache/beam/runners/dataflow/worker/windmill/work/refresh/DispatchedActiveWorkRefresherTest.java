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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import com.google.common.truth.Correspondence;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.beam.runners.dataflow.worker.DataflowExecutionStateSampler;
import org.apache.beam.runners.dataflow.worker.streaming.ComputationState;
import org.apache.beam.runners.dataflow.worker.streaming.ShardedKey;
import org.apache.beam.runners.dataflow.worker.streaming.Work;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill.HeartbeatRequest;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkProcessingContext;
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

@RunWith(JUnit4.class)
public class DispatchedActiveWorkRefresherTest {

  private static final Supplier<Instant> A_LONG_TIME_AGO =
      () -> Instant.parse("1998-09-04T00:00:00Z");
  private static final String COMPUTATION_ID_PREFIX = "ComputationId-";

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
      Consumer<Map<String, List<HeartbeatRequest>>> activeWorkRefresherFn) {
    return new DispatchedActiveWorkRefresher(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        DataflowExecutionStateSampler.instance(),
        activeWorkRefresherFn,
        Executors.newSingleThreadScheduledExecutor());
  }

  private Work createOldWork(int workIds, Consumer<Work> processWork) {
    WorkCommitter workCommitter = mock(WorkCommitter.class);
    doNothing().when(workCommitter).commit(any(Commit.class));
    WindmillStream.GetDataStream getDataStream = mock(WindmillStream.GetDataStream.class);
    when(getDataStream.requestKeyedData(anyString(), any()))
        .thenReturn(Windmill.KeyedGetDataResponse.getDefaultInstance());
    return Work.create(
        WorkProcessingContext.builder()
            .setWorkItem(
                Windmill.WorkItem.newBuilder()
                    .setKey(ByteString.EMPTY)
                    .setShardingKey(workIds)
                    .setWorkToken(workIds)
                    .setCacheToken(workIds)
                    .build())
            .setWorkCommitter(workCommitter::commit)
            .setComputationId("computation")
            .setInputDataWatermark(Instant.EPOCH)
            .setKeyedDataFetcher(
                request ->
                    Optional.ofNullable(getDataStream.requestKeyedData("computationId", request)))
            .build(),
        DispatchedActiveWorkRefresherTest.A_LONG_TIME_AGO,
        ImmutableList.of(),
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
    Map<String, List<Work>> computationsAndWork = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      ComputationState computationState = createComputationState(i);
      Work fakeWork = createOldWork(i, processWork);
      computationState.activateWork(ShardedKey.create(ByteString.EMPTY, i), fakeWork);

      computations.add(computationState);
      List<Work> activeWorkForComputation =
          computationsAndWork.computeIfAbsent(
              computationState.getComputationId(), ignored -> new ArrayList<>());
      activeWorkForComputation.add(fakeWork);
    }

    Map<String, List<HeartbeatRequest>> expectedHeartbeats = new HashMap<>();
    CountDownLatch heartbeatsSent = new CountDownLatch(1);
    TestClock fakeClock = new TestClock(Instant.now());

    ActiveWorkRefresher activeWorkRefresher =
        createActiveWorkRefresher(
            fakeClock::now,
            activeWorkRefreshPeriodMillis,
            0,
            () -> computations,
            heartbeats -> {
              expectedHeartbeats.putAll(heartbeats);
              heartbeatsSent.countDown();
            });

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(activeWorkRefreshPeriodMillis * 2));
    heartbeatsSent.await();
    activeWorkRefresher.stop();

    assertThat(computationsAndWork.size()).isEqualTo(expectedHeartbeats.size());
    for (Map.Entry<String, List<HeartbeatRequest>> expectedHeartbeat :
        expectedHeartbeats.entrySet()) {
      String computationId = expectedHeartbeat.getKey();
      List<HeartbeatRequest> heartbeatRequests = expectedHeartbeat.getValue();
      List<Work> work = computationsAndWork.get(computationId);

      // Compare the heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys.
      assertThat(heartbeatRequests)
          .comparingElementsUsing(
              Correspondence.from(
                  (HeartbeatRequest h, Work w) ->
                      h.getWorkToken() == w.getWorkItem().getWorkToken()
                          && h.getCacheToken() == w.getWorkItem().getWorkToken()
                          && h.getShardingKey() == w.getWorkItem().getShardingKey(),
                  "heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys should be equal."))
          .containsExactlyElementsIn(work);
    }

    activeWorkRefresher.stop();
    // Free the work processing threads.
    workIsProcessed.countDown();
  }

  @Test
  public void testInvalidateStuckCommits() throws InterruptedException {
    int stuckCommitDurationMillis = 100;
    Table<ComputationState, Work, WindmillStateCache.ForComputation> computations =
        HashBasedTable.create();
    WindmillStateCache stateCache = WindmillStateCache.ofSizeMbs(100);
    ByteString key = ByteString.EMPTY;
    for (int i = 0; i < 5; i++) {
      WindmillStateCache.ForComputation perComputationStateCache =
          spy(stateCache.forComputation(COMPUTATION_ID_PREFIX + i));
      ComputationState computationState = spy(createComputationState(i, perComputationStateCache));
      Work fakeWork = createOldWork(i, ignored -> {});
      fakeWork.setState(Work.State.COMMITTING);
      computationState.activateWork(ShardedKey.create(key, i), fakeWork);
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
            ignored -> {});

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(stuckCommitDurationMillis));
    invalidateStuckCommitRan.await();
    activeWorkRefresher.stop();

    for (Table.Cell<ComputationState, Work, WindmillStateCache.ForComputation> cell :
        computations.cellSet()) {
      ComputationState computation = cell.getRowKey();
      Work work = cell.getColumnKey();
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

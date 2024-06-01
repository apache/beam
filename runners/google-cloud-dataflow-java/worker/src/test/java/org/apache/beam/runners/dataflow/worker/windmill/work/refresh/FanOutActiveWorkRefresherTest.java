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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.direct.Clock;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FanOutActiveWorkRefresherTest {
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
      Consumer<Map<WindmillStream.GetDataStream, Map<String, List<Windmill.HeartbeatRequest>>>>
          activeWorkRefresherFn) {
    return FanOutActiveWorkRefresher.forTesting(
        clock,
        activeWorkRefreshPeriodMillis,
        stuckCommitDurationMillis,
        computations,
        DataflowExecutionStateSampler.instance(),
        activeWorkRefresherFn,
        Executors.newSingleThreadScheduledExecutor());
  }

  private ExecutableWork createOldWork(int workIds, Consumer<Work> processWork) {
    ShardedKey shardedKey = ShardedKey.create(ByteString.EMPTY, workIds);
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder()
                .setWorkToken(workIds)
                .setCacheToken(workIds)
                .setKey(shardedKey.key())
                .setShardingKey(shardedKey.shardingKey())
                .build(),
            Watermarks.builder().setInputDataWatermark(Instant.EPOCH).build(),
            Work.createFanOutProcessingContext(
                "computation",
                (computationId, request) -> Windmill.KeyedGetDataResponse.getDefaultInstance(),
                ignored -> {},
                mock(WindmillStream.GetDataStream.class)),
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
      when(fakeWork.work().getDataStream().isClosed()).thenReturn(false);
    }

    Map<WindmillStream.GetDataStream, Map<String, List<Windmill.HeartbeatRequest>>>
        fanoutExpectedHeartbeats = new HashMap<>();
    CountDownLatch heartbeatsSent = new CountDownLatch(1);
    TestClock fakeClock = new TestClock(Instant.now());
    ActiveWorkRefresher activeWorkRefresher =
        createActiveWorkRefresher(
            fakeClock::now,
            activeWorkRefreshPeriodMillis,
            0,
            () -> computations,
            heartbeats -> {
              fanoutExpectedHeartbeats.putAll(heartbeats);
              heartbeatsSent.countDown();
            });

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(activeWorkRefreshPeriodMillis * 2));
    heartbeatsSent.await();
    activeWorkRefresher.stop();

    assertThat(computationsAndWork.size()).isEqualTo(fanoutExpectedHeartbeats.size());
    for (Map.Entry<WindmillStream.GetDataStream, Map<String, List<Windmill.HeartbeatRequest>>>
        fanOutExpectedHeartbeat : fanoutExpectedHeartbeats.entrySet()) {
      for (Map.Entry<String, List<Windmill.HeartbeatRequest>> expectedHeartbeat :
          fanOutExpectedHeartbeat.getValue().entrySet()) {
        String computationId = expectedHeartbeat.getKey();
        List<Windmill.HeartbeatRequest> heartbeatRequests = expectedHeartbeat.getValue();
        List<Work> work =
            computationsAndWork.get(computationId).stream()
                .map(ExecutableWork::work)
                .collect(Collectors.toList());
        // Compare the heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys.
        assertThat(heartbeatRequests)
            .comparingElementsUsing(
                Correspondence.from(
                    (Windmill.HeartbeatRequest h, Work w) ->
                        h.getWorkToken() == w.getWorkItem().getWorkToken()
                            && h.getCacheToken() == w.getWorkItem().getWorkToken()
                            && h.getShardingKey() == w.getWorkItem().getShardingKey(),
                    "heartbeatRequest's and Work's workTokens, cacheTokens, and shardingKeys should be equal."))
            .containsExactlyElementsIn(work);
      }
    }

    activeWorkRefresher.stop();
    // Free the work processing threads.
    workIsProcessed.countDown();
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

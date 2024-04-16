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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
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
import org.apache.beam.runners.dataflow.worker.streaming.WorkId;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.WindmillStream;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.Commit;
import org.apache.beam.runners.dataflow.worker.windmill.client.commits.WorkCommitter;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.WorkProcessingContext;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.HashBasedTable;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Table;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

@RunWith(Parameterized.class)
public class ActiveWorkRefresherTest {
  private static final String COMPUTATION_ID_PREFIX = "ComputationId-";
  private static final Supplier<Instant> A_LONG_TIME_AGO =
      () -> Instant.parse("1998-09-04T00:00:00Z");

  @Parameterized.Parameter() public boolean isDirectActiveWorkRefresher;

  private static BoundedQueueExecutor workExecutor() {
    return new BoundedQueueExecutor(
        1,
        60,
        TimeUnit.SECONDS,
        1,
        10000000,
        new ThreadFactoryBuilder().setNameFormat("DataflowWorkUnits-%d").setDaemon(true).build());
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

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {{false}, {true}});
  }

  private ActiveWorkRefresher createActiveWorkRefresher(
      Supplier<Instant> clock,
      int stuckCommitDurationMillis,
      Supplier<Collection<ComputationState>> computations) {
    return isDirectActiveWorkRefresher
        ? DirectActiveWorkRefresher.forTesting(
            clock,
            0,
            stuckCommitDurationMillis,
            computations,
            DataflowExecutionStateSampler.instance(),
            ignored -> {},
            Executors.newSingleThreadScheduledExecutor())
        : DispatchedActiveWorkRefresher.create(
            clock,
            0,
            stuckCommitDurationMillis,
            computations,
            DataflowExecutionStateSampler.instance(),
            ignored -> {},
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
                    .setWorkToken(workIds)
                    .setCacheToken(workIds)
                    .setKey(ByteString.EMPTY)
                    .setShardingKey(workIds)
                    .build())
            .setWorkCommitter(workCommitter::commit)
            .setInputDataWatermark(Instant.EPOCH)
            .setComputationId("computation")
            .setGetDataStream(getDataStream)
            .setKeyedDataFetcher(
                request ->
                    Optional.ofNullable(getDataStream.requestKeyedData("computationId", request)))
            .build(),
        A_LONG_TIME_AGO,
        ImmutableList.of(),
        processWork);
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

    WorkRefreshTestClock fakeClock = new WorkRefreshTestClock(Instant.now());
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
            fakeClock::now, stuckCommitDurationMillis, computations.rowMap()::keySet);

    activeWorkRefresher.start();
    fakeClock.advance(Duration.millis(stuckCommitDurationMillis));
    invalidateStuckCommitRan.await();

    // Capture all invalidate and completeWork args.
    ArgumentCaptor<ByteString> keyArgumentCaptor = ArgumentCaptor.forClass(ByteString.class);
    ArgumentCaptor<Long> shardingKeyArgumentCaptor = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<ShardedKey> shardedKeyArgumentCaptor = ArgumentCaptor.forClass(ShardedKey.class);
    ArgumentCaptor<WorkId> workIdArgumentCaptor = ArgumentCaptor.forClass(WorkId.class);
    for (Table.Cell<ComputationState, Work, WindmillStateCache.ForComputation> cell :
        computations.cellSet()) {
      ComputationState computationState = cell.getRowKey();
      WindmillStateCache.ForComputation perComputationStateCache = cell.getValue();
      verify(perComputationStateCache, times(1))
          .invalidate(keyArgumentCaptor.capture(), shardingKeyArgumentCaptor.capture());
      verify(computationState, times(1))
          .completeWorkAndScheduleNextWorkForKey(
              shardedKeyArgumentCaptor.capture(), workIdArgumentCaptor.capture());
    }

    activeWorkRefresher.stop();

    // Assert all invalidate and completeWork args.
    for (Table.Cell<ComputationState, Work, WindmillStateCache.ForComputation> cell :
        computations.cellSet()) {
      Work work = cell.getColumnKey();
      assertThat(keyArgumentCaptor.getAllValues()).contains(key);
      assertThat(shardingKeyArgumentCaptor.getAllValues())
          .contains(work.getWorkItem().getShardingKey());
      assertThat(shardedKeyArgumentCaptor.getAllValues())
          .contains(ShardedKey.create(key, work.getWorkItem().getShardingKey()));
      assertThat(workIdArgumentCaptor.getAllValues()).contains(work.id());
    }
  }
}

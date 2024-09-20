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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.client.getdata.FakeGetDataClient;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.runners.dataflow.worker.windmill.work.refresh.HeartbeatSender;
import org.apache.beam.sdk.fn.IdGenerators;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComputationStateCacheTest {
  private final BoundedQueueExecutor workExecutor = mock(BoundedQueueExecutor.class);
  private final WindmillStateCache.ForComputation stateCache =
      mock(WindmillStateCache.ForComputation.class);
  private final ComputationConfig.Fetcher configFetcher = mock(ComputationConfig.Fetcher.class);
  private ComputationStateCache computationStateCache;

  private static ExecutableWork createWork(ShardedKey shardedKey, long workToken, long cacheToken) {
    return ExecutableWork.create(
        Work.create(
            Windmill.WorkItem.newBuilder()
                .setKey(shardedKey.key())
                .setShardingKey(shardedKey.shardingKey())
                .setWorkToken(workToken)
                .setCacheToken(cacheToken)
                .build(),
            Watermarks.builder().setInputDataWatermark(Instant.now()).build(),
            Work.createProcessingContext(
                "computationId",
                new FakeGetDataClient(),
                ignored -> {},
                mock(HeartbeatSender.class)),
            Instant::now,
            Collections.emptyList()),
        ignored -> {});
  }

  @Before
  public void setUp() {
    computationStateCache =
        ComputationStateCache.create(
            configFetcher, workExecutor, ignored -> stateCache, IdGenerators.decrementingLongs());
  }

  @Test
  public void testGet_computationStateNotCached() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(mapTask, userTransformToStateFamilyName, ImmutableMap.of());
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertTrue(computationState.isPresent());
    assertThat(computationState.get().getComputationId()).isEqualTo(computationId);
    assertThat(computationState.get().getMapTask()).isEqualTo(mapTask);
    assertThat(computationState.get().getTransformUserNameToStateFamily())
        .isEqualTo(userTransformToStateFamilyName);
  }

  @Test
  public void testGet_computationStateCached() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(mapTask, userTransformToStateFamilyName, ImmutableMap.of());
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    computationStateCache.get(computationId);
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertTrue(computationState.isPresent());
    assertThat(computationState.get().getComputationId()).isEqualTo(computationId);
    assertThat(computationState.get().getMapTask()).isEqualTo(mapTask);
    assertThat(computationState.get().getTransformUserNameToStateFamily())
        .isEqualTo(userTransformToStateFamilyName);
  }

  @Test
  public void testGet_computationStateNotCachedOrFetchable() {
    String computationId = "computationId";
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.empty());
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertFalse(computationState.isPresent());
    // Fetch again to make sure we call configFetcher and nulls/empty are not cached.
    Optional<ComputationState> computationState2 = computationStateCache.get(computationId);
    assertFalse(computationState2.isPresent());
    verify(configFetcher, times(2)).fetchConfig(eq(computationId));
  }

  @Test
  public void testGet_computationStateConfigFetcherFailure() {
    String computationId = "computationId";
    when(configFetcher.fetchConfig(eq(computationId))).thenThrow(new RuntimeException());
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertFalse(computationState.isPresent());
    // Fetch again to make sure we call configFetcher and nulls/empty are not cached.
    Optional<ComputationState> computationState2 = computationStateCache.get(computationId);
    assertFalse(computationState2.isPresent());
    verify(configFetcher, times(2)).fetchConfig(eq(computationId));
  }

  @Test
  public void testGet_usesUserTransformToStateFamilyNameIfNotEmpty() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(
            mapTask, userTransformToStateFamilyName, ImmutableMap.of("stateName1", "stateName"));
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertTrue(computationState.isPresent());
    assertThat(computationState.get().getComputationId()).isEqualTo(computationId);
    assertThat(computationState.get().getMapTask()).isEqualTo(mapTask);
    assertThat(computationState.get().getTransformUserNameToStateFamily())
        .isEqualTo(userTransformToStateFamilyName);
  }

  @Test
  public void testGet_defaultsToStateNameMapWhenUserTransformToStateFamilyNameEmpty() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    ComputationConfig computationConfig =
        ComputationConfig.create(
            mapTask, ImmutableMap.of(), ImmutableMap.of("stateName1", "stateName"));
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    Optional<ComputationState> computationState = computationStateCache.get(computationId);
    assertTrue(computationState.isPresent());
    assertThat(computationState.get().getComputationId()).isEqualTo(computationId);
    assertThat(computationState.get().getMapTask()).isEqualTo(mapTask);
    assertThat(computationState.get().getTransformUserNameToStateFamily())
        .isEqualTo(computationStateCache.getGlobalUsernameToStateFamilyNameMap());
  }

  @Test
  public void testGet_buildsStateNameMap() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> stateNameMap = ImmutableMap.of("stateName1", "stateName");
    ComputationConfig computationConfig = ComputationConfig.create(mapTask, null, stateNameMap);
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    computationStateCache.get(computationId);
    assertThat(computationStateCache.getGlobalUsernameToStateFamilyNameMap())
        .containsExactlyEntriesIn(stateNameMap);
  }

  @Test
  public void testGetIfPresent_computationStateCached() {
    String computationId = "computationId";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(mapTask, userTransformToStateFamilyName, ImmutableMap.of());
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    computationStateCache.get(computationId);
    Optional<ComputationState> computationState = computationStateCache.getIfPresent(computationId);
    assertTrue(computationState.isPresent());
    assertThat(computationState.get().getComputationId()).isEqualTo(computationId);
    assertThat(computationState.get().getMapTask()).isEqualTo(mapTask);
    assertThat(computationState.get().getTransformUserNameToStateFamily())
        .isEqualTo(userTransformToStateFamilyName);
  }

  @Test
  public void testGetIfPresent_computationStateNotCached() {
    Optional<ComputationState> computationState =
        computationStateCache.getIfPresent("computationId");
    assertFalse(computationState.isPresent());
    verifyNoInteractions(configFetcher);
  }

  @Test
  public void testGetAllPresentComputations() {
    String computationId1 = "computationId1";
    String computationId2 = "computationId2";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(mapTask, userTransformToStateFamilyName, ImmutableMap.of());
    when(configFetcher.fetchConfig(eq(computationId1))).thenReturn(Optional.of(computationConfig));
    when(configFetcher.fetchConfig(eq(computationId2))).thenReturn(Optional.of(computationConfig));

    computationStateCache.get(computationId1);
    computationStateCache.get(computationId2);
    Set<String> expectedComputationIds = ImmutableSet.of(computationId1, computationId2);
    Set<String> actualComputationIds =
        computationStateCache.getAllPresentComputations().stream()
            .map(ComputationState::getComputationId)
            .collect(Collectors.toSet());
    assertThat(actualComputationIds).containsExactlyElementsIn(expectedComputationIds);
    computationStateCache
        .getAllPresentComputations()
        .forEach(
            computationState -> {
              assertThat(expectedComputationIds).contains(computationState.getComputationId());
              assertThat(computationState.getMapTask()).isEqualTo(mapTask);
              assertThat(computationState.getTransformUserNameToStateFamily())
                  .isEqualTo(userTransformToStateFamilyName);
            });
  }

  @Test
  public void testTotalCurrentActiveGetWorkBudget() {
    String computationId = "computationId";
    String computationId2 = "computationId2";
    MapTask mapTask = new MapTask().setStageName("stageName").setSystemName("systemName");
    Map<String, String> userTransformToStateFamilyName =
        ImmutableMap.of("userTransformName", "stateFamilyName");
    ComputationConfig computationConfig =
        ComputationConfig.create(mapTask, userTransformToStateFamilyName, ImmutableMap.of());
    when(configFetcher.fetchConfig(eq(computationId))).thenReturn(Optional.of(computationConfig));
    when(configFetcher.fetchConfig(eq(computationId2))).thenReturn(Optional.of(computationConfig));
    ShardedKey shardedKey = ShardedKey.create(ByteString.EMPTY, 1);
    ShardedKey shardedKey2 = ShardedKey.create(ByteString.EMPTY, 2);

    ExecutableWork work1 = createWork(shardedKey, 1, 1);
    ExecutableWork work2 = createWork(shardedKey2, 2, 2);
    ExecutableWork work3 = createWork(shardedKey2, 3, 3);

    // Activate 1 Work for computationId
    Optional<ComputationState> maybeComputationState = computationStateCache.get(computationId);
    assertTrue(maybeComputationState.isPresent());
    ComputationState computationState = maybeComputationState.get();
    computationState.activateWork(work1);

    // Activate 2 Work(s) for computationId2
    Optional<ComputationState> maybeComputationState2 = computationStateCache.get(computationId);
    assertTrue(maybeComputationState2.isPresent());
    ComputationState computationState2 = maybeComputationState2.get();
    computationState2.activateWork(work2);
    computationState2.activateWork(work3);

    // GetWorkBudget should have 3 items. 1 from computationId, 2 from computationId2.
    assertThat(computationStateCache.totalCurrentActiveGetWorkBudget())
        .isEqualTo(
            GetWorkBudget.builder()
                .setItems(3)
                .setBytes(
                    work1.getWorkItem().getSerializedSize()
                        + work2.getWorkItem().getSerializedSize()
                        + work3.getWorkItem().getSerializedSize())
                .build());
  }
}

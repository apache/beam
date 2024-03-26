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
package org.apache.beam.runners.dataflow.worker.streaming.computations;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.StreamingComputationConfig;
import com.google.api.services.dataflow.model.StreamingConfigTask;
import com.google.api.services.dataflow.model.WorkItem;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import org.apache.beam.runners.dataflow.worker.WorkUnitClient;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEngineConfigLoader;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingEngineComputationStateCacheLoaderTest {
  private final BoundedQueueExecutor workExecutor = mock(BoundedQueueExecutor.class);
  private final WorkUnitClient mockDataflowServiceClient = mock(WorkUnitClient.class);
  private final StreamingEngineConfigLoader streamingEngineConfigLoader =
      StreamingEngineConfigLoader.forTesting(
          true,
          0,
          mockDataflowServiceClient,
          ignored -> Executors.newSingleThreadScheduledExecutor(),
          ignored -> {});
  private final StreamingEngineComputationStateCacheLoader cacheLoader =
      new StreamingEngineComputationStateCacheLoader(
          streamingEngineConfigLoader,
          workExecutor,
          ignored -> mock(WindmillStateCache.ForComputation.class));
  private final LoadingCache<String, Optional<ComputationState>> cache =
      CacheBuilder.newBuilder().build(cacheLoader);

  @Test
  public void testLoad_loadsAllKeys() throws IOException, ExecutionException {
    List<StreamingComputationConfig> streamingComputations = new ArrayList<>();
    List<StreamingComputationConfig> streamingConfigTasks = new ArrayList<>();
    StreamingConfigTask streamingConfigTask =
        new StreamingConfigTask().setStreamingComputationConfigs(streamingConfigTasks);
    when(mockDataflowServiceClient.getStreamingConfigWorkItem(anyString()))
        .thenReturn(Optional.of(new WorkItem().setStreamingConfigTask(streamingConfigTask)));
    for (int i = 0; i < 5; i++) {
      String computationId = "computation" + i;
      StreamingComputationConfig streamingComputationConfig =
          new StreamingComputationConfig()
              .setComputationId(computationId)
              .setStageName("stageName" + i)
              .setSystemName("systemName" + i)
              .setInstructions(ImmutableList.of());
      streamingConfigTasks.add(streamingComputationConfig);
      streamingComputations.add(streamingComputationConfig);
    }

    // One get should load all the values.
    cache.getAll(ImmutableList.of(Iterables.getLast(streamingComputations).getComputationId()));

    for (StreamingComputationConfig streamingComputationConfig : streamingComputations) {
      Optional<ComputationState> cachedComputation =
          cache.getUnchecked(streamingComputationConfig.getComputationId());
      assertTrue(cachedComputation.isPresent());
      ComputationState computationState = cachedComputation.get();
      assertThat(computationState.getComputationId())
          .isEqualTo(streamingComputationConfig.getComputationId());
      assertThat(computationState.getMapTask().getStageName())
          .isEqualTo(streamingComputationConfig.getStageName());
      assertThat(computationState.getMapTask().getSystemName())
          .isEqualTo(streamingComputationConfig.getSystemName());
    }

    verify(mockDataflowServiceClient, times(1)).getStreamingConfigWorkItem(anyString());
  }
}

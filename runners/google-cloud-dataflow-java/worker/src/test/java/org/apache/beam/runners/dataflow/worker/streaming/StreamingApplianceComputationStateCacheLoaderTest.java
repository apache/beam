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
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingApplianceConfigLoader;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.WindmillServerStub;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StreamingApplianceComputationStateCacheLoaderTest {
  private final BoundedQueueExecutor workExecutor = mock(BoundedQueueExecutor.class);
  private final WindmillServerStub mockWindmillServer = mock(WindmillServerStub.class);
  private final StreamingApplianceConfigLoader streamingApplianceConfigLoader =
      new StreamingApplianceConfigLoader(mockWindmillServer, ignored -> {});
  private final StreamingApplianceComputationStateCacheLoader cacheLoader =
      new StreamingApplianceComputationStateCacheLoader(
          streamingApplianceConfigLoader,
          workExecutor,
          ignored -> mock(WindmillStateCache.ForComputation.class));
  private final LoadingCache<String, Optional<ComputationState>> cache =
      CacheBuilder.newBuilder().build(cacheLoader);

  private static MapTask mapTask(int suffix) {
    return new MapTask().setSystemName("system" + suffix).setStageName("stage" + suffix);
  }

  @Test
  public void testLoad_loadsAllKeys() throws ExecutionException {
    Map<String, MapTask> mapTasks = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      MapTask mapTask = mapTask(i);
      mapTasks.put(mapTask.getSystemName(), mapTask);
    }
    List<String> serializedMapTasks =
        mapTasks.values().stream()
            .map(
                mapTask -> {
                  try {
                    return Transport.getJsonFactory().toString(mapTask);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

    when(mockWindmillServer.getConfig(any()))
        .thenReturn(
            Windmill.GetConfigResponse.newBuilder().addAllCloudWorks(serializedMapTasks).build());

    // One get should load all the values.
    cache.getAll(ImmutableList.of(Iterables.getLast(mapTasks.keySet())));

    for (Map.Entry<String, MapTask> mapTaskEntry : mapTasks.entrySet()) {
      Optional<ComputationState> cachedComputation = cache.getUnchecked(mapTaskEntry.getKey());
      assertTrue(cachedComputation.isPresent());
      ComputationState computationState = cachedComputation.get();
      assertThat(computationState.getComputationId()).isEqualTo(mapTaskEntry.getKey());
      assertThat(computationState.getMapTask()).isEqualTo(mapTaskEntry.getValue());
    }

    verify(mockWindmillServer, times(1)).getConfig(any());
  }
}

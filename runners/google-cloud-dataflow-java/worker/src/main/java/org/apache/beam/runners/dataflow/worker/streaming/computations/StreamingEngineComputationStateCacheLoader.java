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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.api.services.dataflow.model.MapTask;
import com.google.api.services.dataflow.model.StreamingComputationConfig;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingEnginePipelineConfig;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingEnvironment;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;

/**
 * Creates {@link ComputationState}(s) and loads them into {@link ComputationStateCache} w/
 * configuration backed by Streaming Engine.
 */
@Internal
public final class StreamingEngineComputationStateCacheLoader
    extends CacheLoader<String, Optional<ComputationState>> {

  private final StreamingConfigLoader<StreamingEnginePipelineConfig> streamingEngineConfigLoader;
  private final BoundedQueueExecutor workUnitExecutor;
  private final Function<String, WindmillStateCache.ForComputation>
      perComputationStateCacheViewFactory;

  public StreamingEngineComputationStateCacheLoader(
      StreamingConfigLoader<StreamingEnginePipelineConfig> streamingEngineConfigLoader,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
    this.streamingEngineConfigLoader = streamingEngineConfigLoader;
    this.workUnitExecutor = workUnitExecutor;
    this.perComputationStateCacheViewFactory = perComputationStateCacheViewFactory;
  }

  @Override
  public Optional<ComputationState> load(String computationId) {
    return Preconditions.checkNotNull(
        loadAll(ImmutableList.of(computationId)).getOrDefault(computationId, Optional.empty()));
  }

  @Override
  public Map<String, Optional<ComputationState>> loadAll(
      Iterable<? extends String> computationIds) {
    Optional<StreamingEnginePipelineConfig> streamingConfig =
        streamingEngineConfigLoader.getComputationConfig(Iterables.getOnlyElement(computationIds));
    if (!streamingConfig.isPresent()) {
      return ImmutableMap.of();
    }

    return streamingConfig
        .map(StreamingEnginePipelineConfig::computationConfigs)
        .map(
            configs ->
                createComputationStates(
                    configs, workUnitExecutor, perComputationStateCacheViewFactory))
        .orElse(ImmutableMap.of());
  }

  private ImmutableMap<String, Optional<ComputationState>> createComputationStates(
      List<StreamingComputationConfig> computationConfig,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
    return computationConfig.stream()
        .map(
            config ->
                createComputationState(
                    config, workUnitExecutor, perComputationStateCacheViewFactory))
        .collect(toImmutableMap(ComputationState::getComputationId, Optional::of));
  }

  private ComputationState createComputationState(
      StreamingComputationConfig computationConfig,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
    String computationId = computationConfig.getComputationId();
    MapTask mapTask =
        StreamingEnvironment.fixMapTaskMultiOutputInfoFnInstance()
            .apply(
                new MapTask()
                    .setSystemName(computationConfig.getSystemName())
                    .setStageName(computationConfig.getStageName())
                    .setInstructions(computationConfig.getInstructions()));

    return new ComputationState(
        computationId,
        mapTask,
        workUnitExecutor,
        Optional.ofNullable(computationConfig.getTransformUserNameToStateFamily())
            .orElseGet(ImmutableMap::of),
        perComputationStateCacheViewFactory.apply(computationId));
  }
}

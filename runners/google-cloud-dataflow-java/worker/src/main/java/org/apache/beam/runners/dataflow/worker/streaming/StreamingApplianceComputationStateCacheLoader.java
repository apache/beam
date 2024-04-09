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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap.toImmutableMap;

import com.google.api.services.dataflow.model.MapTask;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.config.StreamingConfigLoader;
import org.apache.beam.runners.dataflow.worker.streaming.harness.StreamingWorkerEnvironment;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.Windmill;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
public final class StreamingApplianceComputationStateCacheLoader
    extends CacheLoader<String, Optional<ComputationState>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(StreamingApplianceComputationStateCacheLoader.class);

  private final StreamingConfigLoader<Windmill.GetConfigResponse> streamingApplianceConfigLoader;
  private final BoundedQueueExecutor workUnitExecutor;
  private final ConcurrentHashMap<String, String> systemNameToComputationIdMap;
  private final Function<String, WindmillStateCache.ForComputation>
      perComputationStateCacheViewFactory;

  public StreamingApplianceComputationStateCacheLoader(
      StreamingConfigLoader<Windmill.GetConfigResponse> streamingApplianceConfigLoader,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
    this.streamingApplianceConfigLoader = streamingApplianceConfigLoader;
    this.workUnitExecutor = workUnitExecutor;
    this.systemNameToComputationIdMap = new ConcurrentHashMap<>();
    this.perComputationStateCacheViewFactory = perComputationStateCacheViewFactory;
  }

  private static Map<String, Map<String, String>> transformUserNameToStateFamilyByComputationId(
      Windmill.GetConfigResponse response) {
    // Outer keys are computation ids. Outer values are map from transform username to state
    // family.
    Map<String, Map<String, String>> transformUserNameToStateFamilyByComputationId =
        new HashMap<>();
    for (Windmill.GetConfigResponse.ComputationConfigMapEntry computationConfig :
        response.getComputationConfigMapList()) {
      Map<String, String> transformUserNameToStateFamily =
          transformUserNameToStateFamilyByComputationId.computeIfAbsent(
              computationConfig.getComputationId(), k -> new HashMap<>());
      for (Windmill.ComputationConfig.TransformUserNameToStateFamilyEntry entry :
          computationConfig.getComputationConfig().getTransformUserNameToStateFamilyList()) {
        transformUserNameToStateFamily.put(entry.getTransformUserName(), entry.getStateFamily());
      }
    }

    return transformUserNameToStateFamilyByComputationId;
  }

  /** Deserialize {@link MapTask} and populate MultiOutputInfos in MapTask. */
  private static Optional<MapTask> deserializeAndFixMapTask(String serializedMapTask) {
    try {
      return Optional.of(
          StreamingWorkerEnvironment.fixMapTaskMultiOutputInfoFnInstance()
              .apply(Transport.getJsonFactory().fromString(serializedMapTask, MapTask.class)));
    } catch (IOException e) {
      LOG.warn("Parsing MapTask failed: {}", serializedMapTask, e);
    }
    return Optional.empty();
  }

  @Override
  public Optional<ComputationState> load(String computationId) throws Exception {
    return Preconditions.checkNotNull(
        loadAll(ImmutableList.of(computationId)).getOrDefault(computationId, Optional.empty()));
  }

  @Override
  public Map<String, Optional<ComputationState>> loadAll(
      Iterable<? extends String> computationIds) {
    Optional<Windmill.GetConfigResponse> maybeResponse =
        streamingApplianceConfigLoader.getComputationConfig(
            Iterables.getOnlyElement(computationIds));
    if (!maybeResponse.isPresent()) {
      return ImmutableMap.of();
    }

    Windmill.GetConfigResponse response = maybeResponse.get();

    // The max work item commit bytes should be modified to be dynamic once it is available in
    // the request.
    for (Windmill.GetConfigResponse.SystemNameToComputationIdMapEntry entry :
        response.getSystemNameToComputationIdMapList()) {
      systemNameToComputationIdMap.put(entry.getSystemName(), entry.getComputationId());
    }

    Map<String, Map<String, String>> transformUserNameToStateFamilyByComputationId =
        transformUserNameToStateFamilyByComputationId(response);

    return response.getCloudWorksList().stream()
        .map(
            serializedMapTask ->
                createComputationState(
                    serializedMapTask, transformUserNameToStateFamilyByComputationId))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toImmutableMap(ComputationState::getComputationId, Optional::of));
  }

  private Optional<ComputationState> createComputationState(
      String serializedMapTask,
      Map<String, Map<String, String>> transformUserNameToStateFamilyByComputationId) {
    return deserializeAndFixMapTask(serializedMapTask)
        .map(
            mapTask -> {
              String computationId =
                  systemNameToComputationIdMap.getOrDefault(
                      mapTask.getSystemName(), mapTask.getSystemName());
              return new ComputationState(
                  computationId,
                  mapTask,
                  workUnitExecutor,
                  transformUserNameToStateFamilyByComputationId.getOrDefault(
                      computationId, ImmutableMap.of()),
                  perComputationStateCacheViewFactory.apply(computationId));
            });
  }
}

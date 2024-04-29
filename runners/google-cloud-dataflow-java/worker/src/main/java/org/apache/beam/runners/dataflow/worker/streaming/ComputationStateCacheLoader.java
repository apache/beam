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

import java.util.Optional;
import java.util.function.Function;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

final class ComputationStateCacheLoader extends CacheLoader<String, ComputationState> {
  private final ComputationConfig.Factory computationConfigFactory;
  private final BoundedQueueExecutor workUnitExecutor;
  private final Function<String, WindmillStateCache.ForComputation>
      perComputationStateCacheViewFactory;

  ComputationStateCacheLoader(
      ComputationConfig.Factory computationConfigFactory,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
    this.computationConfigFactory = computationConfigFactory;
    this.workUnitExecutor = workUnitExecutor;
    this.perComputationStateCacheViewFactory = perComputationStateCacheViewFactory;
  }

  @Override
  public ComputationState load(String computationId) {
    ComputationConfig computationConfig =
        computationConfigFactory
            .getComputationConfig(computationId)
            .orElseThrow(() -> new ComputationStateNotFoundException(computationId));
    return new ComputationState(
        computationId,
        computationConfig.mapTask(),
        workUnitExecutor,
        Optional.ofNullable(computationConfig.userTransformToStateFamilyName())
            .orElseGet(ImmutableMap::of),
        perComputationStateCacheViewFactory.apply(computationId));
  }

  static class ComputationStateNotFoundException extends IllegalStateException {
    public ComputationStateNotFoundException(String computationId) {
      super("No computation found for computationId=[ " + computationId + " ]");
    }
  }
}

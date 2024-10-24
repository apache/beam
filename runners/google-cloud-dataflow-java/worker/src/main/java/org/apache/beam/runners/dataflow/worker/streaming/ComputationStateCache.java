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

import static java.util.stream.Collectors.toConcurrentMap;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import com.google.api.services.dataflow.model.MapTask;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.apiary.FixMultiOutputInfosOnParDoInstructions;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.UncheckedExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cache of {@link String} computationId to {@link ComputationState}. */
@Internal
@ThreadSafe
public final class ComputationStateCache implements StatusDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ComputationStateCache.class);

  private final ConcurrentMap<String, String> pipelineUserNameToStateFamilyNameMap;
  private final LoadingCache<String, ComputationState> computationCache;

  /**
   * Fix up MapTask representation because MultiOutputInfos are missing from system generated
   * ParDoInstructions.
   */
  private final Function<MapTask, MapTask> fixMultiOutputInfosOnParDoInstructions;

  private ComputationStateCache(
      LoadingCache<String, ComputationState> computationCache,
      Function<MapTask, MapTask> fixMultiOutputInfosOnParDoInstructions,
      ConcurrentMap<String, String> pipelineUserNameToStateFamilyNameMap) {
    this.computationCache = computationCache;
    this.fixMultiOutputInfosOnParDoInstructions = fixMultiOutputInfosOnParDoInstructions;
    this.pipelineUserNameToStateFamilyNameMap = pipelineUserNameToStateFamilyNameMap;
  }

  public static ComputationStateCache create(
      ComputationConfig.Fetcher computationConfigFetcher,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory,
      IdGenerator idGenerator) {
    Function<MapTask, MapTask> fixMultiOutputInfosOnParDoInstructions =
        new FixMultiOutputInfosOnParDoInstructions(idGenerator);
    ConcurrentMap<String, String> pipelineUserNameToStateFamilyNameMap = new ConcurrentHashMap<>();
    return new ComputationStateCache(
        CacheBuilder.newBuilder()
            .build(
                new CacheLoader<String, ComputationState>() {
                  @Override
                  public ComputationState load(String computationId) {
                    // LoadingCache load(K key) will throw an exception if we return null here,
                    // throw ComputationStateNotFoundException to represent semantics better.
                    ComputationConfig computationConfig =
                        computationConfigFetcher
                            .fetchConfig(computationId)
                            .orElseThrow(
                                () -> new ComputationStateNotFoundException(computationId));
                    pipelineUserNameToStateFamilyNameMap.putAll(computationConfig.stateNameMap());
                    Map<String, String> transformUserNameToStateFamilyForComputation =
                        !computationConfig.userTransformToStateFamilyName().isEmpty()
                            ? computationConfig.userTransformToStateFamilyName()
                            : pipelineUserNameToStateFamilyNameMap;
                    return new ComputationState(
                        computationId,
                        fixMultiOutputInfosOnParDoInstructions.apply(computationConfig.mapTask()),
                        workUnitExecutor,
                        transformUserNameToStateFamilyForComputation,
                        perComputationStateCacheViewFactory.apply(computationId));
                  }
                }),
        fixMultiOutputInfosOnParDoInstructions,
        pipelineUserNameToStateFamilyNameMap);
  }

  @VisibleForTesting
  public static ComputationStateCache forTesting(
      ComputationConfig.Fetcher computationConfigFetcher,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory,
      IdGenerator idGenerator,
      ConcurrentMap<String, String> pipelineUserNameToStateFamilyNameMap) {
    ComputationStateCache cache =
        create(
            computationConfigFetcher,
            workUnitExecutor,
            perComputationStateCacheViewFactory,
            idGenerator);
    cache.pipelineUserNameToStateFamilyNameMap.putAll(pipelineUserNameToStateFamilyNameMap);
    return cache;
  }

  @VisibleForTesting
  ImmutableMap<String, String> getGlobalUsernameToStateFamilyNameMap() {
    return ImmutableMap.copyOf(pipelineUserNameToStateFamilyNameMap);
  }

  /**
   * Returns the {@link ComputationState} associated with the given computationId. May perform IO if
   * a value is not present, and it is possible that after IO is performed there is no value
   * correlated with that computationId.
   */
  public Optional<ComputationState> get(String computationId) {
    try {
      return Optional.ofNullable(computationCache.get(computationId));
    } catch (UncheckedExecutionException
        | ExecutionException
        | ComputationStateNotFoundException e) {
      if (e.getCause() instanceof ComputationStateNotFoundException
          || e instanceof ComputationStateNotFoundException) {
        LOG.warn(
            "Computation {} is currently unknown, "
                + "known computations are {}. "
                + "This is transient and safe to ignore.",
            computationId,
            ImmutableSet.copyOf(computationCache.asMap().keySet()));
      } else {
        LOG.warn("Error occurred fetching computation for computationId={}", computationId, e);
      }
    }

    return Optional.empty();
  }

  public Optional<ComputationState> getIfPresent(String computationId) {
    return Optional.ofNullable(computationCache.getIfPresent(computationId));
  }

  /** Returns a read-only view of all computations that have been fetched by the worker. */
  public ImmutableList<ComputationState> getAllPresentComputations() {
    return computationCache.asMap().values().stream().collect(toImmutableList());
  }

  /** Returns the current active {@link GetWorkBudget} across all known computations. */
  public GetWorkBudget totalCurrentActiveGetWorkBudget() {
    return computationCache.asMap().values().stream()
        .map(ComputationState::getActiveWorkBudget)
        .reduce(GetWorkBudget.noBudget(), GetWorkBudget::apply);
  }

  @VisibleForTesting
  public void loadCacheForTesting(
      List<MapTask> mapTasks, Function<MapTask, ComputationState> createComputationStateFn) {
    Map<String, ComputationState> computationStates =
        mapTasks.stream()
            .map(fixMultiOutputInfosOnParDoInstructions)
            .map(
                mapTask -> {
                  LOG.info("Adding config for {}: {}", mapTask.getSystemName(), mapTask);
                  return createComputationStateFn.apply(mapTask);
                })
            .collect(toConcurrentMap(ComputationState::getComputationId, Function.identity()));
    computationCache.putAll(computationStates);
  }

  /**
   * Close all {@link ComputationState}(s) present in the cache, then invalidate the entire cache.
   */
  @VisibleForTesting
  public void closeAndInvalidateAll() {
    computationCache.asMap().values().forEach(ComputationState::close);
    computationCache.invalidateAll();
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.println("<h1>Specs</h1>");
    for (ComputationState computationState : getAllPresentComputations()) {
      writer.println("<h3>" + computationState.getComputationId() + "</h3>");
      writer.print("<script>document.write(JSON.stringify(");
      writer.print(computationState.getMapTask().toString());
      writer.println(", null, \"&nbsp&nbsp\").replace(/\\n/g, \"<br>\"))</script>");
    }
  }

  @VisibleForTesting
  static class ComputationStateNotFoundException extends IllegalStateException {
    private ComputationStateNotFoundException(String computationId) {
      super("No computation found for computationId=[ " + computationId + " ]");
    }
  }
}

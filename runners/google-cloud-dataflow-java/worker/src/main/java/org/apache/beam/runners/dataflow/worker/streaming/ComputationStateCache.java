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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import java.io.PrintWriter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.streaming.config.ComputationConfig;
import org.apache.beam.runners.dataflow.worker.util.BoundedQueueExecutor;
import org.apache.beam.runners.dataflow.worker.windmill.state.WindmillStateCache;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cache of {@link String} computationId to {@link ComputationState}. */
@Internal
@ThreadSafe
public final class ComputationStateCache implements StatusDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ComputationStateCache.class);

  private final LoadingCache<String, ComputationState> computationCache;

  private ComputationStateCache(LoadingCache<String, ComputationState> computationCache) {
    this.computationCache = computationCache;
  }

  public static ComputationStateCache create(
      ComputationConfig.Fetcher computationConfigFetcher,
      BoundedQueueExecutor workUnitExecutor,
      Function<String, WindmillStateCache.ForComputation> perComputationStateCacheViewFactory) {
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
                            .getConfig(computationId)
                            .orElseThrow(
                                () -> new ComputationStateNotFoundException(computationId));
                    return new ComputationState(
                        computationId,
                        computationConfig.mapTask(),
                        workUnitExecutor,
                        computationConfig.userTransformToStateFamilyName(),
                        perComputationStateCacheViewFactory.apply(computationId));
                  }
                }));
  }

  /**
   * Returns the {@link ComputationState} associated with the given computationId. May perform IO if
   * a value is not present, and it is possible that after IO is performed there is no value
   * correlated with that computationId.
   */
  public Optional<ComputationState> get(String computationId) {
    try {
      return Optional.ofNullable(computationCache.get(computationId));
    } catch (ExecutionException | ComputationStateNotFoundException e) {
      LOG.warn("Error occurred fetching computation for computationId={}", computationId, e);
    }

    return Optional.empty();
  }

  public Optional<ComputationState> getIfPresent(String computationId) {
    return Optional.ofNullable(computationCache.getIfPresent(computationId));
  }

  /** Returns a read-only view of all computations. */
  public ImmutableList<ComputationState> getAllComputations() {
    return computationCache.asMap().values().stream().collect(toImmutableList());
  }

  public ImmutableSet<String> getAllComputationIds() {
    return ImmutableSet.copyOf(computationCache.asMap().keySet());
  }

  /** Returns the current active {@link GetWorkBudget} across all known computations. */
  public GetWorkBudget totalCurrentActiveGetWorkBudget() {
    return computationCache.asMap().values().stream()
        .map(ComputationState::getActiveWorkBudget)
        .reduce(GetWorkBudget.noBudget(), GetWorkBudget::apply);
  }

  @VisibleForTesting
  public void putAll(Map<String, ComputationState> computationStates) {
    computationCache.putAll(computationStates);
  }

  /**
   * Close all {@link ComputationState}(s) present in the cache, then invalidate the entire cache.
   */
  public void closeAndInvalidateAll() {
    computationCache.asMap().values().forEach(ComputationState::close);
    computationCache.invalidateAll();
  }

  @Override
  public void appendSummaryHtml(PrintWriter writer) {
    writer.println("<h1>Specs</h1>");
    for (ComputationState computationState : getAllComputations()) {
      writer.println("<h3>" + computationState.getComputationId() + "</h3>");
      writer.print("<script>document.write(JSON.stringify(");
      writer.print(computationState.getMapTask().toString());
      writer.println(", null, \"&nbsp&nbsp\").replace(/\\n/g, \"<br>\"))</script>");
    }
  }

  private static class ComputationStateNotFoundException extends IllegalStateException {
    private ComputationStateNotFoundException(String computationId) {
      super("No computation found for computationId=[ " + computationId + " ]");
    }
  }
}

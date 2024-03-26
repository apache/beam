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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList.toImmutableList;

import java.io.PrintWriter;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.runners.dataflow.worker.status.StatusDataProvider;
import org.apache.beam.runners.dataflow.worker.windmill.work.budget.GetWorkBudget;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cache of {@link String} computationId to {@link ComputationState} objects. */
@Internal
@ThreadSafe
public final class ComputationStateCache implements StatusDataProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ComputationStateCache.class);

  private final LoadingCache<String, Optional<ComputationState>> computationCache;

  private ComputationStateCache(LoadingCache<String, Optional<ComputationState>> computationCache) {
    this.computationCache = computationCache;
  }

  public static ComputationStateCache create(
      CacheLoader<String, Optional<ComputationState>> cacheLoader) {
    return new ComputationStateCache(CacheBuilder.newBuilder().build(cacheLoader));
  }

  /**
   * Returns the {@link ComputationState} associated with the given computationId. May perform IO if
   * a value is not present, and it is possible that after IO is performed there is no value
   * correlated with that computationId.
   */
  public Optional<ComputationState> getComputationState(String computationId) {
    try {
      Optional<ComputationState> computationState =
          computationCache.getAll(ImmutableList.of(computationId)).get(computationId);
      return computationState != null ? computationState : Optional.empty();
    } catch (ExecutionException e) {
      LOG.warn("Error occurred fetching computation for computationId={}", computationId, e);
    }
    return Optional.empty();
  }

  /** Returns a read-only view of all computations. */
  public ImmutableList<ComputationState> getAllComputations() {
    return computationCache.asMap().values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toImmutableList());
  }

  /** Returns the current active {@link GetWorkBudget} across all known computations. */
  public GetWorkBudget totalCurrentActiveGetWorkBudget() {
    return computationCache.asMap().values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .map(ComputationState::getActiveWorkBudget)
        .reduce(GetWorkBudget.noBudget(), GetWorkBudget::apply);
  }

  /**
   * Close all {@link ComputationState}(s) present in the cache, then invalidate the entire cache.
   */
  public void close() {
    computationCache.asMap().values().stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .forEach(
            computationState -> {
              try {
                computationState.close();
              } catch (Exception e) {
                LOG.warn(
                    "Exception={} while shutting down computationState={} ", e, computationState);
              }
            });
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
}

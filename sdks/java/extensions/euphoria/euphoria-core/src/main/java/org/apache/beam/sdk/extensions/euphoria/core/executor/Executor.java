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
package org.apache.beam.sdk.extensions.euphoria.core.executor;

import com.google.common.collect.Sets;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.VoidAccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.ReduceStateByKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** The client side, public, technology independent interface to an executor. */
@Audience(Audience.Type.CLIENT)
public interface Executor {

  /**
   * Operators that are considered to be basic and expected to be natively supported by each
   * executor implementation.
   *
   * @return set of basic operators
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return (Set) Sets.newHashSet(FlatMap.class, ReduceStateByKey.class, Union.class);
  }

  /**
   * Submits flow as a job. The returned object is an instance of {@link CompletableFuture} which
   * holds the asynchronous execution of the job. Client can wait for the result synchronously, or
   * different executions can be chained/composed with methods provided by the {@link
   * CompletableFuture}.
   *
   * <p>Example:
   *
   * <pre>{@code
   * CompletableFuture<Result> preparation = exec.submit(flow1);
   * CompletableFuture<Result> execution = preparation.thenCompose(r -> exec.submit(flow2));
   * CompletableFuture<Result> allJobs = execution.thenCompose(r -> exec.submit(flow3));
   *
   * allJobs.handle((result, err) -> {
   *   // clean after completion
   * });
   * }</pre>
   *
   * @param flow {@link Flow} to be submitted
   * @return future of the job's execution
   */
  CompletableFuture<Result> submit(Flow flow);

  /** Cancel all executions. */
  void shutdown();

  /**
   * Set accumulator provider that will be used to collect metrics and counters. When no provider is
   * set a default instance of {@link VoidAccumulatorProvider} will be used.
   *
   * @param factory Factory to create an instance of accumulator provider.
   */
  void setAccumulatorProvider(AccumulatorProvider.Factory factory);

  /** Execution (job) result. */
  class Result {}
}

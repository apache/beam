/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.Repartition;
import cz.seznam.euphoria.core.client.operator.Union;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * The client side, public, technology independent interface to an executor.
 */
public interface Executor {

  /**
   * Execution (job) result.
   */
  class Result {}

  /** 
   * Submits flow as a job. The returned object is an instance of {@link CompletableFuture}
   * which holds the asynchronous execution of the job. Client can wait for the result
   * synchronously, or different executions can be chained/composed with methods provided
   * by the {@link CompletableFuture}.<p>
   * 
   * Example:
   * 
   * <pre>{@code
   *   CompletableFuture<Result> preparation = exec.submit(flow1);
   *   CompletableFuture<Result> execution = preparation.thenCompose(r -> exec.submit(flow2));
   *   CompletableFuture<Result> allJobs = execution.thenCompose(r -> exec.submit(flow3));
   * 
   *   allJobs.handle((result, err) -> {
   *     // clean after completion
   *   });
   * }</pre>
   * 
   * @param flow {@link Flow} to be submitted
   * @return future of the job's execution
   */
  CompletableFuture<Result> submit(Flow flow);
  
  /**
   * Cancel all executions.
   */
  void shutdown();

  /**
   * Operators that are considered to be basic and expected to be natively
   * supported by each executor implementation.
   *
   * @return set of basic operators
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return (Set) Sets.newHashSet(
        FlatMap.class, Repartition.class, ReduceStateByKey.class, Union.class);
  }
}

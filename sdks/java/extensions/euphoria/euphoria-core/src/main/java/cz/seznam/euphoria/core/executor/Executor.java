/**
 * Copyright 2016 Seznam.cz, a.s.
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
 * Interface for any executor.
 */
public interface Executor {

  /** 
   * Submit flow as a job. Asynchronous operation.
   * @param flow {@link Flow} to be submitted
   * @return future of the job's execution
   */
  CompletableFuture<Result> submit(Flow flow);
  
  /**
   * Cancel all executions.
   */
  void shutdown();

  /**
   * Operators that are considered to be basic and each executor has to
   * implement them.
   * @return set of basic operators
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return (Set) Sets.newHashSet(
        FlatMap.class, Repartition.class, ReduceStateByKey.class, Union.class);
  }
  
  /**
   * Execution (job) result. Should contain aggregators, etc.
   */
  public static class Result {
  }
}

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
package org.apache.beam.runners.core;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * A factory for creating aggregators.
 */
public interface AggregatorFactory {
  /**
   * Create an aggregator with the given {@code name} and {@link CombineFn}.
   *
   *  <p>This method is called to create an aggregator for a {@link DoFn}. It receives the
   *  class of the {@link DoFn} being executed and the context of the step it is being
   *  executed in.
   */
  <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
      Class<?> fnClass, ExecutionContext.StepContext stepContext,
      String aggregatorName, CombineFn<InputT, AccumT, OutputT> combine);
}

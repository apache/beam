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

package org.apache.beam.fn.harness.fake;

import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.ExecutionContext.StepContext;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine.CombineFn;

/**
 * A fake implementation of an {@link AggregatorFactory} that is to be filled in at a later time.
 * The factory returns {@link Aggregator}s that do nothing when a value is added.
 */
public class FakeAggregatorFactory implements AggregatorFactory {
  @Override
  public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
      Class<?> fnClass,
      StepContext stepContext,
      String aggregatorName,
      CombineFn<InputT, AccumT, OutputT> combine) {
    return new Aggregator<InputT, OutputT>() {
      @Override
      public void addValue(InputT value) {}

      @Override
      public String getName() {
        return aggregatorName;
      }

      @Override
      public CombineFn<InputT, ?, OutputT> getCombineFn() {
        return combine;
      }
    };
  }
}

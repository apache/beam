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
package org.apache.beam.runners.flink.translation.functions;

import org.apache.beam.runners.core.AggregatorFactory;
import org.apache.beam.runners.core.ExecutionContext;
import org.apache.beam.runners.flink.translation.wrappers.SerializableFnAggregatorWrapper;
import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.flink.api.common.functions.RuntimeContext;

/**
 * A {@link AggregatorFactory} for the Flink Batch Runner.
 */
public class FlinkAggregatorFactory implements AggregatorFactory{

  private final RuntimeContext runtimeContext;

  public FlinkAggregatorFactory(RuntimeContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  @Override
  public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
      Class<?> fnClass, ExecutionContext.StepContext stepContext, String aggregatorName,
      Combine.CombineFn<InputT, AccumT, OutputT> combine) {
    @SuppressWarnings("unchecked")
    SerializableFnAggregatorWrapper<InputT, OutputT> result =
        (SerializableFnAggregatorWrapper<InputT, OutputT>)
            runtimeContext.getAccumulator(aggregatorName);

    if (result == null) {
      result = new SerializableFnAggregatorWrapper<>(combine);
      runtimeContext.addAccumulator(aggregatorName, result);
    }
    return result;
  }
}

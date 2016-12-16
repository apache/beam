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

package org.apache.beam.runners.gearpump.translators.utils;

import java.io.Serializable;

import org.apache.beam.sdk.transforms.Aggregator;
import org.apache.beam.sdk.transforms.Aggregator.AggregatorFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.util.ExecutionContext;

/**
 * no-op aggregator factory.
 */
public class NoOpAggregatorFactory implements AggregatorFactory, Serializable {

  @Override
  public <InputT, AccumT, OutputT> Aggregator<InputT, OutputT> createAggregatorForDoFn(
      Class<?> fnClass,
      ExecutionContext.StepContext stepContext,
      String aggregatorName,
      Combine.CombineFn<InputT, AccumT, OutputT> combine) {
    return new NoOpAggregator<>();
  }

  private static class NoOpAggregator<InputT, OutputT> implements Aggregator<InputT, OutputT>,
      java.io.Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public void addValue(InputT value) {
    }

    @Override
    public String getName() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Combine.CombineFn<InputT, ?, OutputT> getCombineFn() {
      // TODO Auto-generated method stub
      return null;
    }

  };
}

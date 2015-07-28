/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base implementation of a {@link CombiningValueState} reading from multiple sources and writing to
 * a single result.
 *
 * @param <InputT> the type of values added to the state
 * @param <AccumT> the type of accumulators that are actually stored
 * @param <OutputT> the type of value extracted from the state
 */
class MergedCombiningValue<InputT, AccumT, OutputT>
    implements CombiningValueStateInternal<InputT, AccumT, OutputT> {

  private final Collection<CombiningValueStateInternal<InputT, AccumT, OutputT>> sources;
  private final CombiningValueStateInternal<InputT, AccumT, OutputT> result;
  private final CombineFn<InputT, AccumT, OutputT> combineFn;

  public MergedCombiningValue(
      Collection<CombiningValueStateInternal<InputT, AccumT, OutputT>> sources,
      CombiningValueStateInternal<InputT, AccumT, OutputT> result,
      CombineFn<InputT, AccumT, OutputT> combineFn) {
    this.sources = sources;
    this.result = result;
    this.combineFn = combineFn;
  }

  @Override
  public void clear() {
    for (State source : sources) {
      source.clear();
    }
    result.clear();
  }

  @Override
  public StateContents<OutputT> get() {
    final StateContents<AccumT> accum = getAccum();
    return new StateContents<OutputT>() {
      @Override
      public OutputT read() {
        return combineFn.extractOutput(accum.read());
      }
    };
  }

  @Override
  public void add(InputT input) {
    result.add(input);
  }

  @Override
  public void addAccum(AccumT accum) {
    result.addAccum(accum);
  }

  @Override
  public StateContents<AccumT> getAccum() {
    final List<StateContents<AccumT>> futures = new ArrayList<>(sources.size());
    for (CombiningValueStateInternal<InputT, AccumT, OutputT> source : sources) {
      futures.add(source.getAccum());
    }

    return new StateContents<AccumT>() {
      @Override
      public AccumT read() {
        List<AccumT> accumulators = new ArrayList<>(futures.size());
        for (StateContents<AccumT> future : futures) {
          accumulators.add(future.read());
        }

        // Combine the accumualtors and compact the underyling state.
        AccumT combined = combineFn.mergeAccumulators(accumulators);
        clear();
        addAccum(combined);

        // This should be in memory. Since we may have mutated combined by
        // incorporating it into the result, we re-read it from memory.
        return result.getAccum().read();
      }
    };
  }

  @Override
  public StateContents<Boolean> isEmpty() {
    // Initiate the get's right away
    final List<StateContents<Boolean>> futures = new ArrayList<>(sources.size());
    for (CombiningValueStateInternal<InputT, AccumT, OutputT> source : sources) {
      futures.add(source.isEmpty());
    }

    // But defer the actual reads until later.
    return new StateContents<Boolean>() {
      @Override
      public Boolean read() {
        for (StateContents<Boolean> future : futures) {
          if (!future.read()) {
            return false;
          }
        }
        return true;
      }
    };
  }
}

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

package com.google.cloud.dataflow.sdk.runners;

import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn;

import java.util.Collection;
import java.util.Map;

/**
 * A collection of values associated with an {@link Aggregator}. Aggregators declared in a
 * {@link DoFn} are emitted on a per-{@code DoFn}-application basis.
 *
 * @param <T> the output type of the aggregator
 */
public abstract class AggregatorValues<T> {
  /**
   * Get the values of the {@link Aggregator} at all steps it was used.
   */
  public Collection<T> getValues() {
    return getValuesAtSteps().values();
  }

  /**
   * Get the values of the {@link Aggregator} by the user name at each step it was used.
   */
  public abstract Map<String, T> getValuesAtSteps();

  /**
   * Get the total value of this {@link Aggregator} by applying the specified {@link CombineFn}.
   */
  public T getTotalValue(CombineFn<T, ?, T> combineFn) {
    return combineFn.apply(getValues());
  }
}


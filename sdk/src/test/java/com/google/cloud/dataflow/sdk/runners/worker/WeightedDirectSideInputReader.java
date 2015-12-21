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

package com.google.cloud.dataflow.sdk.runners.worker;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.util.DirectSideInputReader;
import com.google.cloud.dataflow.sdk.util.PTuple;
import com.google.cloud.dataflow.sdk.util.SideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedSideInputReader;
import com.google.cloud.dataflow.sdk.util.WeightedValue;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link WeightedSideInputReader} with explicitly provided sizes for all values.
 */
class WeightedDirectSideInputReader extends WeightedSideInputReader.Defaults {

  private final SideInputReader subReader;
  private final Map<TupleTag<?>, Long> weights;

  /**
   * Returns a {@link WeightedDirectSideInputReader} containing the contents in the provided
   * {@code Map}. A {@link DirectSideInputReader} will be used for the actual retrieval logic; this
   * class merely does the size bookkeeping.
   */
  public static WeightedDirectSideInputReader withContents(
      Map<TupleTag<Object>, WeightedValue<Object>> sizedContents) {
    return new WeightedDirectSideInputReader(sizedContents);
  }

  private WeightedDirectSideInputReader(
      Map<TupleTag<Object>, WeightedValue<Object>> sizedContents) {
    weights = new HashMap<>();
    PTuple values = PTuple.empty();
    for (Map.Entry<TupleTag<Object>, WeightedValue<Object>> entry : sizedContents.entrySet()) {
      values = values.and(entry.getKey(), entry.getValue().getValue());
      weights.put(entry.getKey(), entry.getValue().getWeight());
    }
    subReader = DirectSideInputReader.of(values);
  }

  @Override
  public boolean isEmpty() {
    return subReader.isEmpty();
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return subReader.contains(view);
  }

  @Override
  public <T> WeightedValue<T> getWeighted(PCollectionView<T> view, BoundedWindow window) {
    return WeightedValue.of(
        subReader.get(view, window),
        weights.get(view.getTagInternal()));
  }
}

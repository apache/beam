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
import com.google.cloud.dataflow.sdk.util.Sized;
import com.google.cloud.dataflow.sdk.util.SizedSideInputReader;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link SizedSideInputReader} with explicitly provided sizes for all values.
 */
class SizedDirectSideInputReader extends SizedSideInputReader.Defaults {

  private final SideInputReader subReader;
  private final Map<TupleTag<?>, Long> sizes;

  /**
   * Returns a {@link SizedDirectSideInputReader} containing the contents in the provided
   * {@code Map}. A {@link DirectSideInputReader} will be used for the actual retrieval logic; this
   * class merely does the size bookkeeping.
   */
  public static SizedDirectSideInputReader withContents(
      Map<TupleTag<Object>, Sized<Object>> sizedContents) {
    return new SizedDirectSideInputReader(sizedContents);
  }

  private SizedDirectSideInputReader(Map<TupleTag<Object>, Sized<Object>> sizedContents) {
    sizes = new HashMap<>();
    PTuple values = PTuple.empty();
    for (Map.Entry<TupleTag<Object>, Sized<Object>> entry : sizedContents.entrySet()) {
      values = values.and(entry.getKey(), entry.getValue().getValue());
      sizes.put(entry.getKey(), entry.getValue().getSize());
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
  public <T> Sized<T> getSized(PCollectionView<T> view, BoundedWindow window) {
    return Sized.of(
        subReader.get(view, window),
        sizes.get(view.getTagInternal()));
  }
}

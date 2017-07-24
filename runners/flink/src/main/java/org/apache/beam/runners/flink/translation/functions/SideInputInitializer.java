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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;

/**
 * {@link BroadcastVariableInitializer} that initializes the broadcast input as a {@code Map}
 * from window to side input.
 */
public class SideInputInitializer<ElemT, ViewT, W extends BoundedWindow>
    implements BroadcastVariableInitializer<WindowedValue<ElemT>, Map<BoundedWindow, ViewT>> {

  PCollectionView<ViewT> view;

  public SideInputInitializer(PCollectionView<ViewT> view) {
    this.view = view;
  }

  @Override
  public Map<BoundedWindow, ViewT> initializeBroadcastVariable(
      Iterable<WindowedValue<ElemT>> inputValues) {

    // first partition into windows
    Map<BoundedWindow, List<WindowedValue<ElemT>>> partitionedElements = new HashMap<>();
    for (WindowedValue<ElemT> value: inputValues) {
      for (BoundedWindow window: value.getWindows()) {
        List<WindowedValue<ElemT>> windowedValues = partitionedElements.get(window);
        if (windowedValues == null) {
          windowedValues = new ArrayList<>();
          partitionedElements.put(window, windowedValues);
        }
        windowedValues.add(value);
      }
    }

    Map<BoundedWindow, ViewT> resultMap = new HashMap<>();

    for (Map.Entry<BoundedWindow, List<WindowedValue<ElemT>>> elements:
        partitionedElements.entrySet()) {

      @SuppressWarnings("unchecked")
      Iterable<WindowedValue<?>> elementsIterable =
          (List<WindowedValue<?>>) (List<?>) elements.getValue();

      resultMap.put(elements.getKey(), view.getViewFn().apply(elementsIterable));
    }

    return resultMap;
  }
}

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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;

/**
 * {@link BroadcastVariableInitializer} that initializes the broadcast input as a {@code Map} from
 * window to side input.
 */
public class SideInputInitializer<ViewT>
    implements BroadcastVariableInitializer<WindowedValue<?>, Map<BoundedWindow, ViewT>> {
  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(
          Materializations.ITERABLE_MATERIALIZATION_URN,
          Materializations.MULTIMAP_MATERIALIZATION_URN);

  PCollectionView<ViewT> view;

  public SideInputInitializer(PCollectionView<ViewT> view) {
    checkArgument(
        SUPPORTED_MATERIALIZATIONS.contains(view.getViewFn().getMaterialization().getUrn()),
        "This handler is only capable of dealing with %s materializations "
            + "but was asked to handle %s for PCollectionView with tag %s.",
        SUPPORTED_MATERIALIZATIONS,
        view.getViewFn().getMaterialization().getUrn(),
        view.getTagInternal().getId());
    this.view = view;
  }

  @Override
  public Map<BoundedWindow, ViewT> initializeBroadcastVariable(
      Iterable<WindowedValue<?>> inputValues) {

    // first partition into windows
    Map<BoundedWindow, List<WindowedValue<?>>> partitionedElements = new HashMap<>();
    for (WindowedValue<?> value : inputValues) {
      for (BoundedWindow window : value.getWindows()) {
        List<WindowedValue<?>> windowedValues =
            partitionedElements.computeIfAbsent(window, k -> new ArrayList<>());
        windowedValues.add(value);
      }
    }

    Map<BoundedWindow, ViewT> resultMap = new HashMap<>();

    for (Map.Entry<BoundedWindow, List<WindowedValue<?>>> elements :
        partitionedElements.entrySet()) {
      switch (view.getViewFn().getMaterialization().getUrn()) {
        case Materializations.ITERABLE_MATERIALIZATION_URN:
          {
            ViewFn<IterableView, ViewT> viewFn = (ViewFn<IterableView, ViewT>) view.getViewFn();
            resultMap.put(
                elements.getKey(),
                viewFn.apply(
                    () ->
                        elements.getValue().stream()
                            .map(WindowedValue::getValue)
                            .collect(Collectors.toList())));
          }
          break;
        case Materializations.MULTIMAP_MATERIALIZATION_URN:
          {
            ViewFn<MultimapView, ViewT> viewFn = (ViewFn<MultimapView, ViewT>) view.getViewFn();
            Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
            resultMap.put(
                elements.getKey(),
                viewFn.apply(
                    InMemoryMultimapSideInputView.fromIterable(
                        keyCoder,
                        (Iterable)
                            elements.getValue().stream()
                                .map(WindowedValue::getValue)
                                .collect(Collectors.toList()))));
          }
          break;
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown side input materialization format requested '%s'",
                  view.getViewFn().getMaterialization().getUrn()));
      }
    }

    return resultMap;
  }
}

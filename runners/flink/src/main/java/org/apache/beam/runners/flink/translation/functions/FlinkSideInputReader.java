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
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A {@link SideInputReader} for the Flink Batch Runner. */
public class FlinkSideInputReader implements SideInputReader {
  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(
          Materializations.ITERABLE_MATERIALIZATION_URN,
          Materializations.MULTIMAP_MATERIALIZATION_URN);

  private final Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;

  private RuntimeContext runtimeContext;

  public FlinkSideInputReader(
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> indexByView, RuntimeContext runtimeContext) {
    for (PCollectionView<?> view : indexByView.keySet()) {
      checkArgument(
          SUPPORTED_MATERIALIZATIONS.contains(view.getViewFn().getMaterialization().getUrn()),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          SUPPORTED_MATERIALIZATIONS,
          view.getViewFn().getMaterialization().getUrn(),
          view.getTagInternal().getId());
    }
    sideInputs = new HashMap<>();
    for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : indexByView.entrySet()) {
      sideInputs.put(entry.getKey().getTagInternal(), entry.getValue());
    }
    this.runtimeContext = runtimeContext;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    checkNotNull(view, "View passed to sideInput cannot be null");
    TupleTag<?> tag = view.getTagInternal();
    checkNotNull(sideInputs.get(tag), "Side input for " + view + " not available.");

    Map<BoundedWindow, T> sideInputs =
        runtimeContext.getBroadcastVariableWithInitializer(
            tag.getId(), new SideInputInitializer<>(view));
    T result = sideInputs.get(window);
    if (result == null) {
      switch (view.getViewFn().getMaterialization().getUrn()) {
        case Materializations.ITERABLE_MATERIALIZATION_URN:
          {
            ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) view.getViewFn();
            return viewFn.apply(() -> Collections.emptyList());
          }
        case Materializations.MULTIMAP_MATERIALIZATION_URN:
          {
            ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
            return viewFn.apply(InMemoryMultimapSideInputView.empty());
          }
        default:
          throw new IllegalStateException(
              String.format(
                  "Unknown side input materialization format requested '%s'",
                  view.getViewFn().getMaterialization().getUrn()));
      }
    }
    return result;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }
}

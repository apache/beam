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

package org.apache.beam.runners.spark.util;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;


/**
 * A {@link SideInputReader} for the SparkRunner.
 */
public class SparkSideInputReader implements SideInputReader {
  private final Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs;

  public SparkSideInputReader(
      Map<TupleTag<?>, KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    //--- validate sideInput.
    checkNotNull(view, "The PCollectionView passed to sideInput cannot be null ");
    KV<WindowingStrategy<?, ?>, SideInputBroadcast<?>> windowedBroadcastHelper =
        sideInputs.get(view.getTagInternal());
    checkNotNull(windowedBroadcastHelper, "SideInput for view " + view + " is not available.");

    //--- sideInput window
    final BoundedWindow sideInputWindow =
        view.getWindowMappingFn().getSideInputWindow(window);

    //--- match the appropriate sideInput window.
    // a tag will point to all matching sideInputs, that is all windows.
    // now that we've obtained the appropriate sideInputWindow, all that's left is to filter by it.
    Iterable<WindowedValue<KV<?, ?>>> availableSideInputs =
        (Iterable<WindowedValue<KV<?, ?>>>) windowedBroadcastHelper.getValue().getValue();
    Iterable<KV<?, ?>> sideInputForWindow =
        Iterables.transform(
            Iterables.filter(availableSideInputs, new Predicate<WindowedValue<?>>() {
              @Override
              public boolean apply(@Nullable WindowedValue<?> sideInputCandidate) {
                if (sideInputCandidate == null) {
                  return false;
                }
                return Iterables.contains(sideInputCandidate.getWindows(), sideInputWindow);
              }
            }),
            new Function<WindowedValue<KV<?, ?>>, KV<?, ?>>() {
              @Override
              public KV<?, ?> apply(WindowedValue<KV<?, ?>> windowedValue) {
                return windowedValue.getValue();
              }
            });

    ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
    Coder keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
    return viewFn.apply(
        InMemoryMultimapSideInputView.fromIterable(keyCoder, (Iterable) sideInputForWindow));
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.containsKey(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs != null && sideInputs.isEmpty();
  }
}

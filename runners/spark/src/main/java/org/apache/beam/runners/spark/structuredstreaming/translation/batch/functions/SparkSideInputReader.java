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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch.functions;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.InMemoryMultimapSideInputView;
import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.CoderHelpers;
import org.apache.beam.runners.spark.structuredstreaming.translation.helpers.SideInputBroadcast;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;

/** A {@link SideInputReader} for the Spark Batch Runner. */
public class SparkSideInputReader implements SideInputReader {
  /** A {@link Materializations.MultimapView} which always returns an empty iterable. */
  private static final Materializations.MultimapView EMPTY_MULTIMAP_VIEW =
      new MultimapView() {
        @Override
        public Iterable get() {
          return Collections.EMPTY_LIST;
        }

        @Override
        public Iterable get(@Nullable Object o) {
          return Collections.EMPTY_LIST;
        }
      };

  private final Map<TupleTag<?>, WindowingStrategy<?, ?>> sideInputs;
  private final SideInputBroadcast broadcastStateData;

  public SparkSideInputReader(
      Map<PCollectionView<?>, WindowingStrategy<?, ?>> indexByView,
      SideInputBroadcast broadcastStateData) {
    for (PCollectionView<?> view : indexByView.keySet()) {
      checkArgument(
          Materializations.MULTIMAP_MATERIALIZATION_URN.equals(
              view.getViewFn().getMaterialization().getUrn()),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          Materializations.MULTIMAP_MATERIALIZATION_URN,
          view.getViewFn().getMaterialization().getUrn(),
          view.getTagInternal().getId());
    }
    sideInputs = new HashMap<>();
    for (Map.Entry<PCollectionView<?>, WindowingStrategy<?, ?>> entry : indexByView.entrySet()) {
      sideInputs.put(entry.getKey().getTagInternal(), entry.getValue());
    }
    this.broadcastStateData = broadcastStateData;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    checkNotNull(view, "View passed to sideInput cannot be null");
    TupleTag<?> tag = view.getTagInternal();
    checkNotNull(sideInputs.get(tag), "Side input for " + view + " not available.");

    List<byte[]> sideInputsValues =
        (List<byte[]>) broadcastStateData.getBroadcastValue(tag.getId()).getValue();
    Coder<?> coder = broadcastStateData.getCoder(tag.getId());

    List<WindowedValue<?>> decodedValues = new ArrayList<>();
    for (byte[] value : sideInputsValues) {
      decodedValues.add((WindowedValue<?>) CoderHelpers.fromByteArray(value, coder));
    }

    Map<BoundedWindow, T> sideInputs = initializeBroadcastVariable(decodedValues, view);
    T result = sideInputs.get(window);
    if (result == null) {
      ViewFn<Materializations.MultimapView, T> viewFn =
          (ViewFn<Materializations.MultimapView, T>) view.getViewFn();
      result = viewFn.apply(EMPTY_MULTIMAP_VIEW);
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

  private <T> Map<BoundedWindow, T> initializeBroadcastVariable(
      Iterable<WindowedValue<?>> inputValues, PCollectionView<T> view) {

    // first partition into windows
    Map<BoundedWindow, List<WindowedValue<KV<?, ?>>>> partitionedElements = new HashMap<>();
    for (WindowedValue<KV<?, ?>> value :
        (Iterable<WindowedValue<KV<?, ?>>>) (Iterable) inputValues) {
      for (BoundedWindow window : value.getWindows()) {
        List<WindowedValue<KV<?, ?>>> windowedValues =
            partitionedElements.computeIfAbsent(window, k -> new ArrayList<>());
        windowedValues.add(value);
      }
    }

    Map<BoundedWindow, T> resultMap = new HashMap<>();

    for (Map.Entry<BoundedWindow, List<WindowedValue<KV<?, ?>>>> elements :
        partitionedElements.entrySet()) {

      ViewFn<Materializations.MultimapView, T> viewFn =
          (ViewFn<Materializations.MultimapView, T>) view.getViewFn();
      Coder keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
      resultMap.put(
          elements.getKey(),
          (T)
              viewFn.apply(
                  InMemoryMultimapSideInputView.fromIterable(
                      keyCoder,
                      (Iterable)
                          elements.getValue().stream()
                              .map(WindowedValue::getValue)
                              .collect(Collectors.toList()))));
    }

    return resultMap;
  }
}

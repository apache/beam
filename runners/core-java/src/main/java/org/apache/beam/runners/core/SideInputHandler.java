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
package org.apache.beam.runners.core;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Materializations;
import org.apache.beam.sdk.transforms.Materializations.IterableView;
import org.apache.beam.sdk.transforms.Materializations.MultimapView;
import org.apache.beam.sdk.transforms.ViewFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Generic side input handler that uses {@link StateInternals} to store all data. Both the actual
 * side-input data and data about the windows for which we have side inputs available are stored
 * using {@code StateInternals}.
 *
 * <p>The given {@code StateInternals} must not be scoped to an element key. The state must instead
 * be scoped to one key group for which the side input is being managed.
 *
 * <p>This is useful for runners that transmit the side-input elements in band, as opposed to how
 * Dataflow has an external service for managing side inputs.
 *
 * <p>Note: storing the available windows in an extra state is redundant for now but in the future
 * we might want to know which windows we have available so that we can garbage collect side input
 * data. For now, this will never clean up side-input data because we have no way of knowing when we
 * reach the GC horizon.
 */
public class SideInputHandler implements ReadyCheckingSideInputReader {
  private static final Set<String> SUPPORTED_MATERIALIZATIONS =
      ImmutableSet.of(
          Materializations.ITERABLE_MATERIALIZATION_URN,
          Materializations.MULTIMAP_MATERIALIZATION_URN);

  /** The list of side inputs that we're handling. */
  protected final Collection<PCollectionView<?>> sideInputs;

  /**
   * State internals that are scoped not to the key of a value but are global. The state can still
   * be kept locally but if side inputs are broadcast to all parallel operators then all will have
   * the same view of the state.
   */
  private final StateInternals stateInternals;

  /**
   * A state tag for each side input that we handle. The state is used to track for which windows we
   * have input available.
   */
  private final Map<
          PCollectionView<?>,
          StateTag<CombiningState<BoundedWindow, Set<BoundedWindow>, Set<BoundedWindow>>>>
      availableWindowsTags;

  /** State tag for the actual contents of each side input per window. */
  private final Map<PCollectionView<?>, StateTag<ValueState<Iterable<?>>>> sideInputContentsTags;

  /**
   * Creates a new {@code SideInputHandler} for the given side inputs that uses the given {@code
   * StateInternals} to store side input data and side-input meta data.
   */
  public SideInputHandler(
      Collection<PCollectionView<?>> sideInputs, StateInternals stateInternals) {
    this.sideInputs = sideInputs;
    this.stateInternals = stateInternals;
    this.availableWindowsTags = new HashMap<>();
    this.sideInputContentsTags = new HashMap<>();

    for (PCollectionView<?> sideInput : sideInputs) {
      checkArgument(
          SUPPORTED_MATERIALIZATIONS.contains(sideInput.getViewFn().getMaterialization().getUrn()),
          "This handler is only capable of dealing with %s materializations "
              + "but was asked to handle %s for PCollectionView with tag %s.",
          SUPPORTED_MATERIALIZATIONS,
          sideInput.getViewFn().getMaterialization().getUrn(),
          sideInput.getTagInternal().getId());

      @SuppressWarnings("unchecked")
      Coder<BoundedWindow> windowCoder =
          (Coder<BoundedWindow>)
              sideInput.getWindowingStrategyInternal().getWindowFn().windowCoder();

      StateTag<CombiningState<BoundedWindow, Set<BoundedWindow>, Set<BoundedWindow>>> availableTag =
          StateTags.combiningValue(
              "side-input-available-windows-" + sideInput.getTagInternal().getId(),
              SetCoder.of(windowCoder),
              new WindowSetCombineFn());

      availableWindowsTags.put(sideInput, availableTag);

      StateTag<ValueState<Iterable<?>>> stateTag =
          StateTags.value(
              "side-input-data-" + sideInput.getTagInternal().getId(),
              (Coder) IterableCoder.of(sideInput.getCoderInternal()));
      sideInputContentsTags.put(sideInput, stateTag);
    }
  }

  /**
   * Add the given value to the internal side-input store of the given side input. This might change
   * the result of {@link #isReady(PCollectionView, BoundedWindow)} for that side input.
   */
  public void addSideInputValue(PCollectionView<?> sideInput, WindowedValue<Iterable<?>> value) {
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) sideInput.getWindowingStrategyInternal().getWindowFn().windowCoder();

    StateTag<ValueState<Iterable<?>>> stateTag = sideInputContentsTags.get(sideInput);

    for (BoundedWindow window : value.getWindows()) {
      stateInternals
          .state(StateNamespaces.window(windowCoder, window), stateTag)
          .write(value.getValue());

      stateInternals
          .state(StateNamespaces.global(), availableWindowsTags.get(sideInput))
          .add(window);
    }
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    Iterable<?> elements = getIterable(view, window);
    switch (view.getViewFn().getMaterialization().getUrn()) {
      case Materializations.ITERABLE_MATERIALIZATION_URN:
        {
          ViewFn<IterableView, T> viewFn = (ViewFn<IterableView, T>) view.getViewFn();
          return viewFn.apply(() -> elements);
        }
      case Materializations.MULTIMAP_MATERIALIZATION_URN:
        {
          ViewFn<MultimapView, T> viewFn = (ViewFn<MultimapView, T>) view.getViewFn();
          Coder<?> keyCoder = ((KvCoder<?, ?>) view.getCoderInternal()).getKeyCoder();
          return viewFn.apply(
              InMemoryMultimapSideInputView.fromIterable(keyCoder, (Iterable) elements));
        }
      default:
        throw new IllegalStateException(
            String.format(
                "Unknown side input materialization format requested '%s'",
                view.getViewFn().getMaterialization().getUrn()));
    }
  }

  /**
   * Retrieve the value as written by {@link #addSideInputValue(PCollectionView, WindowedValue)},
   * without applying the SDK specific {@link ViewFn}.
   *
   * @param view
   * @param window
   * @param <T>
   * @return
   */
  public <T> Iterable<?> getIterable(PCollectionView<T> view, BoundedWindow window) {
    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) view.getWindowingStrategyInternal().getWindowFn().windowCoder();

    StateTag<ValueState<Iterable<?>>> stateTag = sideInputContentsTags.get(view);

    ValueState<Iterable<?>> state =
        stateInternals.state(StateNamespaces.window(windowCoder, window), stateTag);

    Iterable<?> elements = state.read();
    // return empty collection when no side input was received for ready window
    return (elements != null) ? elements : Collections.emptyList();
  }

  @Override
  public boolean isReady(PCollectionView<?> sideInput, BoundedWindow window) {
    Set<BoundedWindow> readyWindows =
        stateInternals.state(StateNamespaces.global(), availableWindowsTags.get(sideInput)).read();

    return readyWindows != null && readyWindows.contains(window);
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  /** For keeping track of the windows for which we have available side input. */
  private static class WindowSetCombineFn
      extends Combine.CombineFn<BoundedWindow, Set<BoundedWindow>, Set<BoundedWindow>> {

    @Override
    public Set<BoundedWindow> createAccumulator() {
      return new HashSet<>();
    }

    @Override
    public Set<BoundedWindow> addInput(Set<BoundedWindow> accumulator, BoundedWindow input) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public Set<BoundedWindow> mergeAccumulators(Iterable<Set<BoundedWindow>> accumulators) {
      Set<BoundedWindow> result = new HashSet<>();
      for (Set<BoundedWindow> acc : accumulators) {
        result.addAll(acc);
      }
      return result;
    }

    @Override
    public Set<BoundedWindow> extractOutput(Set<BoundedWindow> accumulator) {
      return accumulator;
    }
  }
}

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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SetCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.ReadyCheckingSideInputReader;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.state.CombiningState;
import org.apache.beam.sdk.util.state.ValueState;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * Generic side input handler that uses {@link StateInternals} to store all data. Both the actual
 * side-input data and data about the windows for which we have side inputs available are stored
 * using {@code StateInternals}.
 *
 * <p>The given {@code StateInternals} must not be scoped to an element key. The state
 * must instead be scoped to one key group for which the side input is being managed.
 *
 * <p>This is useful for runners that transmit the side-input elements in band, as opposed
 * to how Dataflow has an external service for managing side inputs.
 *
 * <p>Note: storing the available windows in an extra state is redundant for now but in the
 * future we might want to know which windows we have available so that we can garbage collect
 * side input data. For now, this will never clean up side-input data because we have no way
 * of knowing when we reach the GC horizon.
 */
public class SideInputHandler implements ReadyCheckingSideInputReader {

  /** The list of side inputs that we're handling. */
  protected final Collection<PCollectionView<?>> sideInputs;

  /**
   * State internals that are scoped not to the key of a value but are global. The state can still
   * be keep locally but if side inputs are broadcast to all parallel operators then all will
   * have the same view of the state.
   */
  private final StateInternals<Void> stateInternals;

  /**
   * A state tag for each side input that we handle. The state is used to track
   * for which windows we have input available.
   */
  private final Map<
      PCollectionView<?>,
      StateTag<
          Object,
          CombiningState<
                        BoundedWindow,
                        Set<BoundedWindow>,
                        Set<BoundedWindow>>>> availableWindowsTags;

  /**
   * State tag for the actual contents of each side input per window.
   */
  private final Map<
      PCollectionView<?>,
      StateTag<Object, ValueState<Iterable<WindowedValue<?>>>>> sideInputContentsTags;

  /**
   * Creates a new {@code SideInputHandler} for the given side inputs that uses
   * the given {@code StateInternals} to store side input data and side-input meta data.
   */
  public SideInputHandler(
      Collection<PCollectionView<?>> sideInputs,
      StateInternals<Void> stateInternals) {
    this.sideInputs = sideInputs;
    this.stateInternals = stateInternals;
    this.availableWindowsTags = new HashMap<>();
    this.sideInputContentsTags = new HashMap<>();

    for (PCollectionView<?> sideInput: sideInputs) {

      @SuppressWarnings("unchecked")
      Coder<BoundedWindow> windowCoder =
          (Coder<BoundedWindow>) sideInput
              .getWindowingStrategyInternal()
              .getWindowFn()
              .windowCoder();

      StateTag<
          Object,
          CombiningState<
                        BoundedWindow,
                        Set<BoundedWindow>,
                        Set<BoundedWindow>>> availableTag = StateTags.combiningValue(
          "side-input-available-windows-" + sideInput.getTagInternal().getId(),
          SetCoder.of(windowCoder),
          new WindowSetCombineFn());

      availableWindowsTags.put(sideInput, availableTag);

      Coder<Iterable<WindowedValue<?>>> coder = sideInput.getCoderInternal();
      StateTag<Object, ValueState<Iterable<WindowedValue<?>>>> stateTag =
          StateTags.value("side-input-data-" + sideInput.getTagInternal().getId(), coder);
      sideInputContentsTags.put(sideInput, stateTag);
    }
  }

  /**
   * Add the given value to the internal side-input store of the given side input. This
   * might change the result of {@link #isReady(PCollectionView, BoundedWindow)} for that side
   * input.
   */
  public void addSideInputValue(
      PCollectionView<?> sideInput,
      WindowedValue<Iterable<?>> value) {

    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) sideInput
            .getWindowingStrategyInternal()
            .getWindowFn()
            .windowCoder();

    // reify the WindowedValue
    List<WindowedValue<?>> inputWithReifiedWindows = new ArrayList<>();
    for (Object e: value.getValue()) {
      inputWithReifiedWindows.add(value.withValue(e));
    }

    StateTag<Object, ValueState<Iterable<WindowedValue<?>>>> stateTag =
        sideInputContentsTags.get(sideInput);

    for (BoundedWindow window: value.getWindows()) {
      stateInternals
          .state(StateNamespaces.window(windowCoder, window), stateTag)
          .write(inputWithReifiedWindows);

      stateInternals
          .state(StateNamespaces.global(), availableWindowsTags.get(sideInput))
          .add(window);
    }
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> sideInput, BoundedWindow window) {

    if (!isReady(sideInput, window)) {
      throw new IllegalStateException(
          "Side input " + sideInput + " is not ready for window " + window);
    }

    @SuppressWarnings("unchecked")
    Coder<BoundedWindow> windowCoder =
        (Coder<BoundedWindow>) sideInput
            .getWindowingStrategyInternal()
            .getWindowFn()
            .windowCoder();

    StateTag<Object, ValueState<Iterable<WindowedValue<?>>>> stateTag =
        sideInputContentsTags.get(sideInput);

    ValueState<Iterable<WindowedValue<?>>> state =
        stateInternals.state(StateNamespaces.window(windowCoder, window), stateTag);

    Iterable<WindowedValue<?>> elements = state.read();

    return sideInput.getViewFn().apply(elements);
  }

  @Override
  public boolean isReady(PCollectionView<?> sideInput, BoundedWindow window) {
    Set<BoundedWindow> readyWindows =
        stateInternals.state(StateNamespaces.global(), availableWindowsTags.get(sideInput)).read();

    boolean result = readyWindows != null && readyWindows.contains(window);
    return result;
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.contains(view);
  }

  @Override
  public boolean isEmpty() {
    return sideInputs.isEmpty();
  }

  /**
   * For keeping track of the windows for which we have available side input.
   */
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
      for (Set<BoundedWindow> acc: accumulators) {
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

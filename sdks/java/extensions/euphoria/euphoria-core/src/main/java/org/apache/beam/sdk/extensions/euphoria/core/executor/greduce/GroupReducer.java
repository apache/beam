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
package org.apache.beam.sdk.extensions.euphoria.core.executor.greduce;

import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Timer;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.MergingWindowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.WindowedElement;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunction;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Context;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ListStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.MergingStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.State;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateFactory;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StateMerger;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.Storage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.StorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorage;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.state.ValueStorageDescriptor;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.Trigger;
import org.apache.beam.sdk.extensions.euphoria.core.client.triggers.TriggerContext;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkState;

/**
 * An implementation of a RSBK group reducer of an ordered stream of already grouped (by a specific
 * key) and windowed elements where no late-comers are tolerated. Use this class only in batch mode!
 */
@Audience(Audience.Type.EXECUTOR)
public class GroupReducer<WidT extends Window, K, InT> {

  // ~ temporary store for trigger states
  final TriggerStorage triggerStorage;
  final TimerSupport<WidT> clock = new TimerSupport<>();
  final HashMap<WidT, State> states = new HashMap<>();
  private final StateFactory<InT, ?, State<InT, ?>> stateFactory;
  private final StateMerger<InT, ?, State<InT, ?>> stateCombiner;
  private final WindowedElementFactory<WidT, Object> elementFactory;
  private final StateContext stateContext;
  private final Collector<WindowedElement<?, Pair<K, ?>>> collector;
  private final Windowing windowing;
  private final Trigger trigger;
  private final AccumulatorProvider accumulators;
  K key;

  public GroupReducer(
      StateFactory<InT, ?, State<InT, ?>> stateFactory,
      StateMerger<InT, ?, State<InT, ?>> stateCombiner,
      StateContext stateContext,
      WindowedElementFactory<WidT, Object> elementFactory,
      Windowing windowing,
      Trigger trigger,
      Collector<WindowedElement<?, Pair<K, ?>>> collector,
      AccumulatorProvider accumulators) {
    this.stateFactory = Objects.requireNonNull(stateFactory);
    this.elementFactory = Objects.requireNonNull(elementFactory);
    this.stateCombiner = Objects.requireNonNull(stateCombiner);
    this.stateContext = Objects.requireNonNull(stateContext);
    this.collector = Objects.requireNonNull(collector);
    this.windowing = Objects.requireNonNull(windowing);
    this.trigger = Objects.requireNonNull(trigger);
    this.accumulators = Objects.requireNonNull(accumulators);

    this.triggerStorage = new TriggerStorage(stateContext.getStorageProvider());
  }

  @SuppressWarnings("unchecked")
  public void process(WindowedElement<WidT, Pair<K, InT>> elem) {
    // ~ make sure we have the key
    updateKey(elem);

    // ~ advance our clock
    clock.updateStamp(elem.getTimestamp(), this::onTimerCallback);

    // ~ get the target window
    WidT window = elem.getWindow();

    // ~ merge the new window into existing ones if necessary
    if (windowing instanceof MergingWindowing) {
      window = mergeWindows(window);
    }

    // ~ add the value to the target window state
    {
      State state = getStateForUpdate(window);
      state.add(elem.getElement().getSecond());
    }

    // ~ process trigger#onElement
    {
      ElementTriggerContext trgCtx = new ElementTriggerContext(window);
      Trigger.TriggerResult windowTr = trigger.onElement(elem.getTimestamp(), window, trgCtx);
      processTriggerResult(window, trgCtx, windowTr);
    }
  }

  @SuppressWarnings("unchecked")
  private State getStateForUpdate(WidT window) {
    return states.computeIfAbsent(
        window,
        w -> {
          return stateFactory.createState(stateContext, null);
        });
  }

  public void close() {
    // ~ fire all pending timers
    clock.updateStamp(Long.MAX_VALUE, this::onTimerCallback);
    // ~ flush any pending states - if any (might trigger non-time based windows)
    for (WidT window : new ArrayList<>(states.keySet())) {
      processTriggerResult(
          window, new ElementTriggerContext(window), Trigger.TriggerResult.FLUSH_AND_PURGE);
    }
  }

  @SuppressWarnings("unchecked")
  private void onTimerCallback(long stamp, WidT window) {
    ElementTriggerContext trgCtx = new ElementTriggerContext(window);
    processTriggerResult(window, trgCtx, trigger.onTimer(stamp, window, trgCtx));
  }

  // ~ merges the given window into the set of the currently actives ones
  // ~ returns the window the new element which derived `newWindow` shall be
  // placed into and a trigger indicating how to react on the window after adding
  // the element
  @SuppressWarnings("unchecked")
  private WidT mergeWindows(WidT newWindow) {
    if (states.containsKey(newWindow)) {
      // ~ the new window exists ... there's nothing to merge
      return newWindow;
    }

    Collection<Pair<Collection<WidT>, WidT>> merges =
        ((MergingWindowing) windowing).mergeWindows(getActivesWindowsPlus(newWindow));
    for (Pair<Collection<WidT>, WidT> merge : merges) {
      Collection<WidT> sources = merge.getFirst();
      WidT target = merge.getSecond();

      // ~ if the newWindow is being merged, replace it with the merge target such
      // that the new element (from which newWindow is originating from) ends up there
      if (sources.contains(newWindow)) {
        newWindow = target;
      }

      // ~ as of now on, we can assume `sources` do _not_ contain `target`;
      // this implies:
      //  a) the target window's state will not be merged into itself
      //  b) the target window's triggers will not be merged into itself
      //  c) the target window's trigger #onClear won't be called
      sources.remove(target);

      // ~ do not bother with the rest of thi for loop if we have
      // no source windows to merge
      if (sources.isEmpty()) {
        continue;
      }

      // ~ make sure to create the target state if necessary
      State targetState = getStateForUpdate(target);

      // ~ merge the (window) states
      {
        List<State> sourceStates = removeStatesForMerging(sources);
        stateCombiner.merge(targetState, (List) sourceStates);
      }

      // ~ merge trigger states
      trigger.onMerge(target, new MergingTriggerContext(sources, target));
      // ~ clear the trigger states of the merged windows
      for (WidT source : sources) {
        if (!source.equals(newWindow)) {
          trigger.onClear(source, new ElementTriggerContext(source));
        }
      }
    }

    return newWindow;
  }

  private List<WidT> getActivesWindowsPlus(WidT newWindow) {
    ArrayList<WidT> actives = new ArrayList<>(states.keySet().size() + 1);
    actives.addAll(states.keySet());
    actives.add(newWindow);
    return actives;
  }

  private List<State> removeStatesForMerging(Collection<WidT> windows) {
    ArrayList<State> xs = new ArrayList<>(windows.size());
    for (WidT window : windows) {
      State x = states.remove(window);
      if (x != null) {
        xs.add(x);
      }
    }
    return xs;
  }

  private void updateKey(WindowedElement<WidT, Pair<K, InT>> elem) {
    if (key == null) {
      key = elem.getElement().getFirst();
    } else {
      // ~ validate we really do process elements of a single key only
      checkState(key.equals(elem.getElement().getFirst()));
    }
  }

  @SuppressWarnings("unchecked")
  private void processTriggerResult(
      WidT window, ElementTriggerContext trgCtx, Trigger.TriggerResult tr) {
    if (tr.isFlush() && tr.isPurge()) {
      // ~ close the window
      State state = states.remove(window);
      if (state != null) {
        state.flush(new ElementCollector(collector, window));
        state.close();
      }
      // ~ clean up trigger states
      trigger.onClear(window, trgCtx);
    }
  }

  /** A thin facade around an executor dependent implementation. */
  @FunctionalInterface
  public interface Collector<T> {
    void collect(T elem);
  }

  /**
   * Creates a new instance of {@link WindowedElement}.
   *
   * @param <W> type of the window
   * @param <T> type of the data element
   */
  @FunctionalInterface
  public interface WindowedElementFactory<W extends Window, T> {
    WindowedElement<W, T> create(W window, long timestamp, T element);
  }

  class ElementCollector<T> implements Context, org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector<T> {

    final Collector<WindowedElement<WidT, Pair<K, T>>> out;
    final WidT window;

    ElementCollector(Collector<WindowedElement<WidT, Pair<K, T>>> out, WidT window) {
      this.out = out;
      this.window = window;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void collect(T elem) {
      out.collect(
          (WindowedElement)
              elementFactory.create(window, window.maxTimestamp() - 1, Pair.of(key, elem)));
    }

    @Override
    public Context asContext() {
      return this;
    }

    @Override
    public Window<?> getWindow() {
      return window;
    }

    @Override
    public Counter getCounter(String name) {
      return accumulators.getCounter(name);
    }

    @Override
    public Histogram getHistogram(String name) {
      return accumulators.getHistogram(name);
    }

    @Override
    public Timer getTimer(String name) {
      return accumulators.getTimer(name);
    }
  }

  class ElementTriggerContext implements TriggerContext {
    protected final Window window;

    ElementTriggerContext(Window window) {
      this.window = Objects.requireNonNull(window);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean registerTimer(long stamp, Window window) {
      clock.registerTimer(stamp, (WidT) window);
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void deleteTimer(long stamp, Window window) {
      clock.deleteTimer(stamp, (WidT) window);
    }

    @Override
    public long getCurrentTimestamp() {
      return clock.getStamp();
    }

    @Override
    public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
      return triggerStorage.getValueStorage(window, descriptor);
    }

    @Override
    public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return triggerStorage.getListStorage(window, descriptor);
    }
  }

  class MergingTriggerContext extends ElementTriggerContext
      implements TriggerContext.TriggerMergeContext {

    private Collection<? extends Window> sources;

    // ~ `trgt` is assumed _not_ to be contained in `srcs`
    MergingTriggerContext(Collection<? extends Window> srcs, Window trgt) {
      super(trgt);
      this.sources = srcs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void mergeStoredState(StorageDescriptor descriptor) {
      if (!(descriptor instanceof MergingStorageDescriptor)) {
        throw new IllegalStateException("Storage descriptor must support merging!");
      }

      MergingStorageDescriptor descr = (MergingStorageDescriptor) descriptor;
      BinaryFunction mergeFn = descr.getMerger();

      // create a new instance of storage
      Storage merged;
      if (descr instanceof ValueStorageDescriptor) {
        merged = getValueStorage((ValueStorageDescriptor) descr);
      } else if (descr instanceof ListStorageDescriptor) {
        merged = getListStorage((ListStorageDescriptor) descr);
      } else {
        throw new IllegalStateException("Cannot merge states for " + descr);
      }

      // merge all existing (non null) trigger states
      for (Window w : sources) {
        Storage s = triggerStorage.getStorage(w, descriptor);
        if (s != null) {
          mergeFn.apply(merged, s);
        }
      }
    }
  }
}

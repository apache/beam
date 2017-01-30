/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.core.executor.greduce;

import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.Storage;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptorBase;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

/**
 * An implementation of a RSBK group reducer of an ordered stream
 * of already grouped (by a specific key) and windowed elements where
 * no late-comers are tolerated.
 */
public class GroupReducer<WID extends Window, KEY, I> {

  // ~ a think facade around an executor dependent implementation
  @FunctionalInterface
  public interface Collector<T> {
    void collect(T elem);
  }

  private final StateFactory<?, State> stateFactory;
  private final CombinableReduceFunction<State> stateCombiner;
  private final StorageProvider stateStorageProvider;
  private final Collector<WindowedElement<?, Pair<KEY, ?>>> collector;
  private final Windowing windowing;
  private final Trigger trigger;

  // ~ temporary store for trigger states
  final TriggerStorage triggerStorage;
  final TimerSupport<WID> clock = new TimerSupport<>();
  final HashMap<WID, State> states = new HashMap<>();
  KEY key;

  public GroupReducer(StateFactory<?, State> stateFactory,
               CombinableReduceFunction<State> stateCombiner,
               StorageProvider stateStorageProvider,
               Windowing windowing,
               Trigger trigger,
               Collector<WindowedElement<?, Pair<KEY, ?>>> collector) {
    this.stateFactory = Objects.requireNonNull(stateFactory);
    this.stateCombiner = Objects.requireNonNull(stateCombiner);
    this.stateStorageProvider = Objects.requireNonNull(stateStorageProvider);
    this.windowing = Objects.requireNonNull(windowing);
    this.trigger = Objects.requireNonNull(trigger);
    this.collector = Objects.requireNonNull(collector);

    this.triggerStorage = new TriggerStorage(stateStorageProvider);
  }

  public void process(WindowedElement<WID, Pair<KEY, I>> elem) {
    // ~ make sure we have the key
    updateKey(elem);

    // ~ advance our clock
    clock.updateStamp(elem.getTimestamp(), this::onTimerCallback);

    // ~ get the target window
    WID window = elem.getWindow();
    Trigger.TriggerResult windowTr = Trigger.TriggerResult.NOOP;

    // ~ merge the new window into existing ones if necessary
    if (windowing instanceof MergingWindowing) {
      Pair<WID, Trigger.TriggerResult> r = mergeWindows(window);
      window = r.getFirst();
      windowTr = Trigger.TriggerResult.merge(windowTr, r.getSecond());
    }

    // ~ get the target window's state
    {
      State state = states.get(window);
      if (state == null) {
        state = stateFactory.apply(
            new ElementCollectContext(collector, window), stateStorageProvider);
        states.put(window, state);
      }
      // ~ add the value to the target window state
      state.add(elem.getElement().getSecond());
    }

    // ~ process trigger#onElement
    {
      ElementTriggerContext trgCtx = new ElementTriggerContext(window);
      windowTr = Trigger.TriggerResult.merge(
          windowTr, trigger.onElement(elem.getTimestamp(), window, trgCtx));
      processTriggerResult(window, trgCtx, windowTr);
    }
  }

  public void close() {
    // ~ fire all pending timers
    clock.updateStamp(Long.MAX_VALUE, this::onTimerCallback);
    // ~ flush any pending states - if any (might trigger non-time based windows)
    for (WID window : new ArrayList<>(states.keySet())) {
      processTriggerResult(
          window,
          new ElementTriggerContext(window),
          Trigger.TriggerResult.FLUSH_AND_PURGE);
    }
  }

  private void onTimerCallback(long stamp, WID window) {
    ElementTriggerContext trgCtx = new ElementTriggerContext(window);
    processTriggerResult(window, trgCtx, trigger.onTimer(stamp, window, trgCtx));
  }

  // ~ merges the given window into the set of the currently actives ones
  // ~ returns the window the new element which derived `newWindow` shall be
  // placed into and a trigger indicating how to react on the window after adding
  // the element
  private Pair<WID, Trigger.TriggerResult> mergeWindows(WID newWindow) {
    if (states.containsKey(newWindow)) {
      // ~ the new window exists ... there's nothing to merge
      return Pair.of(newWindow, Trigger.TriggerResult.NOOP);
    }

    Trigger.TriggerResult newWindowTr = Trigger.TriggerResult.NOOP;

    Collection<Pair<Collection<WID>, WID>> merges =
        ((MergingWindowing) windowing).mergeWindows(getActivesWindowsPlus(newWindow));
    for (Pair<Collection<WID>, WID> merge : merges) {
      Collection<WID> sources = merge.getFirst();
      WID target = merge.getSecond();

      // ~ if the newWindow is being merged, replace it with the merge target such
      // that the new element (from which newWindow is originating from) ends up there
      if (sources.contains(newWindow)) {
        newWindow = target;
      }

      // ~ merge the (window) states
      {
        // ~ first make sure that if any state emits data, it does so for target window
        List<State> sourceStates = removeStatesForMerging(sources);
        for (State state : sourceStates) {
          ((ElementCollectContext) state.getContext()).window = target;
        }
        // ~ now merge the state
        State newState = stateCombiner.apply(sourceStates);
        // ~ and store the new state for the target window
        states.put(target, newState);
      }

      // ~ merge trigger states
      Trigger.TriggerResult tr =
          trigger.onMerge(target, new MergingTriggerContext(sources, target));
      if (newWindow.equals(target)) {
        newWindowTr = Trigger.TriggerResult.merge(newWindowTr, tr);
      }
      // ~ clear the trigger states of the merged windows
      for (WID source : sources) {
        if (source.equals(newWindow) || source.equals(target)) {
          continue;
        }
        trigger.onClear(source, new ElementTriggerContext(source));
      }
    }

    return Pair.of(newWindow, newWindowTr);
  }

  private List<WID> getActivesWindowsPlus(WID newWindow) {
    ArrayList<WID> actives = new ArrayList<>(states.keySet().size() + 1);
    actives.addAll(states.keySet());
    actives.add(newWindow);
    return actives;
  }

  private List<State> removeStatesForMerging(Collection<WID> windows) {
    ArrayList<State> xs = new ArrayList<>(windows.size());
    for (WID window : windows) {
      State x = states.remove(window);
      if (x != null) {
        xs.add(x);
      }
    }
    return xs;
  }

  private void updateKey(WindowedElement<WID, Pair<KEY, I>> elem) {
    if (key == null) {
      key = elem.getElement().getFirst();
    } else {
      // ~ validate we really do process elements of a single key only
      Preconditions.checkState(key.equals(elem.getElement().getFirst()));
    }
  }

  private void processTriggerResult(
      WID window, ElementTriggerContext trgCtx, Trigger.TriggerResult tr) {
    if (tr.isFlush() && tr.isPurge()) {
      // ~ close the window
      State state = states.remove(window);
      if (state != null) {
        state.flush();
        state.close();
      }
      // ~ clean up trigger states
      trigger.onClear(window, trgCtx);
    }
  }

  class ElementCollectContext<T> implements Context<T> {
    final Collector<WindowedElement<WID, Pair<KEY, T>>> out;
    WID window;

    ElementCollectContext(Collector<WindowedElement<WID, Pair<KEY, T>>> out, WID window) {
      this.out = out;
      this.window = window;
    }

    @Override
    public void collect(T elem) {
      long stamp = (window instanceof TimedWindow)
          ? ((TimedWindow) window).maxTimestamp()
          : clock.getStamp();
      out.collect(new WindowedElement<>(window, stamp, Pair.of(key, elem)));
    }

    @Override
    public Object getWindow() {
      return window;
    }
  }

  class ElementTriggerContext implements TriggerContext {
    protected final Window window;

    ElementTriggerContext(Window window) {
      this.window = Objects.requireNonNull(window);
    }

    @Override
    public boolean registerTimer(long stamp, Window window) {
      clock.registerTimer(stamp, (WID) window);
      return true;
    }

    @Override
    public void deleteTimer(long stamp, Window window) {
      clock.deleteTimer(stamp, (WID) window);
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

  class MergingTriggerContext
      extends ElementTriggerContext
      implements TriggerContext.TriggerMergeContext {

    private Collection<? extends Window> sources;

    MergingTriggerContext(Collection<? extends Window> srcs, Window trgt) {
      super(trgt);
      this.sources = srcs;
    }

    @Override
    public void mergeStoredState(StorageDescriptorBase descriptor) {
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
        // ~ avoid accumulating the state of the target window twice
        if (w.equals(this.window)) {
          continue;
        }
        Storage s = triggerStorage.getStorage(w, descriptor);
        if (s != null) {
          mergeFn.apply(merged, s);
        }
      }
    }
  }

}

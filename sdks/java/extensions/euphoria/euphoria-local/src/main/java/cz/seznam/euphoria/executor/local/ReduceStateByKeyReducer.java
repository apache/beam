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
package cz.seznam.euphoria.executor.local;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.MergingStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StateFactory;
import cz.seznam.euphoria.core.client.operator.state.StateMerger;
import cz.seznam.euphoria.core.client.operator.state.Storage;
import cz.seznam.euphoria.core.client.operator.state.StorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.util.Settings;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReduceStateByKeyReducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);
  private final BlockingQueue<Datum> input;
  private final BlockingQueue<Datum> output;
  private final boolean isAttachedWindowing;
  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;
  private final WatermarkEmitStrategy watermarkStrategy;
  private final String name;
  private final Trigger<Window> trigger;
  // ~ both of these are guarded by "processing"
  private final ProcessingState processing;
  private final TriggerScheduler<Window, Object> scheduler;
  // ~ related to accumulators
  private final AccumulatorProvider.Factory accumulatorFactory;
  private final Settings settings;
  private long currentElementTime;
  @SuppressWarnings("unchecked")
  ReduceStateByKeyReducer(
      ReduceStateByKey operator,
      String name,
      BlockingQueue<Datum> input,
      BlockingQueue<Datum> output,
      UnaryFunction keyExtractor,
      UnaryFunction valueExtractor,
      TriggerScheduler scheduler,
      WatermarkEmitStrategy watermarkStrategy,
      StateContext stateContext,
      AccumulatorProvider.Factory accumulatorFactory,
      Settings settings,
      boolean allowEarlyEmitting) {

    this.name = requireNonNull(name);
    this.input = requireNonNull(input);
    this.output = requireNonNull(output);
    this.isAttachedWindowing = operator.getWindowing() == null;
    this.windowing = isAttachedWindowing ? AttachedWindowing.INSTANCE : operator.getWindowing();
    this.keyExtractor = requireNonNull(keyExtractor);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.watermarkStrategy = requireNonNull(watermarkStrategy);
    this.trigger = requireNonNull(windowing.getTrigger());
    this.scheduler = requireNonNull(scheduler);
    this.accumulatorFactory = requireNonNull(accumulatorFactory);
    this.settings = requireNonNull(settings);
    this.processing =
        new ProcessingState(
            output,
            scheduler,
            requireNonNull(operator.getStateFactory()),
            requireNonNull(operator.getStateMerger()),
            stateContext,
            allowEarlyEmitting);
  }

  <W extends Window, K> Triggerable<W, K> guardTriggerable(Triggerable<W, K> t) {
    return ((timestamp, kw) -> {
      synchronized (processing) {
        t.fire(timestamp, kw);
      }
    });
  }

  Triggerable<Window, Object> createTriggerHandler() {
    return ((timestamp, kw) -> {
      // ~ let trigger know about the time event and process window state
      // according to trigger result
      ElementTriggerContext ectx = new ElementTriggerContext(kw);
      Trigger.TriggerResult result = trigger.onTimer(timestamp, kw.window(), ectx);
      handleTriggerResult(result, ectx);
    });
  }

  void handleTriggerResult(Trigger.TriggerResult result, ElementTriggerContext ctx) {

    KeyedWindow scope = ctx.scope;

    // Flush window (emit the internal state to output)
    if (result.isFlush()) {
      processing.flushWindow(scope);
    }
    // Purge given window (discard internal state and cancel all triggers)
    if (result.isPurge()) {
      processing.purgeWindow(scope);
      trigger.onClear(scope.window(), ctx);
    }

    // emit a warning about late comers
    if (result == Trigger.TriggerResult.PURGE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Window {} discarded for key {} at current watermark {} with scheduler {}",
            new Object[] {
              ctx.getScope().window(), ctx.getScope().key(),
              getCurrentWatermark(), scheduler.getClass()
            });
      }
    }
  }

  @Override
  public void run() {
    LOG.debug("Started ReduceStateByKeyReducer for operator {}", name);
    watermarkStrategy.schedule(processing::emitWatermark);
    boolean run = true;
    while (run) {
      try {
        // ~ process incoming data
        Datum item = input.take();
        // ~ make sure to avoid race-conditions with triggers from another
        // thread (i.e. processing-time-trigger-scheduler)
        synchronized (processing) {
          if (item.isElement()) {
            currentElementTime = item.getTimestamp();
            processing.stats.update(currentElementTime);
            processInput(item);
          } else if (item.isEndOfStream()) {
            processEndOfStream((Datum.EndOfStream) item);
            run = false;
          } else if (item.isWatermark()) {
            processWatermark((Datum.Watermark) item);
          } else if (item.isWindowTrigger()) {
            processWindowTrigger((Datum.WindowTrigger) item);
          }
          // ~ send pending notifications about flushed windows
          notifyFlushedWindows();
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  private void notifyFlushedWindows() throws InterruptedException {
    // ~ send notifications to downstream operators about flushed windows
    long max = 0;
    for (Map.Entry<Window, Long> w : processing.takeFlushedWindows().entrySet()) {
      output.put(Datum.windowTrigger(w.getKey(), w.getValue()));
      long flushTime = w.getValue();
      if (flushTime > max) {
        max = flushTime;
      }
    }
    output.put(Datum.watermark(max));
  }

  private void processInput(WindowedElement element) {
    if (windowing instanceof MergingWindowing) {
      processInputMerging(element);
    } else {
      processInputNonMerging(element);
    }
  }

  @SuppressWarnings("unchecked")
  private void processInputNonMerging(WindowedElement element) {
    Object item = element.getElement();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Iterable<Window> windows = windowing.assignWindowsToElement(element);
    for (Window window : windows) {
      ElementTriggerContext pitctx = new ElementTriggerContext(new KeyedWindow(window, itemKey));

      State windowState = processing.getWindowStateForUpdate(pitctx.getScope());
      windowState.add(itemValue);
      Trigger.TriggerResult result = trigger.onElement(getCurrentElementTime(), window, pitctx);
      // ~ handle trigger result
      handleTriggerResult(result, pitctx);
    }
  }

  @SuppressWarnings("unchecked")
  private void processInputMerging(WindowedElement element) {
    assert windowing instanceof MergingWindowing;

    Object item = element.getElement();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Iterable<Window> windows = windowing.assignWindowsToElement(element);
    for (Window window : windows) {

      // ~ first try to merge the new window into the set of existing ones

      Set<Window> current = processing.getActivesForKey(itemKey);
      current.add(window);

      Collection<Pair<Collection<Window>, Window>> cmds =
          ((MergingWindowing) windowing).mergeWindows(current);

      for (Pair<Collection<Window>, Window> cmd : cmds) {
        Collection<Window> srcs = cmd.getFirst();
        Window trgt = cmd.getSecond();

        // ~ if the new window was merged, continue processing the merge
        // target as the window the new input element should be placed into
        if (srcs.contains(window)) {
          window = trgt;
        }

        // ~ merge window (item) states
        Set<KeyedWindow<Window, Object>> merged =
            processing.mergeWindowStates(srcs, new KeyedWindow<>(trgt, itemKey));

        // ~ merge window trigger states
        trigger.onMerge(
            trgt, new MergingElementTriggerContext(new KeyedWindow<>(trgt, itemKey), merged));
        // ~ clear window trigger states for the merged windows
        for (KeyedWindow w : merged) {
          trigger.onClear(w.window(), new ElementTriggerContext(w));
        }
      }

      // ~ only now, add the element to the new window

      ElementTriggerContext pitctx = new ElementTriggerContext(new KeyedWindow(window, itemKey));
      State windowState = processing.getWindowStateForUpdate(pitctx.getScope());
      windowState.add(itemValue);
      Trigger.TriggerResult tr = trigger.onElement(getCurrentElementTime(), window, pitctx);
      // ~ handle trigger result
      handleTriggerResult(tr, pitctx);
    }
  }

  private void processWatermark(Datum.Watermark watermark) {
    // update current stamp
    long stamp = watermark.getTimestamp();
    processing.updateStamp(stamp);
  }

  private void processWindowTrigger(Datum.WindowTrigger trigger) {
    if (isAttachedWindowing) {
      // reregister trigger of given window
      // TODO: move this to windowing itself so that attached windowing
      // can be implemented 'natively' as instance of generic windowing
      processing.onUpstreamWindowTrigger(trigger.getWindow(), trigger.getTimestamp());
    }
  }

  private void processEndOfStream(Datum.EndOfStream eos) throws InterruptedException {
    // ~ flush all registered triggers
    scheduler.updateStamp(Long.MAX_VALUE);
    // ~ stop triggers - there actually should be none left
    scheduler.close();
    // ~ close all states
    processing.flushAndCloseAllWindows();
    processing.closeOutput();
    output.put(eos);
  }

  // retrieve current watermark stamp
  private long getCurrentWatermark() {
    return scheduler.getCurrentTimestamp();
  }

  private long getCurrentElementTime() {
    return currentElementTime;
  }

  static final class KeyedElementCollector extends WindowedElementCollector<Object> {
    private final Object key;

    KeyedElementCollector(
        Collector<Datum> wrap,
        Window window,
        Object key,
        Supplier<Long> stampSupplier,
        AccumulatorProvider.Factory accumulatorFactory,
        Settings settings) {
      super(wrap, stampSupplier, accumulatorFactory, settings);
      this.key = key;
      this.window = window;
    }

    @Override
    public void collect(Object elem) {
      super.collect(Pair.of(key, elem));
    }
  } // ~ end of KeyedElementCollector

  static final class WindowRegistry {

    final Map<Window, Map<Object, State>> windows = new HashMap<>();
    final Map<Object, Set<Window>> keyMap = new HashMap<>();

    State removeWindowState(KeyedWindow<?, ?> kw) {
      Map<Object, State> keys = windows.get(kw.window());
      if (keys != null) {
        State state = keys.remove(kw.key());
        // ~ garbage collect on windows level
        if (keys.isEmpty()) {
          windows.remove(kw.window());
        }
        Set<Window> actives = keyMap.get(kw.key());
        if (actives != null) {
          actives.remove(kw.window());
          if (actives.isEmpty()) {
            keyMap.remove(kw.key());
          }
        }
        return state;
      }
      return null;
    }

    void setWindowState(KeyedWindow<?, ?> kw, State state) {
      Map<Object, State> keys = windows.get(kw.window());
      if (keys == null) {
        windows.put(kw.window(), keys = new HashMap<>());
      }
      keys.put(kw.key(), state);
      Set<Window> actives = keyMap.get(kw.key());
      if (actives == null) {
        keyMap.put(kw.key(), actives = new HashSet<>());
      }
      actives.add(kw.window());
    }

    State getWindowState(KeyedWindow<?, ?> kw) {
      return getWindowState(kw.window(), kw.key());
    }

    State getWindowState(Window window, Object key) {
      Map<Object, State> keys = windows.get(window);
      if (keys != null) {
        return keys.get(key);
      }
      return null;
    }

    Map<Object, State> getWindowStates(Window window) {
      return windows.get(window);
    }

    Set<Window> getActivesForKey(Object itemKey) {
      return keyMap.get(itemKey);
    }
  } // ~ end of WindowRegistry

  static final class ScopedStorage {
    final HashMap<StorageKey, Object> store = new HashMap<>();
    final StorageProvider storageProvider;
    ScopedStorage(StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
    }

    Storage removeStorage(KeyedWindow scope, StorageDescriptor descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      return (Storage) store.remove(skey);
    }

    Storage getStorage(KeyedWindow scope, StorageDescriptor descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      return (Storage) store.get(skey);
    }

    @SuppressWarnings("unchecked")
    <T> ValueStorage<T> getValueStorage(KeyedWindow scope, ValueStorageDescriptor<T> descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      Storage s = (Storage) store.get(skey);
      if (s == null) {
        store.put(skey, s = storageProvider.getValueStorage(descriptor));
      }
      return (ValueStorage<T>) s;
    }

    @SuppressWarnings("unchecked")
    <T> ListStorage<T> getListStorage(KeyedWindow scope, ListStorageDescriptor<T> descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      Storage s = (Storage) store.get(skey);
      if (s == null) {
        store.put(skey, s = storageProvider.getListStorage(descriptor));
      }
      return (ListStorage<T>) s;
    }

    private StorageKey storageKey(KeyedWindow kw, StorageDescriptor desc) {
      return new StorageKey(kw.key(), kw.window(), desc.getName());
    }

    static final class StorageKey {
      private final Object itemKey;
      private final Window itemWindow;
      private final String storeId;

      public StorageKey(Object itemKey, Window itemWindow, String storeId) {
        this.itemKey = itemKey;
        this.itemWindow = itemWindow;
        this.storeId = storeId;
      }

      @Override
      public boolean equals(Object o) {
        if (o instanceof StorageKey) {
          StorageKey that = (StorageKey) o;
          return Objects.equals(this.itemKey, that.itemKey)
              && Objects.equals(this.itemWindow, that.itemWindow)
              && Objects.equals(this.storeId, that.storeId);
        }
        return false;
      }

      @Override
      public int hashCode() {
        int result = itemKey != null ? itemKey.hashCode() : 0;
        result = 31 * result + (itemWindow != null ? itemWindow.hashCode() : 0);
        result = 31 * result + (storeId != null ? storeId.hashCode() : 0);
        return result;
      }
    }
  } // ~ end of ScopedStorage

  final class ClearingValueStorage<T> implements ValueStorage<T> {
    private final ValueStorage<T> wrap;
    private final KeyedWindow<?, ?> scope;
    private final StorageDescriptor descriptor;

    ClearingValueStorage(
        ValueStorage<T> wrap, KeyedWindow<?, ?> scope, StorageDescriptor descriptor) {
      this.wrap = wrap;
      this.scope = scope;
      this.descriptor = descriptor;
    }

    @Override
    public void clear() {
      wrap.clear();
      processing.triggerStorage.removeStorage(scope, descriptor);
    }

    @Override
    public void set(T value) {
      wrap.set(value);
    }

    @Override
    public T get() {
      return wrap.get();
    }
  } // ~ end of ClearingValueStorage

  final class ClearingListStorage<T> implements ListStorage<T> {
    private final ListStorage<T> wrap;
    private final KeyedWindow scope;
    private final StorageDescriptor descriptor;

    public ClearingListStorage(
        ListStorage<T> wrap, KeyedWindow scope, StorageDescriptor descriptor) {
      this.wrap = wrap;
      this.scope = scope;
      this.descriptor = descriptor;
    }

    @Override
    public void clear() {
      wrap.clear();
      processing.triggerStorage.removeStorage(scope, descriptor);
    }

    @Override
    public void add(T element) {
      wrap.add(element);
    }

    @Override
    public Iterable<T> get() {
      return wrap.get();
    }
  } // ~ end of ClearingListStorage

  class ElementTriggerContext implements TriggerContext {
    private final KeyedWindow<Window, Object> scope;

    ElementTriggerContext(KeyedWindow<Window, Object> scope) {
      this.scope = scope;
    }

    KeyedWindow getScope() {
      return scope;
    }

    @Override
    public boolean registerTimer(long stamp, Window window) {
      checkState(this.scope.window().equals(window));
      return scheduler.scheduleAt(stamp, this.scope, guardTriggerable(createTriggerHandler()));
    }

    @Override
    public void deleteTimer(long stamp, Window window) {
      checkState(this.scope.window().equals(window));
      scheduler.cancel(stamp, this.scope);
    }

    @Override
    public long getCurrentTimestamp() {
      return scheduler.getCurrentTimestamp();
    }

    @Override
    public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
      return new ClearingValueStorage<>(
          processing.triggerStorage.getValueStorage(this.scope, descriptor),
          this.scope,
          descriptor);
    }

    @Override
    public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return new ClearingListStorage<>(
          processing.triggerStorage.getListStorage(this.scope, descriptor), this.scope, descriptor);
    }
  } // ~ end of ElementTriggerContext

  class MergingElementTriggerContext extends ElementTriggerContext
      implements TriggerContext.TriggerMergeContext {
    final Collection<KeyedWindow<Window, Object>> mergeSources;

    MergingElementTriggerContext(
        KeyedWindow<Window, Object> target, Collection<KeyedWindow<Window, Object>> sources) {
      super(target);
      this.mergeSources = sources;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void mergeStoredState(StorageDescriptor storageDescriptor) {
      if (!(storageDescriptor instanceof MergingStorageDescriptor)) {
        throw new IllegalStateException("Storage descriptor must support merging!");
      }
      MergingStorageDescriptor descr = (MergingStorageDescriptor) storageDescriptor;
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
      for (KeyedWindow<?, ?> w : this.mergeSources) {
        Storage s = processing.triggerStorage.getStorage(w, storageDescriptor);
        if (s != null) {
          mergeFn.apply(merged, s);
        }
      }
    }
  } // ~ end of MergingElementTriggerContext

  // statistics related to the running operator
  final class ProcessingStats {

    final ProcessingState processing;
    long watermarkPassed = -1;
    long maxElementStamp = -1;
    long lastLogTime = -1;

    ProcessingStats(ProcessingState processing) {
      this.processing = processing;
    }

    void update(long elementStamp) {
      watermarkPassed = processing.triggering.getCurrentTimestamp();
      if (maxElementStamp < elementStamp) {
        maxElementStamp = elementStamp;
      }
      long now = System.currentTimeMillis();
      if (lastLogTime + 5000 < now) {
        log();
        lastLogTime = now;
      }
    }

    private void log() {
      LOG.info(
          "Reducer {} processing stats: at watermark {}, maxElementStamp {}",
          new Object[] {ReduceStateByKeyReducer.this.name, watermarkPassed, maxElementStamp});
    }
  } // ~ end of ProcessingStats

  final class ProcessingState {

    final boolean allowEarlyEmitting;

    final ScopedStorage triggerStorage;
    final StateContext stateContext;
    final WindowRegistry wRegistry = new WindowRegistry();

    final Collector<Datum> stateOutput;
    final BlockingQueue<Datum> rawOutput;
    final TriggerScheduler<Window, Object> triggering;
    final StateFactory stateFactory;
    final StateMerger stateMerger;

    final ProcessingStats stats = new ProcessingStats(this);

    // flushed windows with the time of the flush
    private Map<Window, Long> flushedWindows = new HashMap<>();

    private ProcessingState(
        BlockingQueue<Datum> output,
        TriggerScheduler<Window, Object> triggering,
        StateFactory stateFactory,
        StateMerger stateMerger,
        StateContext stateContext,
        boolean allowEarlyEmitting) {

      this.stateContext = stateContext;
      this.triggerStorage = new ScopedStorage(stateContext.getStorageProvider());
      this.stateOutput = LocalExecutor.QueueCollector.wrap(requireNonNull(output));
      this.rawOutput = output;
      this.triggering = requireNonNull(triggering);
      this.stateFactory = requireNonNull(stateFactory);
      this.stateMerger = requireNonNull(stateMerger);
      this.allowEarlyEmitting = allowEarlyEmitting;
    }

    Map<Window, Long> takeFlushedWindows() {
      if (flushedWindows.isEmpty()) {
        return Collections.emptyMap();
      }
      Map<Window, Long> flushed = flushedWindows;
      flushedWindows = new HashMap<>();
      return flushed;
    }

    // ~ signal eos further down the output channel
    void closeOutput() {
      try {
        this.rawOutput.put(Datum.endOfStream());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    /** Flushes (emits result) the specified window. */
    @SuppressWarnings("unchecked")
    void flushWindow(KeyedWindow<?, ?> kw) {
      State state = wRegistry.getWindowState(kw);
      if (state == null) {
        return;
      }
      state.flush(newCollector(kw));
      // ~ remember we flushed the window such that we can emit one
      // notification to downstream operators for all keys in this window
      flushedWindows.put(kw.window(), getCurrentWatermark());
    }

    /** Purges the specified window. */
    State purgeWindow(KeyedWindow<?, ?> kw) {
      State state = wRegistry.removeWindowState(kw);
      if (state == null) {
        return null;
      }
      state.close();
      return state;
    }

    /** Flushes and closes all window storages and clear the window registry. */
    @SuppressWarnings("unchecked")
    void flushAndCloseAllWindows() {
      for (Map.Entry<Window, Map<Object, State>> windowState : wRegistry.windows.entrySet()) {
        for (Map.Entry<Object, State> itemState : windowState.getValue().entrySet()) {
          State state = itemState.getValue();
          state.flush(newCollector(new KeyedWindow(windowState.getKey(), itemState.getKey())));
          state.close();
        }
      }
      wRegistry.windows.clear();
    }

    @SuppressWarnings("unchecked")
    State getWindowStateForUpdate(KeyedWindow<?, ?> kw) {
      State state = wRegistry.getWindowState(kw);
      if (state == null) {
        // ~ if no such window yet ... set it up
        state =
            stateFactory.createState(stateContext, allowEarlyEmitting ? newCollector(kw) : null);
        wRegistry.setWindowState(kw, state);
      }
      return state;
    }

    private KeyedElementCollector newCollector(KeyedWindow kw) {
      return new KeyedElementCollector(
          stateOutput,
          kw.window(),
          kw.key(),
          processing.triggering::getCurrentTimestamp,
          accumulatorFactory,
          settings);
    }

    // ~ returns a freely modifable collection of windows actively
    // for the given item key
    Set<Window> getActivesForKey(Object itemKey) {
      Set<Window> actives = wRegistry.getActivesForKey(itemKey);
      if (actives == null || actives.isEmpty()) {
        return new HashSet<>();
      } else {
        return new HashSet<>(actives);
      }
    }

    // ~ merges window states for sources and places it on 'target'
    // ~ returns a list of windows which were merged and actually removed
    @SuppressWarnings("unchecked")
    Set<KeyedWindow<Window, Object>> mergeWindowStates(
        Collection<Window> sources, KeyedWindow<Window, Object> target) {
      // ~ first find the states to be merged into `target`
      List<Pair<Window, State>> merge = new ArrayList<>(sources.size());
      for (Window source : sources) {
        if (!source.equals(target.window())) {
          State state = wRegistry.removeWindowState(new KeyedWindow<>(source, target.key()));
          if (state != null) {
            merge.add(Pair.of(source, state));
          }
        }
      }
      // ~ prepare for the state merge
      List<State<?, ?>> statesToMerge = new ArrayList<>(merge.size());
      // ~ if any of the states emits any data during the merge, we'll make
      // sure it happens in the scope of the merge target window
      for (Pair<Window, State> m : merge) {
        statesToMerge.add(m.getSecond());
      }
      // ~ now merge the state and re-assign it to the merge-window
      if (!statesToMerge.isEmpty()) {
        State targetState = getWindowStateForUpdate(target);
        stateMerger.merge(targetState, statesToMerge);
      }
      // ~ finally return a list of windows which were actually merged and removed
      return merge
          .stream()
          .map(Pair::getFirst)
          .map(w -> new KeyedWindow<>(w, target.key()))
          .collect(toSet());
    }

    /** Update current timestamp by given watermark. */
    void updateStamp(long stamp) {
      triggering.updateStamp(stamp);
    }

    /** Update trigger of given window. */
    void onUpstreamWindowTrigger(Window window, long stamp) {
      LOG.debug("Updating trigger of window {} to {}", window, stamp);

      Map<Object, State> ws = wRegistry.getWindowStates(window);
      if (ws == null || ws.isEmpty()) {
        return;
      }

      for (Map.Entry<Object, State> e : ws.entrySet()) {
        @SuppressWarnings("unchecked")
        KeyedWindow<Window, Object> kw = new KeyedWindow<>(window, e.getKey());

        Triggerable<Window, Object> t =
            guardTriggerable(
                (tstamp, tkw) -> {
                  flushWindow(tkw);
                  purgeWindow(tkw);
                  trigger.onClear(kw.window(), new ElementTriggerContext(tkw));
                });
        if (!triggering.scheduleAt(stamp, kw, t)) {
          LOG.debug("Manually firing already passed flush event for window {}", kw);
          t.fire(stamp, kw);
        }
      }
    }

    void emitWatermark() {
      final long stamp = getCurrentWatermark();
      try {
        rawOutput.put(Datum.watermark(stamp));
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }
  } // ~ end of ProcessingState
}

package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Time.ProcessingTime;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
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
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.QueueCollector;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
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

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class ReduceStateByKeyReducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);

  static final class KeyedElementCollector extends WindowedElementCollector {
    private final Object key;

    KeyedElementCollector(Collector<Datum> wrap, Window window, Object key,
        Supplier<Long> stampSupplier) {
      super(wrap, stampSupplier);
      this.key = key;
      this.window = window;
    }

    @Override
    public void collect(Object elem) {
      super.collect(Pair.of(key, elem));
    }
  } // ~ end of KeyedElementCollector

  final class ClearingValueStorage<T> implements ValueStorage<T> {
    private final ValueStorage<T> wrap;
    private final KeyedWindow scope;
    private final StorageDescriptorBase descriptor;

    ClearingValueStorage(ValueStorage<T> wrap,
                         KeyedWindow scope,
                         StorageDescriptorBase descriptor) {
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
    private final StorageDescriptorBase descriptor;

    public ClearingListStorage(ListStorage<T> wrap, KeyedWindow scope,
                               StorageDescriptorBase descriptor) {
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
    private KeyedWindow scope;

    ElementTriggerContext(KeyedWindow scope) {
      this.scope = scope;
    }

    void setScope(KeyedWindow scope) {
      this.scope = scope;
    }

    KeyedWindow getScope() {
      return scope;
    }

    @Override
    public boolean registerTimer(long stamp, Window window) {
      Preconditions.checkState(this.scope.window().equals(window));
      return scheduler.scheduleAt(
          stamp, this.scope, guardTriggerable(createTriggerHandler()));
    }

    @Override
    public void deleteTimer(long stamp, Window window) {
      Preconditions.checkState(this.scope.window().equals(window));
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
      return new ClearingListStorage<T>(
          processing.triggerStorage.getListStorage(this.scope, descriptor),
          this.scope,
          descriptor);
    }
  } // ~ end of ElementTriggerContext

  class MergingElementTriggerContext
      extends ElementTriggerContext
      implements TriggerContext.TriggerMergeContext {
    final Collection<KeyedWindow> mergeSources;

    MergingElementTriggerContext(KeyedWindow target, Collection<KeyedWindow> sources) {
      super(target);
      this.mergeSources = sources;
    }

    @Override
    public void mergeStoredState(StorageDescriptorBase storageDescriptor) {
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
      for (KeyedWindow w : this.mergeSources) {
        Storage s = processing.triggerStorage.getStorage(w, storageDescriptor);
        if (s != null) {
          mergeFn.apply(merged, s);
        }
      }
    }
  } // ~ end of MergingElementTriggerContext

  static final class WindowRegistry {

    final Map<Window, Map<Object, State>> windows = new HashMap<>();
    final Map<Object, Set<Window>> keyMap = new HashMap<>();

    State removeWindowState(KeyedWindow kw) {
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

    void setWindowState(KeyedWindow kw, State state) {
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

    State getWindowState(KeyedWindow kw) {
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
      LOG.info("Reducer {} processing stats: at watermark {}, maxElementStamp {}",
          new Object[] {
            ReduceStateByKeyReducer.this.name,
            watermarkPassed,
            maxElementStamp});
    }
  } // ~ end of ProcessingStats

  static final class ScopedStorage {
    final class StorageKey {
      private Object itemKey;
      private Window itemWindow;
      private String storeId;

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

    final HashMap<StorageKey, Object> store = new HashMap<>();
    final StorageProvider storageProvider;

    ScopedStorage(StorageProvider storageProvider) {
      this.storageProvider = storageProvider;
    }

    Storage removeStorage(KeyedWindow scope, StorageDescriptorBase descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      return (Storage) store.remove(skey);
    }

    Storage getStorage(KeyedWindow scope, StorageDescriptorBase descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      return (Storage) store.get(skey);
    }

    void putStorage(KeyedWindow scope,
                    StorageDescriptorBase descriptor,
                    Storage storage) {

      store.put(storageKey(scope, descriptor), storage);
    }

    <T> ValueStorage<T> getValueStorage(
        KeyedWindow scope, ValueStorageDescriptor<T> descriptor)
    {
      StorageKey skey = storageKey(scope, descriptor);
      Storage s = (Storage) store.get(skey);
      if (s == null) {
        store.put(skey, s = storageProvider.getValueStorage(descriptor));
      }
      return (ValueStorage<T>) s;
    }

    <T> ListStorage<T> getListStorage(
        KeyedWindow scope, ListStorageDescriptor<T> descriptor) {
      StorageKey skey = storageKey(scope, descriptor);
      Storage s = (Storage) store.get(skey);
      if (s == null) {
        store.put(skey, s = storageProvider.getListStorage(descriptor));
      }
      return (ListStorage<T>) s;
    }

    private StorageKey storageKey(KeyedWindow kw, StorageDescriptorBase desc) {
      return new StorageKey(kw.key(), kw.window(), desc.getName());
    }
  } // ~ end of ScopedStorage

  final class ProcessingState {

    final ScopedStorage triggerStorage;
    final StorageProvider storageProvider;
    final WindowRegistry wRegistry = new WindowRegistry();

    final Collector<Datum> stateOutput;
    final BlockingQueue<Datum> rawOutput;
    final TriggerScheduler triggering;
    final StateFactory stateFactory;
    final CombinableReduceFunction stateCombiner;

    final ProcessingStats stats = new ProcessingStats(this);

    // flushed windows with the time of the flush
    private Map<Window, Long> flushedWindows = new HashMap<>();

    @SuppressWarnings("unchecked")
    private ProcessingState(
        BlockingQueue<Datum> output,
        TriggerScheduler triggering,
        StateFactory stateFactory,
        CombinableReduceFunction stateCombiner,
        StorageProvider storageProvider) {

      this.triggerStorage = new ScopedStorage(storageProvider);
      this.storageProvider = storageProvider;
      this.stateOutput = QueueCollector.wrap(requireNonNull(output));
      this.rawOutput = output;
      this.triggering = requireNonNull(triggering);
      this.stateFactory = requireNonNull(stateFactory);
      this.stateCombiner = requireNonNull(stateCombiner);
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

    /**
     * Flushes (emits result) the specified window.
     */
    void flushWindow(KeyedWindow kw) {
      State state = wRegistry.getWindowState(kw);
      if (state == null) {
        return;
      }
      state.flush();
      // ~ remember we flushed the window such that we can emit one
      // notification to downstream operators for all keys in this window
      flushedWindows.put(kw.window(), getCurrentWatermark());
    }

    /**
     * Purges the specified window.
     */
    State purgeWindow(KeyedWindow kw) {
      State state = wRegistry.removeWindowState(kw);
      if (state == null) {
        return null;
      }
      state.close();
      return state;
    }

    /**
     * Flushes and closes all window storages and clear the window registry.
     */
    void flushAndCloseAllWindows() {
      for (Map.Entry<Window, Map<Object, State>> windowState : wRegistry.windows.entrySet()) {
        for (Map.Entry<Object, State> itemState : windowState.getValue().entrySet()) {
          State state = itemState.getValue();
          state.flush();
          state.close();
        }
      }
      wRegistry.windows.clear();
    }

    State getWindowStateForUpdate(KeyedWindow kw) {
      State state = wRegistry.getWindowState(kw);
      if (state == null) {
        // ~ if no such window yet ... set it up
        state = (State) stateFactory.apply(
                new KeyedElementCollector(
                    stateOutput, kw.window(), kw.key(),
                    processing.triggering::getCurrentTimestamp),
                storageProvider);
        wRegistry.setWindowState(kw, state);
      }
      return state;
    }

    Collection<Window> getActivesForKey(Object itemKey) {
      Set<Window> actives = wRegistry.getActivesForKey(itemKey);
      if (actives == null || actives.isEmpty()) {
        return Collections.emptyList();
      } else {
        List<Window> ctxs = new ArrayList<>(actives.size());
        for (Window active : actives) {
          State state = wRegistry.getWindowState(active, itemKey);
          Objects.requireNonNull(state, () -> "no storage for active window?! (key: " + itemKey + " / window: " + active + ")");
          ctxs.add(active);
        }
        return ctxs;
      }
    }

    Trigger.TriggerResult mergeWindows(Collection<Window> toBeMerged, KeyedWindow tkw) {

      State mergeWindowState = getWindowStateForUpdate(tkw);

      List<Pair<KeyedWindow, State>> toCombine = new ArrayList<>(toBeMerged.size());
      for (Window toMerge : toBeMerged) {
        // ~ skip "self merge requests" to prevent removing its window storage
        if (toMerge.equals(tkw.window())) {
          continue;
        }
        // ~ remove the toMerge window and merge its state into the mergeWindow
        KeyedWindow<Window, Object> wkw = new KeyedWindow<>(toMerge, tkw.key());
        State toMergeState = wRegistry.removeWindowState(wkw);
        if (toMergeState != null) {
          toCombine.add(Pair.of(wkw, toMergeState));
        }
      }
      Trigger.TriggerResult tr = Trigger.TriggerResult.NOOP;
      if (!toCombine.isEmpty()) {
        // ~ merge trigger states
        tr = trigger.onMerge(tkw.window(),
            new MergingElementTriggerContext(
                tkw, toCombine.stream().map(Pair::getFirst).collect(toList())));
        // ~ clear trigger states for the merged windows
        for (Pair<KeyedWindow, State> ws : toCombine) {
          KeyedWindow wkw = ws.getFirst();
          trigger.onClear(wkw.window(), new ElementTriggerContext(wkw));
        }
        // ~ now merge the windows' state
        ArrayList<State> states = new ArrayList<>(toCombine.size() + 1);
        // ~ add the merge window to the list of windows to combine;
        // by definition, we'll have it at the very first position in the list
        states.add(mergeWindowState);
        // ~ if any of the states emits any data during the merge, we'll make
        // sure it happens in the scope of the merge target window
        for (Pair<KeyedWindow, State> tc : toCombine) {
          State st = tc.getSecond();
          states.add(st);
          ((KeyedElementCollector) st.getContext()).setWindow(tkw.window());
        }
        // ~ now merge the state and re-assign it to the merge-window
        State newState = (State) stateCombiner.apply(states);
        wRegistry.setWindowState(tkw, newState);

      }
      return tr;
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
        KeyedWindow kw = new KeyedWindow<>(window, e.getKey());

        Triggerable t = guardTriggerable((tstamp, tkw) -> {
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

  private final BlockingQueue<Datum> input;
  private final BlockingQueue<Datum> output;

  private final boolean isAttachedWindowing;
  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;
  private final StateFactory stateFactory;
  private final CombinableReduceFunction stateCombiner;
  private final WatermarkEmitStrategy watermarkStrategy;
  private final String name;

  private final Trigger trigger;

  // ~ both of these are guarded by "processing"
  private final ProcessingState processing;
  private final TriggerScheduler scheduler;

  private long currentElementTime;

  @SuppressWarnings("rawtypes")
  ReduceStateByKeyReducer(ReduceStateByKey operator,
                          String name,
                          BlockingQueue<Datum> input,
                          BlockingQueue<Datum> output,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          TriggerScheduler scheduler,
                          WatermarkEmitStrategy watermarkStrategy,
                          StorageProvider storageProvider) {

    this.name = requireNonNull(name);
    this.input = requireNonNull(input);
    this.output = requireNonNull(output);
    this.isAttachedWindowing = operator.getWindowing() == null;
    this.windowing = isAttachedWindowing
        ? AttachedWindowing.INSTANCE : replaceTimeFunction(operator.getWindowing());
    this.keyExtractor = requireNonNull(keyExtractor);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.stateFactory = requireNonNull(operator.getStateFactory());
    this.stateCombiner = requireNonNull(operator.getStateCombiner());
    this.watermarkStrategy = requireNonNull(watermarkStrategy);
    this.trigger = requireNonNull(windowing.getTrigger());
    this.scheduler = requireNonNull(scheduler);
    this.processing = new ProcessingState(
        output, scheduler,
        stateFactory, stateCombiner,
        storageProvider);
  }

  Triggerable guardTriggerable(Triggerable t) {
    return ((timestamp, kw) -> {
      synchronized (processing) {
        t.fire(timestamp, kw);
      }
    });
  }

  Triggerable createTriggerHandler() {
    return ((timestamp, kw) -> {
      // ~ let trigger know about the time event and process window state
      // according to trigger result
      ElementTriggerContext ectx = new ElementTriggerContext(kw);
      Trigger.TriggerResult result = trigger.onTimeEvent(timestamp, kw.window(), ectx);
      handleTriggerResult(result, ectx);
    });
  }

  void handleTriggerResult(
      Trigger.TriggerResult result, ElementTriggerContext ctx) {

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
            new Object[]{ctx.getScope().window(), ctx.getScope().key(),
                         getCurrentWatermark(), scheduler.getClass()});
      }
    }
  }

  @SuppressWarnings("unchecked")
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
            currentElementTime = item.getStamp();
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

  private final ElementTriggerContext pitctx = new ElementTriggerContext(null);

  @SuppressWarnings("unchecked")
  private void processInput(WindowedElement element) {
    Object item = element.get();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<Window> windows = windowing.assignWindowsToElement(element);
    for (Window window : windows) {
      pitctx.setScope(new KeyedWindow(window, itemKey));

      State windowState = processing.getWindowStateForUpdate(pitctx.getScope());
      windowState.add(itemValue);
      Trigger.TriggerResult result =
          trigger.onElement(getCurrentElementTime(), item, window, pitctx);
      // ~ handle trigger result
      handleTriggerResult(result, pitctx);

      // ~ deal with window merging if required
      if (windowing instanceof MergingWindowing) {
        Collection<Window> actives = processing.getActivesForKey(itemKey);
        if (actives == null || actives.isEmpty()) {
          // nothing to do
        } else {
          Collection<Pair<Collection<Window>, Window>> merges =
              ((MergingWindowing) this.windowing).mergeWindows(actives);
          if (merges != null && !merges.isEmpty()) {
            for (Pair<Collection<Window>, Window> merge : merges) {
              KeyedWindow tkw = new KeyedWindow<>(merge.getSecond(), itemKey);
              Trigger.TriggerResult tr = processing.mergeWindows(merge.getFirst(), tkw);
              pitctx.setScope(tkw);
              handleTriggerResult(tr, pitctx);
            }
          }
        }
      }
    }
  }

  private void processWatermark(Datum.Watermark watermark) {
    // update current stamp
    long stamp = watermark.getStamp();
    processing.updateStamp(stamp);
  }

  private void processWindowTrigger(Datum.WindowTrigger trigger) {
    if (isAttachedWindowing) {
      // reregister trigger of given window
      // FIXME: move this to windowing itself so that attached windowing
      // can be implemented 'natively' as instance of generic windowing
      processing.onUpstreamWindowTrigger(trigger.getWindow(), trigger.getStamp());
    }
  }

  private void processEndOfStream(Datum.EndOfStream eos) throws InterruptedException {
    // ~ stop triggers
    scheduler.close();
    // close all states
    processing.flushAndCloseAllWindows();
    processing.closeOutput();
    output.put(eos);
  }

  // retrieve current watermark stamp
  private long getCurrentWatermark() {
    return scheduler.getCurrentTimestamp();
  }

  /**
   * Replace time function to element time for time windowing's with
   * procesing time.
   */
  private Windowing replaceTimeFunction(Windowing windowing) {
    if (!(windowing instanceof Time) ||
        !(((Time) windowing).getEventTimeFn() instanceof ProcessingTime)) {
      return windowing;
    }
    Time timeWindowing = (Time) windowing;
    if (timeWindowing.getEarlyTriggeringPeriod() == null) {
      return Time.of(Duration.ofMillis(timeWindowing.getDuration()))
          .using(o -> getCurrentElementTime());
    }
    return Time.of(Duration.ofMillis(timeWindowing.getDuration()))
        .earlyTriggering(timeWindowing.getEarlyTriggeringPeriod())
        .using(o -> getCurrentElementTime());
  }

  private long getCurrentElementTime() {
    return currentElementTime;
  }



}

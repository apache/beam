package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Time.ProcessingTime;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.QueueCollector;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

import static java.util.Objects.requireNonNull;
import java.util.function.Supplier;

class ReduceStateByKeyReducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);

  private static final class KeyedElementCollector extends WindowedElementCollector {
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

  private class ElementTriggerContext implements TriggerContext {
    private final Object itemKey;

    ElementTriggerContext(Object itemKey) {
      this.itemKey = itemKey;
    }

    @Override
    public boolean registerTimer(long stamp, Window window) {
      KeyedWindow kw = new KeyedWindow<>(window, itemKey);
      return triggering.scheduleAt(
              stamp, kw, guardTriggerable(createTriggerHandler(trigger)));
    }

    @Override
    public void deleteTimer(long stamp, Window window) {
      triggering.cancel(stamp, new KeyedWindow<>(window, itemKey));
    }

    @Override
    public long getCurrentTimestamp() {
      return triggering.getCurrentTimestamp();
    }

    @Override
    public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
      return null;
    }

    @Override
    public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
      return null;
    }
  } // ~ end of ElementContext

  @SuppressWarnings("unchecked")
  private static final class WindowRegistry {

    final Map<Window, Map<Object, State>> windows = new HashMap<>();
    final Map<Object, Set<Window>> keyMap = new HashMap<>();

    State removeWindowState(Window window, Object itemKey) {
      Map<Object, State> keys = windows.get(window);
      if (keys != null) {
        State state = keys.remove(itemKey);
        // ~ garbage collect on windows level
        if (keys.isEmpty()) {
          windows.remove(window);
        }
        Set<Window> actives = keyMap.get(itemKey);
        if (actives != null) {
          actives.remove(window);
          if (actives.isEmpty()) {
            keyMap.remove(itemKey);
          }
        }
        return state;
      }
      return null;
    }

    void setWindowState(Window window, Object itemKey, State state) {
      Map<Object, State> keys = windows.get(window);
      if (keys == null) {
        windows.put(window, keys = new HashMap<>());
      }
      keys.put(itemKey, state);
      Set<Window> actives = keyMap.get(itemKey);
      if (actives == null) {
        keyMap.put(itemKey, actives = new HashSet<>());
      }
      actives.add(window);
    }

    State getWindowState(Window window, Object itemKey) {
      Map<Object, State> keys = windows.get(window);
      if (keys != null) {
        return keys.get(itemKey);
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
  private final class ProcessingStats {

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

  private final class ProcessingState {

    final StorageProvider storageProvider;
    final WindowRegistry wRegistry = new WindowRegistry();
    final int maxKeyStatesPerWindow;

    final Collector<Datum> stateOutput;
    final BlockingQueue<Datum> rawOutput;
    final TriggerScheduler triggering;
    final StateFactory stateFactory;
    final CombinableReduceFunction stateCombiner;

    final ProcessingStats stats = new ProcessingStats(this);

    // do we have bounded input?
    // if so, do not register windows for triggering, just trigger
    // windows at the end of input
    private final boolean isBounded;

    // flushed windows with the time of the flush
    private Map<Window, Long> flushedWindows = new HashMap<>();

    @SuppressWarnings("unchecked")
    private ProcessingState(
        BlockingQueue<Datum> output,
        TriggerScheduler triggering,
        StateFactory stateFactory,
        CombinableReduceFunction stateCombiner,
        StorageProvider storageProvider,
        boolean isBounded,
        int maxKeyStatesPerWindow) {

      this.storageProvider = storageProvider;
      this.stateOutput = QueueCollector.wrap(requireNonNull(output));
      this.rawOutput = output;
      this.triggering = requireNonNull(triggering);
      this.stateFactory = requireNonNull(stateFactory);
      this.stateCombiner = requireNonNull(stateCombiner);
      this.isBounded = isBounded;
      this.maxKeyStatesPerWindow = maxKeyStatesPerWindow;
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
    void flushWindow(Window window, Object itemKey) {
      State state = wRegistry.getWindowState(window, itemKey);
      if (state == null) {
        return;
      }
      state.flush();
      // ~ remember we flushed the window such that we can emit one
      // notification to downstream operators for all keys in this window
      flushedWindows.put(window, getCurrentWatermark());
    }

    /**
     * Purges the specified window.
     */
    State purgeWindow(Window window, Object itemKey) {
      State state = wRegistry.removeWindowState(window, itemKey);
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

    State lookupWindowState(Window window, Object itemKey) {
      return wRegistry.getWindowState(window, itemKey);
    }

    State getWindowStateForUpdate(Window window, Object itemKey) {
      State state = wRegistry.getWindowState(window, itemKey);
      if (state == null) {
        // ~ if no such window yet ... set it up
        state = (State) stateFactory.apply(
                new KeyedElementCollector(
                    stateOutput, window, itemKey,
                    processing.triggering::getCurrentTimestamp),
                storageProvider);
        wRegistry.setWindowState(window, itemKey, state);
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

    boolean mergeWindows(
            Collection<Window> toBeMerged, Window mergeWindow, Object itemKey) {

      State mergeWindowState =
          getWindowStateForUpdate(mergeWindow, itemKey);
      if (mergeWindowState == null) {
        LOG.warn("No window storage for {}; triggering discarded it!", mergeWindow);
        for (Window w : toBeMerged) {
          purgeWindow(w, itemKey);
        }
        return false;
      }

      List<State> toCombine = new ArrayList<>(toBeMerged.size());
      for (Window toMerge : toBeMerged) {
        // ~ skip "self merge requests" to prevent removing its window storage
        if (toMerge.equals(mergeWindow)) {
          continue;
        }
        // ~ remove the toMerge window and merge its state into the mergeWindow
        State toMergeState =
            wRegistry.removeWindowState(toMerge, itemKey);
        if (toMergeState != null) {
          toCombine.add(toMergeState);
        }
      }
      if (!toCombine.isEmpty()) {
        // ~ add the merge window to the list of windows to combine;
        // by definition, we'll have it at the very first position in the list
        toCombine.add(0, mergeWindowState);
        // ~ if any of the states emits any data during the merge, we'll make
        // sure it happens in the scope of the merge target window
        for (State ws : toCombine) {
          ((KeyedElementCollector) ws.getContext()).setWindow(mergeWindow);
        }
        // ~ now merge the state and re-assign it to the merge-window
        State newState =(State) stateCombiner.apply(toCombine);
        mergeWindowState.state = newState;

      }

      return true;
    }

    /** Update current timestamp by given watermark. */
    void updateStamp(long stamp) {
      triggering.updateStamp(stamp);
    }

    /** Update trigger of given window ID. */
    void onUpstreamWindowTrigger(WindowID windowID, long stamp) {
      LOG.debug("Updating trigger of window {} to {}", windowID, stamp);

      Map<Object, WindowStorage> ws = wRegistry.getWindowStates(windowID);
      if (ws == null || ws.isEmpty()) {
        return;
      }

      for (Map.Entry<Object, WindowStorage> e : ws.entrySet()) {
        KeyedWindow kw = new KeyedWindow(e.getValue().window.getWindowID(), e.getKey());
        triggering.cancel(kw);

        Triggerable t = guardTriggerable((timestamp, window) -> {
          flushWindow(window.window(), window.key());
          purgeWindow(window.window(), window.key());
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
  private final TriggerScheduler triggering;

  private long currentElementTime;

  @SuppressWarnings("rawtypes")
  ReduceStateByKeyReducer(ReduceStateByKey operator,
                          String name,
                          BlockingQueue<Datum> input,
                          BlockingQueue<Datum> output,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          TriggerScheduler triggering,
                          WatermarkEmitStrategy watermarkStrategy,
                          StorageProvider storageProvider,
                          boolean isBounded,
                          int maxKeyStatesPerWindow) {

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
    this.trigger = windowing.getTrigger();
    this.triggering = requireNonNull(triggering);
    this.processing = new ProcessingState(
        output, triggering,
        stateFactory, stateCombiner,
        storageProvider, isBounded,
        maxKeyStatesPerWindow);
  }

  Triggerable guardTriggerable(Triggerable t) {
    return ((timestamp, kw) -> {
      synchronized (processing) {
        t.fire(timestamp, kw);
      }
    });
  }

  Triggerable createTriggerHandler(Trigger t) {
    return ((timestamp, kw) -> {
      // ~ let trigger know about the time event and process window state
      // according to trigger result
      ElementTriggerContext ectx = new ElementTriggerContext(kw.key());
      WindowStorage w = processing.lookupWindowState(kw.window(), kw.key());
      if (w == null) {
        throw new IllegalStateException("Window already gone?! (" + kw + ")");
      }
      Trigger.TriggerResult result = t.onTimeEvent(timestamp, w.window, ectx);

      // Flush window (emit the internal state to output)
      if (result.isFlush()) {
        processing.flushWindow(kw.window(), kw.key());
      }
      // Purge given window (discard internal state and cancel all triggers)
      if (result.isPurge()) {
        WindowStorage state = processing.purgeWindow(kw.window(), kw.key());
        if (state != null) {
          triggering.cancel(kw);
        }
      }
    });
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

  private void processInput(WindowedElement element) {
    Object item = element.get();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<Window> windows = windowing.assignWindowsToElement(element);
    for (Window window : windows) {
      State windowState =
          processing.getWindowStateForUpdate(window, itemKey);
      if (windowState != null) {
        windowState.addValue(itemValue);
      } else {
        // window is already closed
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Window {} discarded for key {} at current watermark {} with triggering {}",
            new Object[] { window, itemKey, getCurrentWatermark(), triggering.getClass() });
        }
      }

      if (windowing instanceof MergingWindowing) {
        Collection<Window> actives = processing.getActivesForKey(itemKey);
        if (actives == null || actives.isEmpty()) {
          // nothing to do
        } else {
          MergingWindowing mwindowing = (MergingWindowing) this.windowing;

          Collection<Pair<Collection<Window>, Window>> merges =
              mwindowing.mergeWindows(actives);
          if (merges != null && !merges.isEmpty()) {
            for (Pair<Collection<Window>, Window> merge : merges) {
              boolean merged = processing.mergeWindows(
                  merge.getFirst(), merge.getSecond(), itemKey);
              if (merged) {
                // ~ cancel all pending triggers for the windows which there merged
                merge.getFirst().forEach(wctx ->
                    triggering.cancel(new KeyedWindow(wctx.getWindowID(), itemKey)));
                // ~ check if the target window is complete
                if (mwindowing.isComplete(merge.getSecond())) {
                  processing.flushWindow(merge.getSecond().getWindowID(), itemKey);
                  processing.purgeWindow(merge.getSecond().getWindowID(), itemKey);
                }
              }
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
    triggering.close();
    // close all states
    processing.flushAndCloseAllWindows();
    processing.closeOutput();
    output.put(eos);
  }

  // retrieve current watermark stamp
  private long getCurrentWatermark() {
    return triggering.getCurrentTimestamp();
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

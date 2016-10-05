package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.QueueCollector;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
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

class ReduceStateByKeyReducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);

  private static final class KeyedElementCollector extends WindowedElementCollector {
    private final Object key;

    KeyedElementCollector(Collector<Datum> wrap, WindowID window, Object key) {
      super(wrap);
      this.key = key;
      this.windowID = window;
    }

    @Override
    public void collect(Object elem) {
      super.collect(WindowedPair.of(windowID.getLabel(), key, elem));
    }
  } // ~ end of KeyedElementCollector

  private class ElementContext implements TriggerContext {
    private final Object itemKey;

    ElementContext(Object itemKey) {
      this.itemKey = itemKey;
    }

    @Override
    public boolean scheduleTriggerAt(long stamp, WindowContext w, Trigger trigger) {
      KeyedWindow kw = new KeyedWindow<>(w.getWindowID(), itemKey);
      return triggering.scheduleAt(
          stamp, kw, guardTriggerable(createTriggerHandler(trigger)));
    }

    @Override
    public long getCurrentTimestamp() {
      return triggering.getCurrentTimestamp();
    }
  } // ~ end of ElementContext

  // ~ storage of a single window's (internal) state
  private static final class WindowStorage {
    private final WindowContext windowContext;
    private State state;

    public WindowStorage(WindowContext windowContext, State state) {
      this.windowContext = windowContext;
      this.state = state;
    }

    void addValue(Object itemValue) {
      this.state.add(itemValue);
    }
  } // ~ end of WindowStorage

  private static final class WindowRegistry {

    final Map<WindowID, Map<Object, WindowStorage>> windows = new HashMap<>();
    final Map<Object, Set<WindowID>> keyMap = new HashMap<>();

    WindowStorage removeWindowStorage(WindowID window, Object itemKey) {
      Map<Object, WindowStorage> keys = windows.get(window);
      if (keys != null) {
        WindowStorage state = keys.remove(itemKey);
        // ~ garbage collect on windows level
        if (keys.isEmpty()) {
          windows.remove(window);
        }
        Set<WindowID> actives = keyMap.get(itemKey);
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

    void setWindowStorage(WindowID window, Object itemKey, WindowStorage storage) {
      Map<Object, WindowStorage> keys = windows.get(window);
      if (keys == null) {
        windows.put(window, keys = new HashMap<>());
      }
      keys.put(itemKey, storage);
      Set<WindowID> actives = keyMap.get(itemKey);
      if (actives == null) {
        keyMap.put(itemKey, actives = new HashSet<>());
      }
      actives.add(window);
    }

    WindowStorage getWindowStorage(WindowID window, Object itemKey) {
      Map<Object, WindowStorage> keys = windows.get(window);
      if (keys != null) {
        return keys.get(itemKey);
      }
      return null;
    }

    Map<Object, WindowStorage> getWindowStorages(WindowID window) {
      return windows.get(window);
    }

    Set<WindowID> getActivesForKey(Object itemKey) {
      return keyMap.get(itemKey);
    }
  } // ~ end of WindowRegistry

  private final class ProcessingState {

    final StorageProvider storageProvider;
    final WindowRegistry wRegistry = new WindowRegistry();
    final int maxKeyStatesPerWindow;

    final Collector<Datum> stateOutput;
    final BlockingQueue<Datum> rawOutput;
    final TriggerScheduler triggering;
    final StateFactory stateFactory;
    final CombinableReduceFunction stateCombiner;

    // do we have bounded input?
    // if so, do not register windows for triggering, just trigger
    // windows at the end of input
    private final boolean isBounded;

    private Set<WindowID> flushedWindows = new HashSet<>();

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

    Set<WindowID> takeFlushedWindows() {
      if (flushedWindows.isEmpty()) {
        return Collections.emptySet();
      }
      Set<WindowID> flushed = flushedWindows;
      flushedWindows = new HashSet<>();
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
     * Flushes (emits result) the specified window
     */
    void flushWindow(WindowID window, Object itemKey) {
      WindowStorage state = wRegistry.getWindowStorage(window, itemKey);
      if (state == null) {
        return;
      }
      state.state.flush();
      // ~ remember we flushed the window such that we can emit one
      // notification to downstream operators for all keys in this window
      flushedWindows.add(window);
    }

    /**
     * Purges the specified window.
     */
    WindowStorage purgeWindow(WindowID window, Object itemKey) {
      WindowStorage state = wRegistry.removeWindowStorage(window, itemKey);
      if (state == null) {
        return null;
      }
      state.state.close();
      return state;
    }

    /**
     * Flushes and closes all window storages and clear the window registry.
     */
    void flushAndCloseAllWindows() {
      for (Map.Entry<WindowID, Map<Object, WindowStorage>> windowState : wRegistry.windows.entrySet()) {
        for (Map.Entry<Object, WindowStorage> itemState : windowState.getValue().entrySet()) {
          State state = itemState.getValue().state;
          state.flush();
          state.close();
        }
      }
      wRegistry.windows.clear();
    }

    WindowStorage lookupWindowState(WindowID window, Object itemKey) {
      return wRegistry.getWindowStorage(window, itemKey);
    }

    WindowStorage getWindowStateForUpdate(WindowID window, Object itemKey) {
      WindowStorage wStore = wRegistry.getWindowStorage(window, itemKey);
      if (wStore == null) {
        WindowContext wctx = windowing.createWindowContext(window);

        Trigger.TriggerResult triggerState = Trigger.TriggerResult.NOOP;
        if (!isBounded) {
          // ~ default policy is to NOOP with current window
          triggerState = Trigger.TriggerResult.NOOP;

          // ~ give the window a chance to register triggers
          List<Trigger> triggers = wctx.createTriggers();
          if (!triggers.isEmpty()) {
            // ~ default policy for time-triggered window
            triggerState = Trigger.TriggerResult.PASSED;
          }
          ElementContext ectx = new ElementContext(itemKey);
          for (Trigger t : triggers) {
            Trigger.TriggerResult result = t.schedule(wctx, ectx);
            if (result == Trigger.TriggerResult.NOOP) {
              triggerState = Trigger.TriggerResult.NOOP;
            }
          }
        }
        if (triggerState == Trigger.TriggerResult.PASSED) {
          // the window should have been already triggered
          // just discard the element, no other option for now
          return null;
        } else {
          // ~ if no such window yet ... set it up
          wStore = new WindowStorage(wctx, (State) stateFactory.apply(
              new KeyedElementCollector(stateOutput, window, itemKey),
              storageProvider));
          wRegistry.setWindowStorage(window, itemKey, wStore);
        }
      }
      return wStore;
    }

    Collection<WindowContext> getActivesForKey(Object itemKey) {
      Set<WindowID> actives = wRegistry.getActivesForKey(itemKey);
      if (actives == null || actives.isEmpty()) {
        return Collections.emptyList();
      } else {
        List<WindowContext> ctxs = new ArrayList<>(actives.size());
        for (WindowID active : actives) {
          WindowStorage state = wRegistry.getWindowStorage(active, itemKey);
          Objects.requireNonNull(state, () -> "no storage for active window?! (key: " + itemKey + " / window: " + active + ")");
          ctxs.add(state.windowContext);
        }
        return ctxs;
      }
    }

    boolean mergeWindows(
        Collection<WindowContext> toBeMerged, WindowContext mergeWindow, Object itemKey) {

      WindowStorage mergeWindowState =
          getWindowStateForUpdate(mergeWindow.getWindowID(), itemKey);
      if (mergeWindowState == null) {
        LOG.warn("No window storage for {}; triggering discarded it!", mergeWindow);
        for (WindowContext w : toBeMerged) {
          purgeWindow(w.getWindowID(), itemKey);
        }
        return false;
      }

      List<WindowStorage> toCombine = new ArrayList<>(toBeMerged.size());
      for (WindowContext toMerge : toBeMerged) {
        // ~ skip "self merge requests" to prevent removing its window storage
        if (toMerge.getWindowID().equals(mergeWindowState.windowContext.getWindowID())) {
          continue;
        }
        // ~ remove the toMerge window and merge its state into the mergeWindow
        WindowStorage toMergeState =
            wRegistry.removeWindowStorage(toMerge.getWindowID(), itemKey);
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
        for (WindowStorage ws : toCombine) {
          ((KeyedElementCollector) ws.state.getCollector())
              .assignWindowing(mergeWindow.getWindowID());
        }
        // ~ now merge the state and re-assign it to the merge-window
        State newState =
            (State) stateCombiner.apply(Iterables.transform(toCombine, ws -> ws.state));
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

      Map<Object, WindowStorage> ws = wRegistry.getWindowStorages(windowID);
      if (ws == null || ws.isEmpty()) {
        return;
      }

      for (Map.Entry<Object, WindowStorage> e : ws.entrySet()) {
        KeyedWindow kw = new KeyedWindow(e.getValue().windowContext.getWindowID(), e.getKey());
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

  // ~ both of these are guarded by "processing"
  private final ProcessingState processing;
  private final TriggerScheduler triggering;

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
        ? AttachedWindowing.INSTANCE : operator.getWindowing();
    this.keyExtractor = requireNonNull(keyExtractor);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.stateFactory = requireNonNull(operator.getStateFactory());
    this.stateCombiner = requireNonNull(operator.getStateCombiner());
    this.watermarkStrategy = requireNonNull(watermarkStrategy);
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
      ElementContext ectx = new ElementContext(kw.key());
      WindowStorage w = processing.lookupWindowState(kw.window(), kw.key());
      if (w == null) {
        throw new IllegalStateException("Window already gone?! (" + kw + ")");
      }
      Trigger.TriggerResult result = t.onTimeEvent(timestamp, w.windowContext, ectx);

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
          watermarkStrategy.emitIfNeeded(processing::emitWatermark);
          if (item.isElement()) {
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
    long stamp = getCurrentWatermark();
    for (WindowID w : processing.takeFlushedWindows()) {
      output.put(Datum.windowTrigger(w, stamp));
      output.put(Datum.watermark(stamp));
    }
  }

  private void processInput(WindowedElement element) {
    Object item = element.get();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<WindowID<Object>> windows = windowing.assignWindowsToElement(element);
    for (WindowID window : windows) {
      WindowStorage windowState =
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
        Collection<WindowContext> actives = processing.getActivesForKey(itemKey);
        if (actives == null || actives.isEmpty()) {
          // nothing to do
        } else {
          MergingWindowing mwindowing = (MergingWindowing) this.windowing;

          Collection<Pair<Collection<WindowContext>, WindowContext>> merges =
              mwindowing.mergeWindows(actives);
          if (merges != null && !merges.isEmpty()) {
            for (Pair<Collection<WindowContext>, WindowContext> merge : merges) {
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
    long stamp = watermark.getWatermark();
    processing.updateStamp(stamp);
  }

  private void processWindowTrigger(Datum.WindowTrigger trigger) {
    if (isAttachedWindowing) {
      // reregister trigger of given window
      // FIXME: move this to windowing itself so that attached windowing
      // can be implemented 'natively' as instance of generic windowing
      processing.onUpstreamWindowTrigger(trigger.getWindowID(), trigger.getStamp());
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

}

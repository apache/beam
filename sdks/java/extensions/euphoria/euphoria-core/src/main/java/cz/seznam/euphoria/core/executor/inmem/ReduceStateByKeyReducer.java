package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.triggers.Triggerable;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.QueueCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

class ReduceStateByKeyReducer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);

  private static final class LRU<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;
    LRU(int maxSize) { this.maxSize = maxSize; }
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > maxSize;
    }
  }

  // ~ storage of a single window's (internal) state
  private static final class WindowStorage {
    private final WindowContext<Object, Object> windowContext;
    private final Map<Object, State<?, ?>> keyStates;
    WindowStorage(WindowContext<Object, Object> windowContext, int maxKeysStates) {
      this.windowContext = requireNonNull(windowContext);
      if (maxKeysStates > 0) {
        this.keyStates = new LRU<>(maxKeysStates);
      } else {
        this.keyStates = new HashMap<>();
      }
    }
    // ~ make a copy of 'base' with the specified 'window' assigned
    WindowStorage(WindowContext<Object, Object> windowContext, WindowStorage base) {
      this.windowContext = requireNonNull(windowContext);
      this.keyStates = base.keyStates;
    }
    WindowContext<Object, Object> getWindowContext() {
      return windowContext;
    }
    Map<Object, State<?, ?>> getKeyStates() {
      return keyStates;
    }
  } // ~ end of WindowStorage

  private static final class WindowRegistry {
    
    final Map<WindowID<Object, Object>, WindowStorage> windows = new HashMap<>();
    final Map<Object, Set<WindowID<Object, Object>>> groupMap = new HashMap<>();

    // ~ removes the given window and returns its key states (possibly null)
    WindowStorage removeWindow(WindowID<Object, Object> wid) {
      Set<WindowID<Object, Object>> groupIds = groupMap.get(wid.getGroup());
      if (groupIds != null) {
        groupIds.remove(wid);
        if (groupIds.isEmpty()) {
          groupMap.remove(wid.getGroup());
        }
      }
      return windows.remove(wid);
    }

    WindowStorage getWindowStorage(WindowID<Object, Object> wid) {
      return windows.get(wid);
    }

    void addWindowStorage(WindowStorage store) {
      addWindowStorage(store, false);
    }

    void addWindowStorage(WindowStorage store, boolean allowOverride) {
      WindowContext<Object, Object> w = store.getWindowContext();
      WindowID<Object, Object> windowID = w.getWindowID();
      WindowStorage old = windows.put(windowID, store);
      Set<WindowID<Object, Object>> groupIds = groupMap.get(windowID.getGroup());
      if (groupIds == null) {
        groupIds = new HashSet<>();
        groupMap.put(windowID.getGroup(), groupIds);
      }
      groupIds.add(windowID);
      if (!allowOverride && old != null) {
        throw new IllegalArgumentException("Window ID " + windowID
            + " was already present");
      }
    }

    List<WindowContext<Object, Object>> getAllWindowsList() {
      return windows.values().stream()
          .map(WindowStorage::getWindowContext)
          .collect(Collectors.toList());
    }

    List<WindowContext> getWindowContextsForGroup(Object windowGroup) {
      Set<WindowID<Object, Object>> groupIds = groupMap.get(windowGroup);
      if (groupIds == null) {
        return Collections.emptyList();
      }
      return groupIds.stream().map(windows::get)
          .map(WindowStorage::getWindowContext)
          .collect(Collectors.toList());
   }
    
  } // ~ end of WindowRegistry

  private final class ProcessingState implements TriggerContext {

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

    // ~ are we still actively processing input?
    private boolean active = true;

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

    // ~ signal eos further down the output channel
    public void closeOutput() {
      try {
        this.rawOutput.put(Datum.endOfStream());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }


    /**
     * Flushes (emits result) the specified window
     * @return accumulated states for the specified window
     */
    private Collection<State<?, ?>> flushWindowStates(WindowContext<Object, Object> w) {
      WindowStorage wKeyStates = wRegistry.getWindowStorage(w.getWindowID());

      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State<?, ?>> keyStates = wKeyStates.getKeyStates();
      if (keyStates == null) {
        return Collections.emptySet();
      }

      return keyStates.values();
    }

    /**
     * Purges the specified window
     * @return accumulated states for the specified window
     */
    private Collection<State<?, ?>> purgeWindowStates(WindowContext<Object, Object> w) {
      WindowStorage wKeyStates = wRegistry.removeWindow(w.getWindowID());
      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State<?, ?>> keyStates = wKeyStates.getKeyStates();
      if (keyStates == null) {
        return Collections.emptySet();
      }

      return keyStates.values();
    }

    private Collection<WindowContext<Object, Object>> getAllWindowContexts() {
      return wRegistry.getAllWindowsList();
    }

    @SuppressWarnings("unchecked")
    public Pair<WindowContext, State<Object, Object>> getWindowStateForUpdate(
        WindowContext w, Object itemKey) {

      WindowStorage wStore = getOrCreateWindowStorage(w, false);
      if (wStore == null) {
        // the window is already closed
        return null;
      }
      // ~ remove and re-insert to give LRU based implementations a chance
      Map<Object, State<?, ?>> keyStates = wStore.getKeyStates();
      State state = keyStates.remove(itemKey);
      if (state == null) {
        // ~ collector decorating state output with a window label and item key
        WindowedElementCollector collector = new WindowedElementCollector(stateOutput) {
          @Override
          public void collect(Object elem) {
            super.collect(WindowedPair.of(
                super.windowID.getLabel(), itemKey, elem));
          }
        };
        collector.assignWindowing(w.getWindowID());
        state = (State) stateFactory.apply(collector, storageProvider);
        keyStates.put(itemKey, state);
      } else {
        keyStates.put(itemKey, state);
      }
      return Pair.of(wStore.getWindowContext(), state);
    }

    // ~ retrieves the WindowStorage associated with a window
    // and optionally override the window instance associated
    // with it
    private WindowStorage
    getOrCreateWindowStorage(WindowContext<Object, Object> w,
        boolean setWindowInstance) {
      
      WindowStorage wStore = wRegistry.getWindowStorage(w.getWindowID());
      if (wStore == null) {
        Trigger.TriggerResult triggerState = Trigger.TriggerResult.NOOP;
        if (!isBounded) {
          // ~ default policy is to NOOP with current window
          triggerState = Trigger.TriggerResult.NOOP;

          // ~ give the window a chance to register triggers
          List<Trigger> triggers = w.createTriggers();
          if (!triggers.isEmpty()) {
            // ~ default policy for time-triggered window
            triggerState = Trigger.TriggerResult.PASSED;
          }
          for (Trigger t : triggers) {
            Trigger.TriggerResult result = t.schedule(w, this);
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
          wRegistry.addWindowStorage(wStore = new WindowStorage(w, maxKeyStatesPerWindow));
        }
      } else if (setWindowInstance && wStore.getWindowContext() != w) {
        wRegistry.addWindowStorage(wStore = new WindowStorage(w, wStore), true);
      }
      return wStore;
    }

    public Collection<WindowContext> getActiveWindowsForGroup(Object windowGroup) {
      return wRegistry.getWindowContextsForGroup(windowGroup);
    }

    boolean mergeWindows(Collection<WindowContext<Object, Object>> toBeMerged,
        WindowContext<Object, Object> mergeWindow) {
      // ~ make sure 'mergeWindow' does exist
      WindowStorage ws = getOrCreateWindowStorage(mergeWindow, true);
      if (ws == null) {
        LOG.warn("No window storage for {}; potentially the triggering discarded it!",
            mergeWindow);
        wRegistry.removeWindow(mergeWindow.getWindowID());
        for (WindowContext<Object, Object> w : toBeMerged) {
          wRegistry.removeWindow(w.getWindowID());
        }
        return false;
      }

      for (WindowContext<Object, Object> toMerge : toBeMerged) {
        if (toMerge.getWindowID().equals(ws.getWindowContext().getWindowID())) {
          continue;
        }

        // ~ remove the toMerge window and merge all
        // of its keyStates into the mergeWindow
        WindowStorage toMergeState = wRegistry.removeWindow(toMerge.getWindowID());
        if (toMergeState != null) {
          mergeWindowKeyStates(
              toMergeState.getKeyStates(), ws.getKeyStates(), ws.getWindowContext());
        }
      }

      return true;
    }

    @SuppressWarnings("unchecked")
    private void mergeWindowKeyStates(
        Map<Object, State<?, ?>> src,
        Map<Object, State<?, ?>> dst,
        WindowContext<Object, Object> dstWindow) {
      
      List<State> toCombine = new ArrayList<>(2);

      for (Map.Entry<Object, State<?, ?>> s : src.entrySet()) {
        toCombine.clear();

        State dstKeyState = dst.get(s.getKey());
        if (dstKeyState == null) {
          dst.put(s.getKey(), s.getValue());
        } else {
          toCombine.add(dstKeyState);
          toCombine.add(s.getValue());
          @SuppressWarnings("unchecked")
          State newState = (State) stateCombiner.apply(toCombine);
          if (newState.getCollector() instanceof WindowedElementCollector) {
            ((WindowedElementCollector) newState.getCollector())
                .assignWindowing(dstWindow.getWindowID());
          }
          dst.put(s.getKey(), newState);
        }
      }
    }

    @Override
    public boolean scheduleTriggerAt(long stamp, WindowContext w, Trigger trigger) {
      return triggering.scheduleAt(
          stamp, w, ReduceStateByKeyReducer.this.createTriggerHandler(trigger));
    }

    @Override
    public long getCurrentTimestamp() {
      return triggering.getCurrentTimestamp();
    }

    /** Update current timestamp by given watermark. */
    void updateStamp(long stamp) {
      triggering.updateStamp(stamp);
    }

    /** Update trigger of given window ID. */
    void updateWindowTrigger(WindowID<Object, Object> windowID, long stamp) {
      LOG.debug("Updating trigger of windowID {} to {}", windowID, stamp);
      synchronized (this) {
        WindowStorage windowStorage = wRegistry.getWindowStorage(windowID);
        if (windowStorage != null) {
          // we might receive the window close event multiple times
          // so the window might be already flushed
          WindowContext<Object, Object> windowContext = windowStorage.getWindowContext();

          triggering.cancel(windowContext);
          Triggerable<Object, Object> triggerable = (t, ctx) -> {
                synchronized (ProcessingState.this) {
                  flushWindow(windowContext);
                  purgeWindow(windowContext);
                }
              };

          if (!triggering.scheduleAt(stamp, windowContext, triggerable)) {
            LOG.debug("Manually firing already passed flush event for windowID {}",
                windowID);
            triggerable.fire(stamp, windowContext);
          }
        }
      }

    }

    void emitWatermarkAndUpdateTriggering() {
      final long stamp = getCurrentTimestamp();
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

  // ~ the state is guarded by itself (triggers are fired
  // from within a separate thread)
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

  private Triggerable<Object, Object> createTriggerHandler(Trigger t) {
    return ((timestamp, w) -> {

      // ~ let trigger know about the time event and process window state
      // according to trigger result
      Trigger.TriggerResult result = t.onTimeEvent(timestamp, w, processing);

      if (result.isFlush()) {
        flushWindow(w);
      }
      if (result.isPurge()) {
        purgeWindow(w);
      }
    });
  }

  /**
   * Flush window (emit the internal state to output)
   * @param windowContext window context instance to processed
   */
  private void flushWindow(WindowContext<Object, Object> windowContext) {
    try {
      WindowID<Object, Object> flushedID = windowContext.getWindowID();
      Collection<State<?, ?>> evicted = processing.flushWindowStates(windowContext);
      evicted.stream().forEachOrdered(State::flush);      
      long stamp = getCurrentWatermark();
      output.put(Datum.windowTrigger(flushedID, stamp));
      output.put(Datum.watermark(stamp));
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Purge given window (discard internal state and cancel all triggers)
   * @param windowContext window context instance to be processed
   */
  private void purgeWindow(WindowContext<Object, Object> windowContext) {
    Collection<State<?, ?>> evicted;
    synchronized (processing) {
      if (!processing.active) {
        return;
      }
      // ~ cancel all triggers related to this window and purge state
      triggering.cancel(windowContext);
      evicted = processing.purgeWindowStates(windowContext);
    }

    evicted.stream().forEachOrdered(State::close);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    LOG.debug("Started ReduceStateByKeyReducer for operator {}", name);
    watermarkStrategy.schedule(processing::emitWatermarkAndUpdateTriggering);
    for (;;) {
      try {
        // ~ now process incoming data
        Datum item = input.take();
        watermarkStrategy.emitIfNeeded(processing::emitWatermarkAndUpdateTriggering);
        if (!item.isElement()) {
          if (item.isEndOfStream()) {
            // ~ stop triggers
            triggering.close();
            // close all states
            synchronized (processing) {
              processing.getAllWindowContexts().stream()
                  .forEach(wctx -> {
                    flushWindow(wctx);
                    purgeWindow(wctx);
                  });
              processing.active = false;
              processing.closeOutput();
            }
            output.put(item);
            return;
          }
          if (item.isWatermark()) {
            // update current stamp
            long stamp = ((Datum.Watermark) item).getWatermark();
            processing.updateStamp(stamp);
          } else if (item.isWindowTrigger() && isAttachedWindowing) {
            // reregister trigger of given window
            // FIXME: move this to windowing itself so that attached windowing
            // can be implemented 'natively' as instance of generic windowing
            Datum.WindowTrigger trigger = (Datum.WindowTrigger) item;
            processing.updateWindowTrigger(trigger.getWindowID(), trigger.getStamp());
          }          
        } else {
          final List<WindowContext> toEvict = processInput(item);
          for (WindowContext w : toEvict) {
            flushWindow(w);
            purgeWindow(w);
          }
        }
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  Set<Object> seenGroups = new HashSet<>();
  List<WindowContext> toEvict = new ArrayList<>();

  // ~ returns a list of windows which are to be evicted
  @SuppressWarnings("unchecked")
  private List<WindowContext> processInput(WindowedElement element) {

    Object item = element.get();
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<WindowID<Object, Object>> itemWindowLabels;
    itemWindowLabels = windowing.assignWindowsToElement(element);
    
    List<WindowContext> itemWindows = itemWindowLabels.stream()
        .map(windowing::createWindowContext)
        .collect(Collectors.toList());

    seenGroups.clear();
    toEvict.clear();

    synchronized (processing) {
      for (WindowContext<Object, Object> itemWindow : itemWindows) {
        Pair<WindowContext, State<Object, Object>> windowState =
            processing.getWindowStateForUpdate(itemWindow, itemKey);
        if (windowState != null) {
          windowState.getSecond().add((Object) itemValue);
          seenGroups.add(itemWindow.getWindowID().getGroup());
        } else {
          // window is already closed
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Element window {} discarded at current watermark {} with triggering {}",
                new String[] {
                    itemWindow.toString(),
                    String.valueOf(getCurrentWatermark()),
                    triggering.getClass().toString() });
          }
        }
      }

      if (windowing instanceof MergingWindowing) {
        for (Object group : seenGroups) {
          Collection<WindowContext> actives = processing.getActiveWindowsForGroup(group);
          MergingWindowing mwindowing = (MergingWindowing) this.windowing;
          if (actives.isEmpty()) {
            // ~ we've seen the group ... so we must have some actives
            throw new IllegalStateException("No active windows!");
          }
          Collection<Pair<Collection<WindowContext<Object, Object>>, WindowContext<Object, Object>>> merges
              = mwindowing.mergeWindows(actives);
          if (merges != null && !merges.isEmpty()) {
            for (
                Pair<Collection<WindowContext<Object, Object>>, WindowContext<Object, Object>> merge
                    : merges) {
              boolean merged = processing.mergeWindows(
                  merge.getFirst(), merge.getSecond());
              if (merged) {
                merge.getFirst().forEach(triggering::cancel);
                if (mwindowing.isComplete(merge.getSecond())) {
                  toEvict = new ArrayList<>();
                  toEvict.add(merge.getSecond());
                }
              }
            }
          }
        }
      }
    }

    return toEvict;
  }


  // retrieve current watermark stamp
  private long getCurrentWatermark() {
    return triggering.getCurrentTimestamp();
  }

}

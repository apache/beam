package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.core.client.triggers.Triggerable;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.EndOfStream;
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Objects.requireNonNull;

class ReduceStateByKeyReducer implements Runnable, EndOfWindowBroadcast.Subscriber {

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
    private final Window window;
    private final Map<Object, State> keyStates;
    // ~ initialized lazily; peers which have seen this window (and will eventually
    // emit or have already emitted a corresponding EoW)
    private List<EndOfWindowBroadcast.Subscriber> eowPeers;
    WindowStorage(Window window, int maxKeysStates) {
      this.window = requireNonNull(window);
      if (maxKeysStates > 0) {
        this.keyStates = new LRU<>(maxKeysStates);
      } else {
        this.keyStates = new HashMap<>();
      }
      this.eowPeers = null;
    }
    // ~ make a copy of 'base' with the specified 'window' assigned
    WindowStorage(Window window, WindowStorage base) {
      this.window = requireNonNull(window);
      this.keyStates = base.keyStates;
      this.eowPeers = base.eowPeers;
    }
    Window getWindow() {
      return window;
    }
    Map<Object, State> getKeyStates() {
      return keyStates;
    }
    List<EndOfWindowBroadcast.Subscriber> getEowPeers() {
      return eowPeers;
    }
    void addEowPeer(EndOfWindowBroadcast.Subscriber eow) {
      if (eowPeers == null) {
        eowPeers = new CopyOnWriteArrayList<>(Collections.singletonList(eow));
      } else if (!eowPeers.contains(requireNonNull(eow))) {
        eowPeers.add(eow);
      }
    }
  } // ~ end of WindowStorage

  private static final class WindowRegistry {
    // ~ a mapping of GROUP -> LABEL -> (WINDOW, ITEMKEY -> STATE)
    final Map<Object, Map<Object, WindowStorage>> windows = new HashMap<>();

    // ~ removes the given window and returns its key states (possibly null)
    WindowStorage removeWindow(Object wGroup, Object wLabel) {
      Map<Object, WindowStorage> byLabel = windows.get(wGroup);
      if (byLabel == null) {
        return null;
      }
      WindowStorage wStore = byLabel.remove(wLabel);
      // ~ garbage collect
      if (byLabel.isEmpty()) {
        windows.remove(wGroup);
      }
      return wStore;
    }

    WindowStorage getWindow(Object wGroup, Object wLabel) {
      Map<Object, WindowStorage> byLabel = windows.get(wGroup);
      if (byLabel == null) {
        return null;
      }
      return byLabel.get(wLabel);
    }

    void setWindow(WindowStorage store) {
      Window w = store.getWindow();
      Map<Object, WindowStorage> byLabel = windows.get(w.getGroup());
      if (byLabel == null) {
        windows.put(w.getGroup(), byLabel = new HashMap<>());
      }
      byLabel.put(w.getLabel(), store);
    }

    List<Window> getAllWindowsList() {
      return windows.values().stream()
          .flatMap(l -> l.values().stream())
          .map(WindowStorage::getWindow)
          .collect(Collectors.toList());
    }

    List<Window> getAllWindowsList(Object windowGroup) {
      Map<Object, WindowStorage> group = windows.get(windowGroup);
      if (group == null) {
        return Collections.emptyList();
      }
      return group.values().stream()
          .map(WindowStorage::getWindow)
          .collect(Collectors.toList());
    }
  } // ~ end of WindowRegistry

  private final class ProcessingState implements TriggerContext {

    final WindowRegistry wRegistry = new WindowRegistry();
    final int maxKeyStatesPerWindow;

    final Collector<Object> stateOutput;
    final BlockingQueue<Object> rawOutput;
    final TriggerScheduler triggering;
    final UnaryFunction stateFactory;
    final CombinableReduceFunction stateCombiner;

    // do we have bounded input?
    // if so, do not register windows for triggering, just trigger
    // windows at the end of input
    private final boolean isBounded;

    // ~ are we still actively processing input?
    private boolean active = true;

    @SuppressWarnings("unchecked")
    private ProcessingState(
        BlockingQueue output,
        TriggerScheduler triggering,
        UnaryFunction stateFactory,
        CombinableReduceFunction stateCombiner,
        boolean isBounded,
        int maxKeyStatesPerWindow)
    {
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
        this.rawOutput.put(EndOfStream.get());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public WindowStorage getWindowStorage(Window w) {
      return wRegistry.getWindow(w.getGroup(), w.getLabel());
    }

    /**
     * Flushes (emits result) the specified window
     *  @return accumulated states for the specified window
     */
    private Collection<State> flushWindowStates(Window w) {
      WindowStorage wKeyStates =
              wRegistry.getWindow(w.getGroup(), w.getLabel());

      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State> keyStates = wKeyStates.getKeyStates();
      if (keyStates == null) {
        return Collections.emptySet();
      }

      return keyStates.values();
    }

    /**
     * Purges the specified window
     * @return accumulated states for the specified window
     */
    private Collection<State> purgeWindowStates(Window w) {
      WindowStorage wKeyStates =
              wRegistry.removeWindow(w.getGroup(), w.getLabel());
      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State> keyStates = wKeyStates.getKeyStates();
      if (keyStates == null) {
        return Collections.emptySet();
      }

      return keyStates.values();
    }

    private Collection<State> purgeAllWindowStates() {
      List<Window> ws = wRegistry.getAllWindowsList();
      List<State> states = newArrayListWithCapacity(ws.size());
      for (Window w : ws) {
        states.addAll(purgeWindowStates(w));
      }
      return states;
    }

    @SuppressWarnings("unchecked")
    public Pair<Window, State> getWindowStateForUpdate(Window w, Object itemKey) {
      WindowStorage wStore = getOrCreateWindowStorage(w, false);
      if (wStore == null) {
        // the window is already closed
        return null;
      }
      // ~ remove and re-insert to give LRU based implementations a chance
      Map<Object, State> keyStates = wStore.getKeyStates();
      State state = keyStates.remove(itemKey);
      if (state == null) {
        // ~ collector decorating state output with a window label and item key
        DatumCollector collector = new DatumCollector(stateOutput) {
          @Override public void collect(Object elem) {
            super.collect(WindowedPair.of(getAssignLabel(), itemKey, elem));
          }
        };
        collector.assignWindowing(w.getGroup(), w.getLabel());
        state = (State) stateFactory.apply(collector);
        keyStates.put(itemKey, state);
      } else {
        keyStates.put(itemKey, state);
      }
      return Pair.of(wStore.getWindow(), state);
    }

    // ~ retrieves the keyStates associated with a window
    // and optionally override the window instance associated
    // with it
    private WindowStorage
    getOrCreateWindowStorage(Window<?, ?> w, boolean setWindowInstance) {
      WindowStorage wStore =
              wRegistry.getWindow(w.getGroup(), w.getLabel());
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
            Trigger.TriggerResult result = t.init(w, this);
            if (result == Trigger.TriggerResult.NOOP) {
              triggerState = result;
            }
          }
        }
        if (triggerState ==  Trigger.TriggerResult.PASSED) {
          // the window should have been already triggered
          // just discard the element, no other option for now
          return null;
        } else {
          // ~ if no such window yet ... set it up
          wRegistry.setWindow(wStore = new WindowStorage(w, maxKeyStatesPerWindow));
        }
      } else if (setWindowInstance && wStore.getWindow() != w) {
        // ~ identity comparison on purpose
        wRegistry.setWindow(wStore = new WindowStorage(w, wStore));
      }
      return wStore;
    }

    public Collection<Window> getActiveWindows(Object windowGroup) {
      return wRegistry.getAllWindowsList(windowGroup);
    }

    boolean mergeWindows(Collection<Window> toBeMerged, Window mergeWindow) {
      // ~ make sure 'mergeWindow' does exist
      WindowStorage ws = getOrCreateWindowStorage(mergeWindow, true);
      if (ws == null) {
        LOG.warn("No window storage for {}; potentially the triggering discarded it!",
            mergeWindow);
        wRegistry.removeWindow(mergeWindow.getGroup(), mergeWindow.getLabel());
        for (Window w : toBeMerged) {
          wRegistry.removeWindow(w.getGroup(), w.getLabel());
        }
        return false;
      }

      for (Window toMerge : toBeMerged) {
        if (Objects.equals(toMerge.getGroup(), ws.getWindow().getGroup())
            && Objects.equals(toMerge.getLabel(), ws.getWindow().getLabel())) {
          continue;
        }

        // ~ remove the toMerge window and merge all
        // of its keyStates into the mergeWindow
        WindowStorage toMergeState =
            wRegistry.removeWindow(toMerge.getGroup(), toMerge.getLabel());
        if (toMergeState != null) {
          mergeWindowKeyStates(toMergeState.getKeyStates(), ws.getKeyStates(), ws.getWindow());
        }
      }

      return true;
    }

    private void mergeWindowKeyStates(
        Map<Object, State> src, Map<Object, State> dst, Window dstWindow)
    {
      List<State> toCombine = new ArrayList<>(2);

      for (Map.Entry<Object, State> s : src.entrySet()) {
        toCombine.clear();

        State dstKeyState = dst.get(s.getKey());
        if (dstKeyState == null) {
          dst.put(s.getKey(), s.getValue());
        } else {
          toCombine.add(dstKeyState);
          toCombine.add(s.getValue());
          @SuppressWarnings("unchecked")
          State newState = (State) stateCombiner.apply(toCombine);
          if (newState.getCollector() instanceof DatumCollector) {
            ((DatumCollector) newState.getCollector())
                .assignWindowing(dstWindow.getGroup(), dstWindow.getLabel());
          }
          dst.put(s.getKey(), newState);
        }
      }
    }

    @Override
    public boolean scheduleTriggerAt(long stamp, Window w, Trigger trigger) {
      if (triggering.scheduleAt(stamp, w, ReduceStateByKeyReducer.this.createTriggerHandler(trigger)) != null) {
        // successfully scheduled
        return true;
      }
      return false;
    }

    @Override
    public long getCurrentTimestamp() {
      return triggering.getCurrentTimestamp();
    }

  } // ~ end of ProcessingState

  private final BlockingQueue input;

  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;

  // ~ the state is guarded by itself (triggers are fired
  // from within a separate thread)
  private final ProcessingState processing;

  private final TriggerScheduler triggering;

  private final EndOfWindowBroadcast eowBroadcast;

  private final String name;

  ReduceStateByKeyReducer(String name,
                          BlockingQueue input,
                          BlockingQueue output,
                          Windowing windowing,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          UnaryFunction stateFactory,
                          CombinableReduceFunction stateCombiner,
                          TriggerScheduler triggering,
                          boolean isBounded,
                          int maxKeyStatesPerWindow,
                          EndOfWindowBroadcast eowBroadcast) {
    this.name = name;
    this.input = requireNonNull(input);
    this.windowing = windowing == null ? DatumAttachedWindowing.INSTANCE : windowing;
    this.keyExtractor = requireNonNull(keyExtractor);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.triggering = requireNonNull(triggering);
    this.eowBroadcast = requireNonNull(eowBroadcast);
    this.processing = new ProcessingState(
        output, triggering,
        stateFactory, stateCombiner,
        isBounded,
        maxKeyStatesPerWindow);
    this.eowBroadcast.subscribe(this);
  }

  private Triggerable createTriggerHandler(Trigger t) {
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
   * Flush window (emmit the internal state to output)
   * @param window window instance to processed
   */
  private void flushWindow(Window window) {
    // ~ notify others we're about to evict the window
    @SuppressWarnings("unchecked")
    EndOfWindow eow = new EndOfWindow<>(window);

    // ~ notify others we're about to evict the window
    List<EndOfWindowBroadcast.Subscriber> eowPeers = null;
    synchronized (processing) {
      WindowStorage ws =
          processing.getWindowStorage(window);
      if (ws != null) {
        eowPeers = ws.getEowPeers();
      }
    }
    eowBroadcast.notifyEndOfWindow(eow, this,
        eowPeers == null ? Collections.emptyList() : eowPeers);

    synchronized (processing) {
      if (!processing.active) {
        return;
      }
      Collection<State> evicted = processing.flushWindowStates(window);
      evicted.stream().forEachOrdered(State::flush);
    }
    processing.stateOutput.collect(eow);
  }

  /**
   * Purge given window (discard internal state and cancel all triggers)
   * @param window window instance to be processed
   */
  private void purgeWindow(Window window) {
    Collection<State> evicted;
    synchronized (processing) {
      if (!processing.active) {
        return;
      }
      // ~ cancel all triggers related to this window and purge state
      triggering.cancel(window);
      evicted = processing.purgeWindowStates(window);
    }

    evicted.stream().forEachOrdered(State::close);
  }

  // ~ notified by peer partitions about the end-of-an-window; we'll
  // resend the event to our own partition output unless we know the
  // window
  @Override
  public void onEndOfWindowBroadcast(
      EndOfWindow eow, EndOfWindowBroadcast.Subscriber src)
  {
    final boolean known;
    synchronized (processing) {
      WindowStorage ws = processing.getWindowStorage(eow.getWindow());
      if (ws == null) {
        known = false;
      } else {
        known = true;
        ws.addEowPeer(src);
      }
    }
    if (!known) {
      processing.stateOutput.collect(eow);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    for (;;) {
      try {
        // ~ now process incoming data
        Object item = input.take();
        if (item instanceof EndOfStream) {
          break;
        }

        if (item instanceof EndOfWindow) {
          Window w = ((EndOfWindow) item).getWindow();
          flushWindow(w);
          purgeWindow(w);
        } else {
          final List<Window> toEvict = processInput((Datum) item);
          for (Window w : toEvict) {
            flushWindow(w);
            purgeWindow(w);
          }
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    // ~ stop triggers
    triggering.close();
    // close all states
    synchronized (processing) {
      processing.active = false;
      processing.purgeAllWindowStates().stream().forEachOrdered(s -> {s.flush(); s.close(); });
      processing.closeOutput();
    }
  }

  Set<Object> seenGroups = new HashSet<>();
  List<Window> toEvict = new ArrayList<>();

  // ~ returns a list of windows which are to be evicted
  @SuppressWarnings("unchecked")
  private List<Window> processInput(Datum datum) {

    Object item = datum.element;
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<Window> itemWindows;
    if (windowing == DatumAttachedWindowing.INSTANCE) {
      windowing.updateTriggering(triggering, datum);
      itemWindows = windowing.assignWindows(datum);
    } else {
      windowing.updateTriggering(triggering, item);
      itemWindows = windowing.assignWindows(item);
    }

    seenGroups.clear();
    toEvict.clear();

    synchronized (processing) {
      for (Window itemWindow : itemWindows) {
        Pair<Window, State> windowState =
            processing.getWindowStateForUpdate(itemWindow, itemKey);
        if (windowState != null) {
          windowState.getSecond().add(itemValue);
          seenGroups.add(itemWindow.getGroup());
        } else {
          // window is already closed
          LOG.debug("Element window {} discarded", itemWindow);
        }
      }

      if (windowing instanceof MergingWindowing) {
        for (Object group : seenGroups) {
          Collection<Window> actives = processing.getActiveWindows(group);
          MergingWindowing mwindowing = (MergingWindowing) this.windowing;
          if (actives.isEmpty()) {
            // ~ we've seen the group ... so we must have some actives
            throw new IllegalStateException("No active windows!");
          }
          Collection<Pair<Collection<Window>, Window>> merges
              = mwindowing.mergeWindows(actives);
          if (merges != null && !merges.isEmpty()) {
            for (Pair<Collection<Window>, Window> merge : merges) {
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
}

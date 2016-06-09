package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.operator.WindowedPair;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.EndOfStream;
import cz.seznam.euphoria.core.executor.inmem.InMemExecutor.QueueCollector;
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
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Objects.requireNonNull;

class ReduceStateByKeyReducer implements Runnable, EndOfWindowBroadcast.Subscriber {

  private static final Logger LOG = LoggerFactory.getLogger(ReduceStateByKeyReducer.class);

  // ~ storage of a single window's (internal) state
  private static final class WindowStorage {
    private final Window window;
    private final Map<Object, State> keyStates;
    WindowStorage(Window window, Map<Object, State> keyStates) {
      this.window = requireNonNull(window);
      this.keyStates = requireNonNull(keyStates);
    }
    Window getWindow() {
      return window;
    }
    Map<Object, State> getKeyStates() {
      return keyStates;
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

  private static final class ProcessingState {

    final WindowRegistry wRegistry = new WindowRegistry();

    // ~ an index over (item) keys to their held aggregating state
    final Map<Object, State> aggregatingStates = new HashMap<>();

    final Collector stateOutput;
    final BlockingQueue rawOutput;
    final Triggering triggering;
    final UnaryFunction<Window, Void> evictFn;
    final UnaryFunction stateFactory;
    final CombinableReduceFunction stateCombiner;
    final boolean aggregating;

    // do we have bounded input?
    // if so, do not register windows for trigerring, just trigger
    // windows at the end of input
    private final boolean isBounded;

    // ~ are we still actively processing input?
    private boolean active = true;

    private ProcessingState(
        BlockingQueue output,
        Triggering triggering,
        UnaryFunction<Window, Void> evictFn,
        UnaryFunction stateFactory,
        CombinableReduceFunction stateCombiner,
        boolean aggregating,
        boolean isBounded)
    {
      this.stateOutput = QueueCollector.wrap(requireNonNull(output));
      this.rawOutput = output;
      this.triggering = requireNonNull(triggering);
      this.evictFn = requireNonNull(evictFn);
      this.stateFactory = requireNonNull(stateFactory);
      this.stateCombiner = requireNonNull(stateCombiner);
      this.aggregating = aggregating;
      this.isBounded = isBounded;
    }

    // ~ signal eos further down the output channel
    public void closeOutput() {
      try {
        this.rawOutput.put(EndOfStream.get());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    public boolean isKnownWindow(Window w) {
      return wRegistry.getWindow(w.getGroup(), w.getLabel()) != null;
    }

    // ~ evicts the specified window returning states accumulated for it
    public Collection<State> evictWindow(Window w) {
      WindowStorage wKeyStates =
          wRegistry.removeWindow(w.getGroup(), w.getLabel());
      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State> keyStates = wKeyStates.getKeyStates();
      if (keyStates == null) {
        return Collections.emptySet();
      }

      if (aggregating) {
        for (Map.Entry<Object, State> e : keyStates.entrySet()) {
          State committed = aggregatingStates.get(e.getKey());
          State state = e.getValue();
          if (committed != null) {
            state = (State) stateCombiner.apply(newArrayList(committed, state));
            e.setValue(state);
          }
          aggregatingStates.put(e.getKey(), state);
        }
      }

      return keyStates.values();
    }

    public Collection<State> evictAllWindows() {
      List<Window> ws = wRegistry.getAllWindowsList();
      List<State> states = newArrayListWithCapacity(ws.size());
      for (Window w : ws) {
        states.addAll(evictWindow(w));
      }
      return states;
    }

    public Pair<Window, State> getWindowState(Window w, Object itemKey) {
      WindowStorage wStore = getOrCreateWindowStorage(w, false);
      if (wStore == null) {
        // the window is already closed
        return null;
      }
      State state = wStore.getKeyStates().get(itemKey);
      if (state == null) {
        // ~ collector decorating state output with a window label and item key
        DatumCollector collector = new DatumCollector(stateOutput) {
          @Override public void collect(Object elem) {
            super.collect(WindowedPair.of(getAssignLabel(), itemKey, elem));
          }
        };
        collector.assignWindowing(w.getGroup(), w.getLabel());
        state = (State) stateFactory.apply(collector);
        wStore.getKeyStates().put(itemKey, state);
      }
      return Pair.of(wStore.getWindow(), state);
    }

    // ~ retrieves the keyStates associated with a window
    // and optionally override the window instance associated
    // with it
    private WindowStorage
    getOrCreateWindowStorage(Window w, boolean setWindowInstance) {
      WindowStorage wStore =
          wRegistry.getWindow(w.getGroup(), w.getLabel());
      if (wStore == null) {
        Window.TriggerState triggerState = Window.TriggerState.INACTIVE;
        if (!isBounded) {
          // ~ give the window a chance to register triggers
          triggerState = w.registerTrigger(triggering, evictFn);
        }
        if (triggerState == Window.TriggerState.PASSED) {
          // the window should have been already triggered
          // just discard the element, no other option for now
          return null;
        } else {
          // ~ if no such window yet ... set it up
          wRegistry.setWindow(wStore = new WindowStorage(w, new HashMap<>()));
        }
      } else if (setWindowInstance && wStore.getWindow() != w) {
        // ~ identity comparison on purpose
        wRegistry.setWindow(wStore = new WindowStorage(w, wStore.getKeyStates()));
      }
      return wStore;
    }

    public Collection<Window> getActiveWindows(Object windowGroup) {
      return wRegistry.getAllWindowsList(windowGroup);
    }

//    @SuppressWarnings("unchecked")
    public void mergeWindows(Collection<Window> toBeMerged, Window mergeWindow) {
      // ~ make sure 'mergeWindow' does exist
      WindowStorage ws = getOrCreateWindowStorage(mergeWindow, true);

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
          toCombine.add(s.getValue());
          toCombine.add(dstKeyState);
          State newState = (State) stateCombiner.apply(toCombine);
          if (newState.getCollector() instanceof DatumCollector) {
            ((DatumCollector) newState.getCollector())
                .assignWindowing(dstWindow.getGroup(), dstWindow.getLabel());
          }
          dst.put(s.getKey(), newState);
        }
      }
    }
  } // ~ end of ProcessingState

  private final BlockingQueue input;

  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;

  // ~ the state is guarded by itself (triggers are fired
  // from within a separate thread)
  private final ProcessingState processing;

  private final Triggering triggering;

  private final EndOfWindowBroadcast eowBroadcast;

  ReduceStateByKeyReducer(BlockingQueue input,
                          BlockingQueue output,
                          Windowing windowing,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          UnaryFunction stateFactory,
                          CombinableReduceFunction stateCombiner,
                          Triggering triggering,
                          boolean isBounded,
                          EndOfWindowBroadcast eowBroadcast) {
    this.input = requireNonNull(input);
    this.windowing = windowing == null ? DatumAttachedWindowing.INSTANCE : windowing;
    this.keyExtractor = requireNonNull(keyExtractor);
    this.valueExtractor = requireNonNull(valueExtractor);
    this.triggering = requireNonNull(triggering);
    this.eowBroadcast = requireNonNull(eowBroadcast);
    this.processing = new ProcessingState(
        output, triggering,
        createEvictTrigger(),
        stateFactory, stateCombiner, this.windowing.isAggregating(),
        isBounded);
    this.eowBroadcast.subscribe(this);
  }

  private UnaryFunction<Window, Void> createEvictTrigger() {
    return (UnaryFunction<Window, Void>) window -> {
      fireEvictTrigger(window);
      return null;
    };
  }

  private void fireEvictTrigger(Window window) {
    // ~ notify others we're about to evict the window
    EndOfWindow eow = new EndOfWindow<>(window);
    eowBroadcast.notifyEndOfWindow(eow, this);

    Collection<State> evicted;
    synchronized (processing) {
      if (!processing.active) {
        return;
      }
      evicted = processing.evictWindow(window);
    }
    evicted.stream().forEachOrdered(state -> {
      state.flush();
      if (!windowing.isAggregating()) {
        state.close();
      }
    });
    processing.stateOutput.collect(eow);
  }

  // ~ notified by peer partitions about the end-of-an-window; we'll
  // resend the event to our own partition output unless we know the
  // window
  @Override
  public void onEndOfWindowBroadcast(EndOfWindow eow) {
    final boolean known;
    synchronized (processing) {
      known = processing.isKnownWindow(eow.getWindow());
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
          fireEvictTrigger(w);
        } else {
          final List<Window> toEvict = processInput((Datum) item);
          for (Window w : toEvict) {
            fireEvictTrigger(w);
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
      if (windowing.isAggregating()) {
        processing.evictAllWindows().stream().forEachOrdered(State::flush);
        processing.aggregatingStates.values().stream().forEachOrdered(State::close);
      } else {
        processing.evictAllWindows().stream().forEachOrdered(s -> {s.flush(); s.close(); });
      }
      processing.closeOutput();
    }
  }

  Set<Object> seenGroups = new HashSet<>();
  List<Window> toEvict = new ArrayList<>();

  // ~ returns a list of windows which are to be evicted (the list may be {@code null})
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
            processing.getWindowState(itemWindow, itemKey);
        if (windowState != null) {
          windowState.getSecond().add(itemValue);
          seenGroups.add(itemWindow.getGroup());
        } else {
          // window is already closed
          LOG.trace("Element window {} discarded", itemWindow);
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
              processing.mergeWindows(merge.getFirst(), merge.getSecond());
              if (mwindowing.isComplete(merge.getSecond())) {
                toEvict = new ArrayList<>();
                toEvict.add(merge.getSecond());
              }
            }
          }
        }
      }
    }

    return toEvict;
  }
}

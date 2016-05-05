package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.InMemExecutor.EndOfStream;
import cz.seznam.euphoria.core.executor.InMemExecutor.QueueCollector;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

class ReduceStateByKeyReducer implements Runnable {

  private final BlockingQueue input;
  private final BlockingQueue output;

  private final Windowing windowing;
  private final UnaryFunction keyExtractor;
  private final UnaryFunction valueExtractor;
  private final BinaryFunction stateFactory;
  private final CombinableReduceFunction stateCombiner;

  // ~ the below state is guarded by this mutex (triggers are fired
  // from within a separate thread)
  private final Object stateLock = new Object();

  // ~ XXX future optimization: might be shared across reducers in the
  // inmem to reduce the number of threads necessary
  final LocalTriggering triggering = new LocalTriggering();

  final Map<Window, Pair<Window, Map<Object, State>>> windowStates = new HashMap<>();
  final Map<Object, Set<Window>> activeWindowsPerKey = new HashMap<>();
  // XXX needs some rule to garbage collect obsolete aggregations
  final Map<Object, State> aggregatingStatesPerKey = new HashMap<>();

  final UnaryFunction<Window, Void> evictTrigger;

  ReduceStateByKeyReducer(BlockingQueue input,
                          BlockingQueue output,
                          Windowing windowing,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          BinaryFunction stateFactory,
                          CombinableReduceFunction stateCombiner)
  {
    this.input = input;
    this.output = output;
    this.windowing = windowing;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.stateFactory = stateFactory;
    this.stateCombiner = stateCombiner;

    this.evictTrigger = createEvictTrigger();
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

        synchronized (stateLock) {
          processInputItem(item);
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    // ~ stop triggers
    triggering.close();
    // close all states
    synchronized (stateLock) {
      windowStates.values().stream()
          .flatMap(m -> m.getSecond().values().stream())
          .forEach(State::close);
    }
    // ~ signal eos further down the channel
    try {
      output.put(EndOfStream.get());
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private UnaryFunction<Window, Void> createEvictTrigger() {
    return (UnaryFunction<Window, Void>) window -> {
      synchronized (stateLock) {
        Pair<Window, Map<Object, State>> windowState = windowStates.remove(window);
        window = windowState.getFirst();

        // ~ remove the window to be closed from the key->active-windows index
        for (Object key : windowState.getSecond().keySet()) {
          Set<Window> actives = activeWindowsPerKey.get(key);
          if (actives != null) {
            actives.remove(window);
            if (actives.isEmpty()) {
              activeWindowsPerKey.remove(key);
            }
          }
        }

        // ~ now flush the states tracked in the window
        windowState.getSecond().values().stream().forEachOrdered(State::flush);
      }
      return null;
    };
  }

  @SuppressWarnings("unchecked")
  private void processInputItem(Object item) {
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    Set<Window> itemWindows = windowing.assignWindows(item);
    for (Window itemWindow : itemWindows) {
      Pair<Window, Map<Object, State>> windowState = getWindowState(itemWindow, itemKey);
      Map<Object, State> keyStates = windowState.getSecond();

      State keyState = keyStates.get(itemKey);
      if (keyState == null) {
        keyState = windowing.isAggregating()
            ? aggregatingStatesPerKey.get(itemKey)
            : null;
        if (keyState == null) {
          keyState = (State) stateFactory.apply(itemKey, QueueCollector.wrap(output));
          if (windowing.isAggregating()) {
            aggregatingStatesPerKey.put(itemKey, keyState);
          }
        }
        keyStates.put(itemKey, keyState);
      }
      keyState.add(itemValue);
    }

    if (windowing instanceof MergingWindowing) {
      Set<Window> actives = activeWindowsPerKey.get(itemKey);
      if (actives != null && !actives.isEmpty()) {
        ((MergingWindowing) this.windowing).mergeWindows(
            Collections.unmodifiableSet(actives),
            (mergedWindows, mergeResult) -> {
              // ~ take the state of mergedWindows and merge it in
              // into the state of the mergeResult while silently dropping
              // the mergedWindows; mergeResult might be a newly created
              // window or a already existing one

              // ~ merge the states and register the potentially
              // new window for the key
              Pair<Window, Map<Object, State>> mergeResultState =
                  getWindowState (mergeResult, itemKey);
              mergeResult = mergeResultState.getFirst();
              State mergedState = combineStates(itemKey, mergedWindows);
              replaceWindowAndKeyState(mergeResult, itemKey, mergedState);

              // ~ now silently deregister the merged windows for the item's key
              Set<Window> activeWindows = activeWindowsPerKey.get(itemKey);
              // ~ activeWindows must now contain at least the mergeResult window
              assert activeWindows != null && !activeWindows.isEmpty();
              for (Window merged : (Iterable<Window>) mergedWindows) {
                if (!merged.equals(mergeResult)) {
                  // ~ de-activate the window
                  activeWindows.remove(merged);
                  // ~ drop it's state for the item's key
                  Pair<Window, Map<Object, State>> states = windowStates.get(merged);
                  if (states != null) {
                    Map<Object, State> keyStates = states.getSecond();
                    keyStates.remove(itemKey);
                    if (keyStates.isEmpty()) {
                      windowStates.remove(merged);
                    }
                  }
                }
              }
            });
      }
    }
  }

  private Pair<Window, Map<Object, State>>
  getWindowState(Window window, Object itemKey)
  {
    Pair<Window, Map<Object, State>> windowState = windowStates.get(window);
    if (windowState == null) {
      windowState = Pair.of(window, new HashMap<>());
      windowStates.put(window, windowState);
      // ~ track the newly created window as an active one
      // for the processed item's key
      setActive(window, itemKey);
      // ~ the itemWindow is new; allow it register triggers
      window.registerTrigger(triggering, evictTrigger);
    }
    return windowState;
  }

  private void setActive(Window window, Object itemKey) {
    Set<Window> actives = activeWindowsPerKey.get(itemKey);
    if (actives == null) {
      activeWindowsPerKey.put(itemKey, actives = new HashSet<>());
    }
    actives.add(window);
  }

  private State combineStates(Object itemKey, Set<Window> ws) {
    Collection<State> states = new HashSet<>();
    for (Window w : ws) {
      Pair<Window, Map<Object, State>> wStates = windowStates.get(w);
      if (wStates != null) {
        Map<Object, State> keyStates = wStates.getSecond();
        State keyState = keyStates.get(itemKey);
        if (keyState != null) {
          states.add(keyState);
        }
      }
    }
    return (State) stateCombiner.apply(states);
  }

  private void replaceWindowAndKeyState(Window window, Object itemKey, State newState) {
    Map<Object, State> keyStates = windowStates.get(window).getSecond();
    keyStates.put(itemKey, newState);
    if (windowing.isAggregating()) {
      aggregatingStatesPerKey.put(itemKey, newState);
    }
  }
}
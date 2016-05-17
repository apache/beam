package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Collector;
import cz.seznam.euphoria.core.client.operator.State;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.InMemExecutor.EndOfStream;
import cz.seznam.euphoria.core.executor.InMemExecutor.QueueCollector;

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
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static java.util.Objects.requireNonNull;

class ReduceStateByKeyReducer implements Runnable {

  private static final class WindowStorage {
    // ~ a mapping of GROUP -> LABEL -> (WINDOW, ITEMKEY -> STATE)
    final Map<Object, Map<Object, Pair<Window, Map<Object, State>>>> wStates
        = new HashMap<>();

    // ~ removes the given window and returns its key states (possibly null)
    Pair<Window, Map<Object, State>> removeWindow(Object wGroup, Object wLabel) {
      Map<Object, Pair<Window, Map<Object, State>>> byLabel = wStates.get(wGroup);
      if (byLabel == null) {
        return null;
      }
      Pair<Window, Map<Object, State>> wState = byLabel.remove(wLabel);
      // ~ garbage collect
      if (byLabel.isEmpty()) {
        wStates.remove(wGroup);
      }
      return wState;
    }

    Pair<Window, Map<Object, State>> getWindow(Object wGroup, Object wLabel) {
      Map<Object, Pair<Window, Map<Object, State>>> byLabel = wStates.get(wGroup);
      if (byLabel == null) {
        return null;
      }
      return byLabel.get(wLabel);
    }

    void setWindow(Pair<Window, Map<Object, State>> states) {
      Window w = states.getFirst();
      Map<Object, Pair<Window, Map<Object, State>>> byLabel = wStates.get(w.getGroup());
      if (byLabel == null) {
        wStates.put(w.getGroup(), byLabel = new HashMap<>());
      }
      byLabel.put(w.getLabel(), states);
    }

    List<Window> getAllWindowsList() {
      return wStates.values().stream()
          .flatMap(l -> l.values().stream())
          .map(Pair::getFirst)
          .collect(Collectors.toList());
    }

    List<Window> getAllWindowsList(Object windowGroup) {
      Map<Object, Pair<Window, Map<Object, State>>> group = wStates.get(windowGroup);
      if (group == null) {
        return Collections.emptyList();
      }
      return group.values().stream()
          .map(Pair::getFirst)
          .collect(Collectors.toList());
    }
  }

  private static final class ProcessingState {

    final WindowStorage wStorage = new WindowStorage();

    // ~ an index over (item) keys to their held aggregating state
    // XXX move to WindowStorage
    final Map<Object, State> aggregatingStates = new HashMap<>();

    final Collector stateOutput;
    final BlockingQueue rawOutput;
    final Triggering triggering;
    final UnaryFunction<Window, Void> evictFn;
    final BinaryFunction stateFactory;
    final CombinableReduceFunction stateCombiner;
    final boolean aggregating;

    // ~ are we still actively processing input?
    private boolean active = true;

    public ProcessingState(
        BlockingQueue output,
        Triggering triggering,
        UnaryFunction<Window, Void> evictFn,
        BinaryFunction stateFactory,
        CombinableReduceFunction stateCombiner,
        boolean aggregating)
    {
      this.stateOutput = QueueCollector.wrap(requireNonNull(output));
      this.rawOutput = output;
      this.triggering = requireNonNull(triggering);
      this.evictFn = requireNonNull(evictFn);
      this.stateFactory = requireNonNull(stateFactory);
      this.stateCombiner = requireNonNull(stateCombiner);
      this.aggregating = aggregating;
    }

    // ~ signal eos further down the output channel
    public void closeOutput() {
      try {
        this.rawOutput.put(EndOfStream.get());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    // ~ evicts the specified window returning states accumulated for it
    public Collection<State> evictWindow(Window w) {
      Pair<Window, Map<Object, State>> wKeyStates =
          wStorage.removeWindow(w.getGroup(), w.getLabel());
      if (wKeyStates == null) {
        return Collections.emptySet();
      }
      Map<Object, State> keyStates = wKeyStates.getSecond();
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
      List<Window> ws = wStorage.getAllWindowsList();
      List<State> states = newArrayListWithCapacity(ws.size());
      for (Window w : ws) {
        states.addAll(evictWindow(w));
      }
      return states;
    }

    public Pair<Window, State> getWindowState(Window w, Object itemKey) {
      Pair<Window, Map<Object, State>> wState = getWindowKeyStates(w, false);
      State state = wState.getSecond().get(itemKey);
      if (state == null) {
        state = (State) stateFactory.apply(itemKey, stateOutput);
        wState.getSecond().put(itemKey, state);
      }
      return Pair.of(wState.getFirst(), state);
    }

    // ~ retrieves the keyStates associated with a window
    // and optionally override the window instance associated
    // with it
    private Pair<Window, Map<Object, State>>
    getWindowKeyStates(Window w, boolean setWindowInstance)
    {
      Pair<Window, Map<Object, State>> wState =
          wStorage.getWindow(w.getGroup(), w.getLabel());
      if (wState == null) {
        // ~ if no such window yet ... set it up
        wStorage.setWindow(wState = Pair.of(w, new HashMap<>()));
        // ~ give the window a chance to register triggers
        w.registerTrigger(triggering, evictFn);
      } else if (setWindowInstance && wState.getFirst() != w) {
        // ~ identity comparison on purpose
        wStorage.setWindow(wState = Pair.of(w, wState.getSecond()));
      }
      return wState;
    }

    public Collection<Window> getActiveWindows(Object windowGroup) {
      return wStorage.getAllWindowsList(windowGroup);
    }

    @SuppressWarnings("unchecked")
    public void mergeWindows(Collection<Window> toBeMerged, Window mergeWindow) {
      // ~ make sure 'mergeWindow' does exist
      Pair<Window, Map<Object, State>> ws = getWindowKeyStates(mergeWindow, true);

      for (Window toMerge : toBeMerged) {
        if (Objects.equals(toMerge.getGroup(), ws.getFirst().getGroup())
            && Objects.equals(toMerge.getLabel(), ws.getFirst().getLabel()))
        {
          continue;
        }

        // ~ remove the toMerge window and merge all
        // of its keyStates into the mergeWindow
        Pair<Window, Map<Object, State>> toMergeState =
            wStorage.removeWindow(toMerge.getGroup(), toMerge.getLabel());
        if (toMergeState != null) {
          mergeWindowKeyStates(toMergeState.getSecond(), ws.getSecond());
        }
      }
    }

    private void mergeWindowKeyStates(Map<Object, State> src, Map<Object, State> dst) {
      List<State> toCombine = new ArrayList<>(2);

      for (Map.Entry<Object, State> s : src.entrySet()) {
        toCombine.clear();

        State dstKeyState = dst.get(s.getKey());
        if (dstKeyState == null) {
          dst.put(s.getKey(), s.getValue());

        } else {
          toCombine.add(s.getValue());
          toCombine.add(dstKeyState);
          dst.put(s.getKey(), (State) stateCombiner.apply(toCombine));
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

  private final ProcessingTimeTriggering triggering = new ProcessingTimeTriggering();

  ReduceStateByKeyReducer(BlockingQueue input,
                          BlockingQueue output,
                          Windowing windowing,
                          UnaryFunction keyExtractor,
                          UnaryFunction valueExtractor,
                          BinaryFunction stateFactory,
                          CombinableReduceFunction stateCombiner)
  {
    this.input = input;
    this.windowing = windowing;
    this.keyExtractor = keyExtractor;
    this.valueExtractor = valueExtractor;
    this.processing = new ProcessingState(
        output, new ProcessingTimeTriggering(),
        createEvictTrigger(),
        stateFactory, stateCombiner, windowing.isAggregating());
  }

  private UnaryFunction<Window, Void> createEvictTrigger() {
    return (UnaryFunction<Window, Void>) window -> {
      Collection<State> evicted;
      synchronized (processing) {
        if (!processing.active) {
          return null;
        }
        evicted = processing.evictWindow(window);
      }
      evicted.stream().forEachOrdered(
          windowing.isAggregating()
            ? State::flush
            : (Consumer<State>) s -> { s.flush(); s.close(); });
      return null;
    };
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

        synchronized (processing) {
          processInput(item);
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

  @SuppressWarnings("unchecked")
  private void processInput(Object item) {
    Object itemKey = keyExtractor.apply(item);
    Object itemValue = valueExtractor.apply(item);

    seenGroups.clear();
    Set<Window> itemWindows = windowing.assignWindows(item);
    for (Window itemWindow : itemWindows) {
      processing.getWindowState(itemWindow, itemKey).getSecond().add(itemValue);
      seenGroups.add(itemWindow.getGroup());
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
            triggerIfComplete(mwindowing, merge.getSecond());
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void triggerIfComplete(MergingWindowing windowing, Window w) {
    if (windowing.isComplete(w)) {
      processing.evictFn.apply(w);
    }
  }
}

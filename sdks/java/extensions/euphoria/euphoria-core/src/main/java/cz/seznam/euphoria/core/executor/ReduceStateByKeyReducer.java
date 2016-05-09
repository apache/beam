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
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

class ReduceStateByKeyReducer implements Runnable {

  private static final class WindowIndex {
    private final Object group;
    private final Object label;

    private int hash;

    WindowIndex(Window w) {
      this.group = w.getGroup();
      this.label = w.getLabel();
    }

    Object getGroup() {
      return group;
    }

    Object getLabel() {
      return label;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WindowIndex) {
        WindowIndex that = (WindowIndex) obj;
        return Objects.equals(this.group, that.group)
            && Objects.equals(this.label, that.label);
      }
      return false;
    }

    static boolean windowsEqual(Window a, Window b) {
      return Objects.equals(a.getGroup(), b.getGroup())
          && Objects.equals(a.getLabel(), b.getLabel());
    }

    @Override
    public int hashCode() {
      int h = hash;
      if (h == 0) {
        h = Objects.hashCode(this.group) * 31 + Objects.hash(this.label);
        hash = h;
      }
      return h;
    }
  } // ~ end of WindowIndex

  private static final class ProcessingState {

    // ~ an index over windows, and the states for each key they containing
    final Map<WindowIndex, Pair<Window, Map<Object, State>>> wStates
        = new HashMap<>();

    // ~ an index over (item) keys to their held aggregating state
    final Map<Object, State> aggregatingStates = new HashMap<>();

    final Collector stateOutput;
    final BlockingQueue rawOutput;
    final Triggering triggering;
    final UnaryFunction<Window, Void> evictFn;
    final BinaryFunction stateFactory;
    final CombinableReduceFunction stateCombiner;
    final boolean aggregating;

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
      Map<Object, State> wKeyState = wStates.remove(new WindowIndex(w)).getSecond();
      if (wKeyState == null) {
        return Collections.emptySet();
      }

      if (aggregating) {
        for (Map.Entry<Object, State> e : wKeyState.entrySet()) {
          State committed = aggregatingStates.get(e.getKey());
          State state = e.getValue();
          if (committed != null) {
                state = (State) stateCombiner.apply(newArrayList(committed, state));
            e.setValue(state);
          }
          aggregatingStates.put(e.getKey(), state);
        }
      }

      return wKeyState.values();
    }

    public Collection<State> evictAllWindows() {
      List<Window> ws = wStates.values()
          .stream()
          .map(Pair::getFirst)
          .collect(Collectors.toList());
      List<State> states = newArrayList();
      for (Window w : ws) {
        states.addAll(evictWindow(w));
      }
      return states;
    }

    public Pair<Window, State> getWindowState(Window w, Object itemKey) {
      Pair<Window, Map<Object, State>> wState = getWindowState_(w, false);
      State state = wState.getSecond().get(itemKey);
      if (state == null) {
        state = (State) stateFactory.apply(itemKey, stateOutput);
        wState.getSecond().put(itemKey, state);
      }
      return Pair.of(wState.getFirst(), state);
    }

    private Pair<Window, Map<Object, State>>
    getWindowState_(Window w, boolean setWindowInstance)
    {
      WindowIndex wIdx = new WindowIndex(w);
      Pair<Window, Map<Object, State>> wState = wStates.get(wIdx);
      if (wState == null) {
        // ~ if no such window yet ... set it up
        wStates.put(wIdx, wState = Pair.of(w, new HashMap<>()));
        // ~ give the window a chance to register triggers
        w.registerTrigger(triggering, evictFn);
      } else if (setWindowInstance && wState.getFirst() != w) {
        // ~ identity comparison on purpose
        wStates.put(wIdx, wState = Pair.of(w, wState.getSecond()));
      }
      return wState;
    }

    public Collection<Window> getActiveWindows(Object windowGroup) {
      // XXX make this faster!
      return wStates.values().stream()
          .map(Pair::getFirst)
          .filter(w -> Objects.equals(w.getGroup(), windowGroup))
          .collect(Collectors.toList());
    }

    public void mergeWindows(Collection<Window> toBeMerged, Window mergeWindow) {
      // ~ make sure 'mergeWindow' does exist
      Pair<Window, Map<Object, State>> ws = getWindowState_(mergeWindow, true);

      for (Window toMerge : toBeMerged) {
        if (WindowIndex.windowsEqual(toMerge, ws.getFirst())) {
          continue;
        }

        // ~ remove the toMerge window and merge all
        // of its keyStates into the mergeWindow
        Pair<Window, Map<Object, State>> toMergeState =
            wStates.remove(new WindowIndex(toMerge));
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
  private final ProcessingState state;

  private final LocalTriggering triggering = new LocalTriggering();

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
    this.state = new ProcessingState(
        output, new LocalTriggering(),
        createEvictTrigger(),
        stateFactory, stateCombiner, windowing.isAggregating());
  }

  private UnaryFunction<Window, Void> createEvictTrigger() {
    return (UnaryFunction<Window, Void>) window -> {
      Collection<State> evicted;
      synchronized (state) {
        evicted = state.evictWindow(window);
      }
      evicted.stream().forEachOrdered(State::flush);
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

        synchronized (state) {
          processInput(item);
        }
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    // ~ stop triggers
    // ~ XXX might want to run all pending triggers
    triggering.close();
    // close all states
    synchronized (state) {
      state.evictAllWindows().stream().forEachOrdered(State::flush);
      state.closeOutput();
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
      state.getWindowState(itemWindow, itemKey).getSecond().add(itemValue);
      seenGroups.add(itemWindow.getGroup());
    }

    if (windowing instanceof MergingWindowing) {
      for (Object group : seenGroups) {
        Collection<Window> actives = state.getActiveWindows(group);
        MergingWindowing mwindowing = (MergingWindowing) this.windowing;
        switch (actives.size()) {
          case 0:
            break;
          case 1: {
            triggerIfComplete(mwindowing, actives.iterator().next());
            break;
          }
          default: {
            Collection<Pair<Collection<Window>, Window>> merges
                = mwindowing.mergeWindows(actives);
            if (merges != null && !merges.isEmpty()) {
              for (Pair<Collection<Window>, Window> merge : merges) {
                state.mergeWindows(merge.getFirst(), merge.getSecond());
                triggerIfComplete(mwindowing, merge.getSecond());
              }
            }
            break;
          }
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void triggerIfComplete(MergingWindowing windowing, Window w) {
    if (windowing.isComplete(w)) {
      state.evictFn.apply(w);
    }
  }
}
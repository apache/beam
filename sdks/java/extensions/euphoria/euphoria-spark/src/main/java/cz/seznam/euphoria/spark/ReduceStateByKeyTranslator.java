package cz.seznam.euphoria.spark;

import com.google.common.collect.Iterators;
import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.operator.CompositeKey;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.util.Pair;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToIntFunction;

class ReduceStateByKeyTranslator implements SparkOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(ReduceStateByKey operator,
                              SparkExecutorContext context) {

    final JavaRDD<WindowedElement> input = (JavaRDD) context.getSingleInput(operator);

    StateFactory<?, State> stateFactory = operator.getStateFactory();

    final UnaryFunction keyExtractor;
    final UnaryFunction valueExtractor;
    final Windowing windowing = operator.getWindowing();
    final UnaryFunction<?, Long> eventTimeAssigner = operator.getEventTimeAssigner();

    // FIXME
    if (windowing instanceof MergingWindowing) {
      throw new UnsupportedOperationException("Merging windows not supported yet");
    }

    // FIXME functions extraction could be moved to the euphoria-core
    if (operator.isGrouped()) {
      UnaryFunction reduceKeyExtractor = operator.getKeyExtractor();
      keyExtractor = (UnaryFunction<Pair, CompositeKey>)
              (Pair p) -> CompositeKey.of(
                      p.getFirst(),
                      reduceKeyExtractor.apply(p.getSecond()));
      UnaryFunction vfn = operator.getValueExtractor();
      valueExtractor = (UnaryFunction<Pair, Object>)
              (Pair p) -> vfn.apply(p.getSecond());
    } else {
      keyExtractor = operator.getKeyExtractor();
      valueExtractor = operator.getValueExtractor();
    }

    // extract composite key (window, key) from data
    JavaPairRDD<KeyedWindow, Object> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(keyExtractor, valueExtractor, windowing, eventTimeAssigner));

    JavaPairRDD<KeyedWindow, Object> sorted = tuples.repartitionAndSortWithinPartitions(
            new PartitioningWrapper(operator.getPartitioning()),
            // ~ comparing by hashcode will effectively group elements with
            // the same key to the one bucket
            Comparator.comparingInt(
                    (ToIntFunction<? super KeyedWindow> & Serializable) KeyedWindow::hashCode));

    // ~ iterate through the sorted partition and incrementally reduce states
    return sorted.mapPartitions(new StateReducer(stateFactory));
  }

  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<WindowedElement, KeyedWindow, Object> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;
    private final UnaryFunction eventTimeAssigner;

    public CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing,
                                 UnaryFunction<?, Long> eventTimeAssigner) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    public Iterator<Tuple2<KeyedWindow, Object>> call(WindowedElement wel) throws Exception {
      List<Tuple2<KeyedWindow, Object>> output;

      // if windowing defined use window assigner
      if (windowing != null) {
        if (eventTimeAssigner != null) {
          wel.setTimestamp((long) eventTimeAssigner.apply(wel.get()));
        }

        output = new LinkedList<>();
        Set<Window> windows = windowing.assignWindowsToElement(wel);
        for (Window wid : windows) {
          Object el = wel.get();
          output.add(new Tuple2<>(
                  new KeyedWindow<>(wid, keyExtractor.apply(el)),
                  valueExtractor.apply(el)));
        }
      } else {
        Object el = wel.get();
        return Iterators.singletonIterator(new Tuple2<>((KeyedWindow)
                new KeyedWindow<>(wel.getWindow(), keyExtractor.apply(el)),
                valueExtractor.apply(el)));
      }
      return output.iterator();
    }
  }

  private static class StateReducer
          implements FlatMapFunction<Iterator<Tuple2<KeyedWindow, Object>>, WindowedElement> {

    private final StateFactory<?, State> stateFactory;
    private final SparkStorageProvider storageProvider;
    private transient Map<KeyedWindow, State> activeStates;

    public StateReducer(StateFactory<?, State> stateFactory) {
      this.stateFactory = stateFactory;
      this.storageProvider = new SparkStorageProvider();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<WindowedElement> call(Iterator<Tuple2<KeyedWindow, Object>> iterator) {
      activeStates = new HashMap<>();
      FunctionContextAsync<?> context = new FunctionContextAsync<>();

      // reduce states in separate thread
      context.runAsynchronously(() -> {
        int currentHashCode = 0;
        while (iterator.hasNext()) {
          Tuple2<KeyedWindow, Object> element = iterator.next();
          KeyedWindow key = element._1();
          Object value = element._2();

          // ~ when hashcode changes we reached end of the current bucket
          // and all currently opened states can be flushed to the output
          if (currentHashCode != key.hashCode()) {
            flushStates();
          }
          currentHashCode = key.hashCode();

          State s = activeStates.get(key);
          if (s == null) {
            context.setWindow(key);
            s = stateFactory.apply((Context) context, storageProvider);
            activeStates.put(key, s);
          }
          s.add(value);
        }

        flushStates();
      });

      // return blocking iterator
      return context.iterator();
    }

    private void flushStates() {
      for (Map.Entry<KeyedWindow, State> e : activeStates.entrySet()) {
        State s = e.getValue();
        s.flush();
        s.close();
      }
      activeStates.clear();
    }


  }
}

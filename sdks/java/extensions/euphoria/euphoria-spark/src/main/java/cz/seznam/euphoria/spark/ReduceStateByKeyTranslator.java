package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ReduceStateByKey;
import cz.seznam.euphoria.core.client.operator.state.State;
import cz.seznam.euphoria.core.client.triggers.Trigger;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.greduce.GroupReducer;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

class ReduceStateByKeyTranslator implements SparkOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(ReduceStateByKey operator,
                              SparkExecutorContext context) {

    final JavaRDD<WindowedElement> input = (JavaRDD) context.getSingleInput(operator);

    StateFactory<?, State> stateFactory = operator.getStateFactory();
    CombinableReduceFunction<State> stateCombiner = operator.getStateCombiner();

    final UnaryFunction keyExtractor = operator.getKeyExtractor();
    final UnaryFunction valueExtractor = operator.getValueExtractor();
    final UnaryFunction<?, Long> eventTimeAssigner = operator.getEventTimeAssigner();
    final Windowing windowing = operator.getWindowing() == null
            ? AttachedWindowing.INSTANCE
            : operator.getWindowing();

    // ~ extract key/value + timestamp from input elements and assign windows
    JavaPairRDD<KeyedWindow, Object> tuples = input.flatMapToPair(
            new CompositeKeyExtractor(keyExtractor, valueExtractor, windowing, eventTimeAssigner));

    // ~ if merging windowing used all windows for one key need to be
    // processed in single task, otherwise they can be freely distributed
    // to better utilize cluster resources
    Partitioner groupingPartitioner;
    Comparator<KeyedWindow> comparator = new KeyTimestampComparator();
    if (windowing instanceof MergingWindowing ||
            !operator.getPartitioning().hasDefaultPartitioner()) {

      groupingPartitioner = new PartitioningWrapper(operator.getPartitioning());
    } else {
      groupingPartitioner = new HashPartitioner(operator.getPartitioning().getNumPartitions());
    }

    JavaPairRDD<KeyedWindow, Object> sorted = tuples.repartitionAndSortWithinPartitions(
            groupingPartitioner,
            comparator);

    // ~ iterate through the sorted partition and incrementally reduce states
    return sorted.mapPartitions(new StateReducer(windowing, stateFactory, stateCombiner));
  }

  /**
   *  Comparing by key hashcode will effectively group elements with the same key
   *  to the one bucket.
   */
  private static class KeyTimestampComparator
          implements Comparator<KeyedWindow>, Serializable {

    @Override
    public int compare(KeyedWindow o1, KeyedWindow o2) {
      int result = Integer.compare(o1.key().hashCode(), o2.key().hashCode());

      if (result == 0) {
        result = Long.compare(o1.timestamp(), o2.timestamp());
      }

      return result;
    }
  }

  /**
   * Extracts {@link KeyedWindow} from {@link WindowedElement} and
   * assigns timestamp according to (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<WindowedElement, KeyedWindow, Object> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;
    private final UnaryFunction eventTimeAssigner;

    public CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing,
                                 UnaryFunction<?, Long> eventTimeAssigner /* optional */) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, Object>> call(WindowedElement wel) throws Exception {
      if (eventTimeAssigner != null) {
        wel.setTimestamp((long) eventTimeAssigner.apply(wel.getElement()));
      }

      Set<Window> windows = windowing.assignWindowsToElement(wel);
      List<Tuple2<KeyedWindow, Object>> out = new ArrayList<>(windows.size());
      for (Window wid : windows) {
        Object el = wel.getElement();
        out.add(new Tuple2<>(
                new KeyedWindow<>(wid, wel.getTimestamp(), keyExtractor.apply(el)),
                valueExtractor.apply(el)));
      }
      return out.iterator();
    }
  }


  private static class StateReducer
          implements FlatMapFunction<Iterator<Tuple2<KeyedWindow, Object>>, WindowedElement> {

    private final Windowing windowing;
    private final Trigger trigger;
    private final StateFactory<?, State> stateFactory;
    private final CombinableReduceFunction<State> stateCombiner;
    private final SparkStorageProvider storageProvider;

    // mapping of [Key -> GroupReducer]
    private transient Map<Object, GroupReducer> activeReducers;

    public StateReducer(Windowing windowing,
                        StateFactory<?, State> stateFactory,
                        CombinableReduceFunction<State> stateCombiner) {
      this.windowing = windowing;
      this.trigger = windowing.getTrigger();
      this.stateFactory = stateFactory;
      this.stateCombiner = stateCombiner;
      this.storageProvider = new SparkStorageProvider();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<WindowedElement> call(Iterator<Tuple2<KeyedWindow, Object>> iterator) {
      activeReducers = new HashMap<>();
      FunctionContextAsync<WindowedElement<?, Pair<?, ?>>> context = new FunctionContextAsync<>();

      // reduce states in separate thread
      context.runAsynchronously(() -> {
        int currentHashCode = 0;
        while (iterator.hasNext()) {
          Tuple2<KeyedWindow, Object> element = iterator.next();
          KeyedWindow kw = element._1();
          Object value = element._2();

          // ~ when hashcode changes we reached end of the current bucket
          // and all currently opened states can be flushed to the output
          if (currentHashCode != kw.key().hashCode()) {
            flushStates();
          }
          currentHashCode = kw.key().hashCode();

          GroupReducer reducer = activeReducers.get(kw.key());
          if (reducer == null) {
            reducer = new GroupReducer<>(stateFactory,
                    stateCombiner,
                    storageProvider,
                    windowing,
                    trigger,
                    el -> context.collect((WindowedElement) el));

            activeReducers.put(kw.key(), reducer);
          }
          reducer.process(
                  new WindowedElement(kw.window(), kw.timestamp(), Pair.of(kw.key(), value)));
        }

        flushStates();
      });

      // return blocking iterator
      return context.iterator();
    }

    private void flushStates() {
      for (Map.Entry<Object, GroupReducer> e : activeReducers.entrySet()) {
        GroupReducer reducer = e.getValue();
        reducer.close();
      }
      activeReducers.clear();
    }


  }
}

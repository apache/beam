/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.MergingWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.functional.StateFactory;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.operator.ExtractEventTime;
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

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

class ReduceStateByKeyTranslator implements SparkOperatorTranslator<ReduceStateByKey> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(ReduceStateByKey operator,
                              SparkExecutorContext context) {

    final JavaRDD<SparkElement> input = (JavaRDD) context.getSingleInput(operator);

    StateFactory<?, State> stateFactory = operator.getStateFactory();
    CombinableReduceFunction<State> stateCombiner = operator.getStateCombiner();

    final UnaryFunction keyExtractor = operator.getKeyExtractor();
    final UnaryFunction valueExtractor = operator.getValueExtractor();
    final ExtractEventTime eventTimeAssigner = operator.getEventTimeAssigner();
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
   * Extracts {@link KeyedWindow} from {@link SparkElement} and
   * assigns timestamp according to (optional) eventTimeAssigner.
   */
  private static class CompositeKeyExtractor
          implements PairFlatMapFunction<SparkElement, KeyedWindow, Object> {

    private final UnaryFunction keyExtractor;
    private final UnaryFunction valueExtractor;
    private final Windowing windowing;
    @Nullable
    private final ExtractEventTime eventTimeAssigner;

    public CompositeKeyExtractor(UnaryFunction keyExtractor,
                                 UnaryFunction valueExtractor,
                                 Windowing windowing,
                                 @Nullable ExtractEventTime eventTimeAssigner) {
      this.keyExtractor = keyExtractor;
      this.valueExtractor = valueExtractor;
      this.windowing = windowing;
      this.eventTimeAssigner = eventTimeAssigner;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterator<Tuple2<KeyedWindow, Object>> call(SparkElement wel) throws Exception {
      if (eventTimeAssigner != null) {
        wel.setTimestamp(eventTimeAssigner.extractTimestamp(wel.getElement()));
      }

      Iterable<Window> windows = windowing.assignWindowsToElement(wel);
      List<Tuple2<KeyedWindow, Object>> out = new ArrayList<>();
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
          implements FlatMapFunction<Iterator<Tuple2<KeyedWindow, Object>>, SparkElement> {

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
    public Iterator<SparkElement> call(Iterator<Tuple2<KeyedWindow, Object>> iterator) {
      activeReducers = new HashMap<>();
      FunctionContextAsync<SparkElement<?, Pair<?, ?>>> context = new FunctionContextAsync<>();

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
                    SparkElement::new,
                    stateCombiner,
                    storageProvider,
                    windowing,
                    trigger,
                    el -> context.collect((SparkElement) el));

            activeReducers.put(kw.key(), reducer);
          }
          reducer.process(
                  new SparkElement(kw.window(), kw.timestamp(), Pair.of(kw.key(), value)));
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
